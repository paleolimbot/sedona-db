// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `RS_NDVI` — normalized difference vegetation index from red and NIR rasters.
//!
//! ```text
//! RS_NDVI(red, nir) -> Raster<Float32>
//! ```
//!
//! Both inputs must be single-band, two-dimensional rasters on the same grid.
//! The result inherits the red raster's grid and contains one Float32 band named
//! `ndvi`. Input nodata and a zero denominator become NaN, which is also the
//! output band's nodata value.
//!
//! Raster pixels live inside one BinaryView value per band, so Arrow/DataFusion
//! numeric kernels cannot consume them directly. This kernel instead borrows
//! both contiguous input buffers and walks them once, pixel-for-pixel, writing
//! Float32 bytes directly into the one output buffer required by the raster
//! representation. It allocates no full-size intermediate numeric arrays.

use std::sync::Arc;

use arrow_buffer::Buffer;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, Volatility};
use sedona_expr::scalar_udf::{SedonaScalarKernel, SedonaScalarUDF};
use sedona_raster::band_builder::check_band_data_len;
use sedona_raster::builder::{RasterBuilder, RasterOverrides, StartBandArgs};
use sedona_raster::error::RasterResultExt;
use sedona_raster::traits::RasterRef;
use sedona_schema::datatypes::SedonaType;
use sedona_schema::matchers::ArgMatcher;
use sedona_schema::raster::BandDataType;

use crate::crs_utils::resolve_crs;
use crate::executor::RasterExecutor;
use crate::pixel_scan::{NodataMatcher, spatial_2d_buffer};
use crate::rs_ensure_loaded::{NEEDS_PIXELS_METADATA_KEY, RETURNS_BYTES_METADATA_KEY};

const FUNC: &str = "RS_NDVI";

/// `RS_NDVI(red, nir)` scalar UDF.
pub fn rs_ndvi_udf() -> SedonaScalarUDF {
    SedonaScalarUDF::new("rs_ndvi", vec![Arc::new(RsNdvi)], Volatility::Immutable)
        .with_metadata(NEEDS_PIXELS_METADATA_KEY, "true")
        .with_metadata(RETURNS_BYTES_METADATA_KEY, "true")
}

#[derive(Debug)]
struct RsNdvi;

impl SedonaScalarKernel for RsNdvi {
    fn return_type(&self, args: &[SedonaType]) -> Result<Option<SedonaType>> {
        ArgMatcher::new(
            vec![ArgMatcher::is_raster(), ArgMatcher::is_raster()],
            SedonaType::Raster,
        )
        .match_args(args)
    }

    fn invoke_batch(
        &self,
        arg_types: &[SedonaType],
        args: &[ColumnarValue],
    ) -> Result<ColumnarValue> {
        let executor = RasterExecutor::new(arg_types, args);
        let mut builder = RasterBuilder::new(executor.num_iterations());

        executor.execute_raster_raster_void(|_, red, nir| match (red, nir) {
            (Some(red), Some(nir)) => append_ndvi(&mut builder, red, nir),
            _ => Ok(builder.append_null()?),
        })?;

        executor.finish(Arc::new(builder.finish()?))
    }
}

fn append_ndvi(
    builder: &mut RasterBuilder,
    red: &dyn RasterRef,
    nir: &dyn RasterRef,
) -> Result<()> {
    check_inputs(red, nir)?;

    let red_band = red.band(0)?;
    let nir_band = nir.band(0)?;
    let red_buffer = spatial_2d_buffer(FUNC, red_band.as_ref())?;
    let nir_buffer = spatial_2d_buffer(FUNC, nir_band.as_ref())?;
    if red_buffer.shape != nir_buffer.shape {
        return exec_err!(
            "{FUNC}: red band shape {:?} does not match NIR band shape {:?}",
            red_buffer.shape,
            nir_buffer.shape
        );
    }

    // RS_EnsureLoaded produces contiguous buffers for OutDb inputs. Requiring
    // contiguity here also avoids silently materialising a second copy when an
    // explicitly strided InDb view is supplied.
    let red_bytes = red_buffer.as_contiguous().context(FUNC)?;
    let nir_bytes = nir_buffer.as_contiguous().context(FUNC)?;
    let red_size = red_buffer.data_type.byte_size();
    let nir_size = nir_buffer.data_type.byte_size();
    let pixel_count = red_bytes.len() / red_size;
    if nir_bytes.len() / nir_size != pixel_count {
        return exec_err!("{FUNC}: red and NIR bands contain different pixel counts");
    }

    let output_len = pixel_count
        .checked_mul(BandDataType::Float32.byte_size())
        .ok_or_else(|| datafusion_common::exec_datafusion_err!("{FUNC}: output is too large"))?;
    check_band_data_len(output_len)
        .map_err(|e| datafusion_common::exec_datafusion_err!("{FUNC}: {e}"))?;

    let red_nodata = NodataMatcher::for_band(FUNC, red_band.as_ref())?;
    let nir_nodata = NodataMatcher::for_band(FUNC, nir_band.as_ref())?;
    let mut output = Vec::with_capacity(output_len);

    for (red_pixel, nir_pixel) in red_bytes
        .chunks_exact(red_size)
        .zip(nir_bytes.chunks_exact(nir_size))
    {
        let is_nodata = red_nodata
            .as_ref()
            .is_some_and(|matcher| matcher.matches(red_pixel))
            || nir_nodata
                .as_ref()
                .is_some_and(|matcher| matcher.matches(nir_pixel));

        let value = if is_nodata {
            f32::NAN
        } else {
            let red = pixel_as_f64(red_pixel, red_buffer.data_type);
            let nir = pixel_as_f64(nir_pixel, nir_buffer.data_type);
            let denominator = nir + red;
            if denominator == 0.0 {
                f32::NAN
            } else {
                ((nir - red) / denominator) as f32
            }
        };
        output.extend_from_slice(&value.to_le_bytes());
    }

    let nodata = f32::NAN.to_le_bytes();
    let dim_names = red_band.dim_names();
    builder.start_raster_from(red, RasterOverrides::default())?;
    builder.start_band(StartBandArgs {
        name: Some("ndvi"),
        nodata: Some(&nodata),
        ..StartBandArgs::new(&dim_names, red_band.shape(), BandDataType::Float32)
    })?;

    if output.is_empty() {
        builder.band_data_writer().append_value([]);
    } else {
        let output = Buffer::from_vec(output);
        builder.append_band_data_buffer(&output, 0, output_len as u32)?;
    }
    builder.finish_band()?;
    builder.finish_raster()?;
    Ok(())
}

fn check_inputs(red: &dyn RasterRef, nir: &dyn RasterRef) -> Result<()> {
    if red.num_bands() != 1 || nir.num_bands() != 1 {
        return exec_err!(
            "{FUNC}: expected one band in each input, got {} red and {} NIR bands",
            red.num_bands(),
            nir.num_bands()
        );
    }
    if red.spatial_shape() != nir.spatial_shape() {
        return exec_err!(
            "{FUNC}: red raster shape {:?} does not match NIR raster shape {:?}",
            red.spatial_shape(),
            nir.spatial_shape()
        );
    }
    if red.transform() != nir.transform() {
        return exec_err!("{FUNC}: red and NIR rasters have different geotransforms");
    }

    let same_crs = red.crs() == nir.crs()
        || match (resolve_crs(red.crs())?, resolve_crs(nir.crs())?) {
            (None, None) => true,
            (Some(red), Some(nir)) => red.crs_equals(nir.as_ref()),
            _ => false,
        };
    if !same_crs {
        return exec_err!("{FUNC}: red and NIR rasters have different CRSes");
    }
    Ok(())
}

#[inline]
fn pixel_as_f64(pixel: &[u8], data_type: BandDataType) -> f64 {
    macro_rules! read {
        ($ty:ty) => {
            <$ty>::from_le_bytes(pixel.try_into().expect("validated pixel width")) as f64
        };
    }

    match data_type {
        BandDataType::UInt8 => pixel[0] as f64,
        BandDataType::Int8 => pixel[0] as i8 as f64,
        BandDataType::UInt16 => read!(u16),
        BandDataType::Int16 => read!(i16),
        BandDataType::UInt32 => read!(u32),
        BandDataType::Int32 => read!(i32),
        BandDataType::UInt64 => read!(u64),
        BandDataType::Int64 => read!(i64),
        BandDataType::Float32 => read!(f32),
        BandDataType::Float64 => read!(f64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::ScalarUDF;
    use sedona_schema::datatypes::RASTER;
    use sedona_testing::raster_spec::{RasterSpec, assert_raster_scalar_equals};
    use sedona_testing::testers::ScalarUdfTester;

    fn tester() -> ScalarUdfTester {
        ScalarUdfTester::new(rs_ndvi_udf().into(), vec![RASTER, RASTER])
    }

    #[test]
    fn metadata() {
        let udf = rs_ndvi_udf();
        let scalar: ScalarUDF = udf.clone().into();
        assert_eq!(scalar.name(), "rs_ndvi");
        for key in [NEEDS_PIXELS_METADATA_KEY, RETURNS_BYTES_METADATA_KEY] {
            assert_eq!(udf.metadata().get(key).map(String::as_str), Some("true"));
        }
    }

    #[test]
    fn computes_pixel_for_pixel() {
        let red = RasterSpec::d2(4, 1).band_values(&[1u16, 2, 0, 4]);
        let nir = RasterSpec::d2(4, 1).band_values(&[3u16, 2, 0, 0]);
        let result = tester().invoke_scalar_scalar(red, nir).unwrap();
        let expected = RasterSpec::d2(4, 1)
            .band_values(&[0.5f32, 0.0, f32::NAN, -1.0])
            .name("ndvi")
            .nodata(f32::NAN);
        assert_raster_scalar_equals(&result, &expected);
    }

    #[test]
    fn input_nodata_becomes_output_nodata() {
        let red = RasterSpec::d2(3, 1)
            .band_values(&[1u8, 255, 2])
            .nodata(255u8);
        let nir = RasterSpec::d2(3, 1)
            .band_values(&[3u16, 4, 999])
            .nodata(999u16);
        let result = tester().invoke_scalar_scalar(red, nir).unwrap();
        let expected = RasterSpec::d2(3, 1)
            .band_values(&[0.5f32, f32::NAN, f32::NAN])
            .name("ndvi")
            .nodata(f32::NAN);
        assert_raster_scalar_equals(&result, &expected);
    }

    #[test]
    fn rejects_different_grids() {
        let red = RasterSpec::d2(2, 1).band_values(&[1u8, 2]);
        let nir = RasterSpec::d2(3, 1).band_values(&[1u8, 2, 3]);
        let err = tester().invoke_scalar_scalar(red, nir).unwrap_err();
        assert!(err.to_string().contains("does not match NIR raster shape"));
    }
}
