# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import numpy as np

from sedonadb.raster import (
    Raster,
    RasterArray,
    RasterScalar,
    RasterType,
    _get_binary_view_buffer,
)


def test_type_class_resolution(con):
    t = con.sql("SELECT RS_Example() as raster")
    tab = t.to_arrow_table()

    assert isinstance(tab.schema.field(0).type, RasterType)
    assert isinstance(tab["raster"].type, RasterType)
    assert isinstance(tab["raster"].chunk(0), RasterArray)
    assert isinstance(tab["raster"][0], RasterScalar)
    assert isinstance(tab["raster"][0].as_py(), Raster)


def test_raster_accessors(con):
    t = con.sql("SELECT RS_Example() as raster")
    tab = t.to_arrow_table()
    r: Raster = tab["raster"][0].as_py()

    assert r.crs.to_json_dict()["id"] == {"authority": "OGC", "code": "CRS84"}
    assert r.width == 64
    assert r.height == 32
    assert len(r.transform) == 6
    assert len(r.bands) == 3
    assert repr(r) == "<Raster 64x32, 3 band(s)>"

    b = r.bands[0]
    assert b.name is None
    assert b.shape == (32, 64)
    assert b.source_shape == (32, 64)
    assert b.outdb_uri is None
    assert b.data_type == "uint8"
    assert repr(b) == "<Band uint8 32x64>"

    arr = b.to_numpy()
    assert arr.shape == b.shape
    assert arr[0, 0] == 127

    for i, b in enumerate(r.bands):
        assert b.data[1, 1] == i + 1


def test_raster_to_lit(con):
    t = con.sql("SELECT RS_Example() as raster")
    tab = t.to_arrow_table()
    r = tab["raster"][0].as_py()

    t2 = con.sql(
        "SELECT RS_Width($1) AS w, RS_Height($1) AS h", params=(r,)
    ).to_pandas()
    assert t2.iloc[0, 0] == r.width
    assert t2.iloc[0, 1] == r.height


def test_raster_zero_copy_access(con):
    """Test that zero-copy buffer extraction works for BinaryView data."""
    t = con.sql("SELECT RS_Example() as raster")
    tab = t.to_arrow_table()
    r: Raster = tab["raster"][0].as_py()

    b = r.bands[0]

    # Get the underlying data array
    data_array = r._array.field("bands").flatten().field("data")

    # Zero-copy buffer extraction should work for out-of-line data
    mv = _get_binary_view_buffer(data_array, index=0)
    assert mv is not None, "Expected out-of-line data for raster band"

    # The memoryview should have the expected size
    assert len(mv) == b.source_data_size

    # to_numpy should return array backed by the same buffer
    arr = b.to_numpy()
    assert arr.shape == b.shape

    # Verify the data matches
    expected_first_value = 127  # Known value from RS_Example
    assert arr[0, 0] == expected_first_value


def test_raster_zero_copy_shares_buffer(con):
    """Test that to_numpy shares the underlying buffer (zero-copy)."""
    t = con.sql("SELECT RS_Example() as raster")
    tab = t.to_arrow_table()
    r: Raster = tab["raster"][0].as_py()

    b = r.bands[0]

    # Get two numpy arrays from the same band
    arr1 = b.to_numpy()
    arr2 = b.to_numpy()

    # They should share the same underlying buffer (same data pointer)
    assert (
        arr1.__array_interface__["data"][0] == arr2.__array_interface__["data"][0]
    ), "to_numpy should return zero-copy view sharing the same buffer"

    # Verify the arrays are views, not copies (same base memory)
    assert np.shares_memory(arr1, arr2)
