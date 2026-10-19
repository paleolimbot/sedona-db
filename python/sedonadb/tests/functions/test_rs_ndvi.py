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

"""SQL integration tests for RS_NDVI."""


def test_rs_ndvi_loads_outdb_inputs(con, sedona_testing):
    """The planner loads both inputs before the native pixel kernel runs."""
    path = sedona_testing / "data/raster/sentinel2.tif"
    result = (
        con.sql(
            """
            WITH ndvi AS (
                SELECT RS_NDVI(RS_FromPath($1), RS_FromPath($1)) AS raster
            )
            SELECT
                RS_Width(raster) AS width,
                RS_Height(raster) AS height,
                RS_BandPixelType(raster, 1) AS pixel_type,
                RS_Value(raster, 1, 1, 1) AS value
            FROM ndvi
            """,
            params=(str(path),),
        )
        .to_arrow_table()
        .to_pylist()
    )

    assert result == [
        {
            "width": 512,
            "height": 512,
            "pixel_type": "REAL_32BITS",
            "value": 0.0,
        }
    ]
