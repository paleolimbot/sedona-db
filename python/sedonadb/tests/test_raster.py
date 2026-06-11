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

from sedonadb.raster import Raster, RasterArray, RasterScalar, RasterType


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

    assert r.width == 64
    assert r.height == 32
    assert len(r.transform) == 6
    assert len(r.bands) == 3

    b = r.bands[0]
    assert b.name is None
    assert b.shape == [32, 64]
    assert b.source_shape == [32, 64]
    assert b.outdb_uri is None
