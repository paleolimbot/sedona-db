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

from typing import List, Optional
import geoarrow.types as gat
import pyarrow as pa

EXTENSION_NAME = "sedona.raster"


class Raster:
    def __init__(self, array, i=0):
        if isinstance(array, pa.ExtensionArray):
            array = array.storage

        self._array = pa.array(array.slice(i, i + 1))

    def _py_field(self, k):
        return self._array.field(k)[0].as_py()

    @property
    def crs(self) -> gat.Crs:
        return gat.type_spec(crs=self._py_field("crs")).crs

    @property
    def width(self) -> int:
        return self._py_field("spatial_shape")[0]

    @property
    def height(self) -> int:
        return self._py_field("spatial_shape")[1]

    @property
    def transform(self) -> List[float]:
        return self._py_field("transform")

    @property
    def bands(self) -> List["Band"]:
        bands_array = self._array.field("bands").flatten()
        return [Band(bands_array, i) for i in range(len(bands_array))]


class Band:
    def __init__(self, array, i=0):
        self._array = pa.array(array.slice(i, i + 1))

    def _py_field(self, k):
        return self._array.field(k)[0].as_py()

    @property
    def name(self) -> Optional[str]:
        return self._py_field("name")

    @property
    def shape(self) -> List[int]:
        views = self._py_field("view")
        if views:
            raise NotImplementedError("Lazy views are not yet supported")

        return self.source_shape

    @property
    def source_shape(self) -> List[int]:
        return self._py_field("source_shape")

    @property
    def outdb_uri(self) -> Optional[str]:
        return self._py_field("outdb_uri")

    @property
    def data(self):
        pass


class RasterScalar(pa.ExtensionScalar):
    """Scalar type for sedona.raster extension arrays."""

    def as_py(self):
        return Raster(pa.array([self.value]))


class RasterArray(pa.ExtensionArray):
    """Array type for sedona.raster extension arrays."""

    pass


class RasterType(pa.ExtensionType):
    """PyArrow extension type for sedona.raster.

    This extension type wraps a struct storage type representing raster data.
    """

    def __init__(self, storage_type: pa.DataType):
        """Create a RasterType with the given storage type.

        Parameters
        ----------
        storage_type : pa.DataType
            The underlying Arrow storage type (must be a struct type).
        """
        if not pa.types.is_struct(storage_type):
            raise TypeError(f"storage_type must be a struct type, not {storage_type}")
        super().__init__(storage_type, EXTENSION_NAME)

    def __arrow_ext_serialize__(self) -> bytes:
        """Serialize extension type metadata."""
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "RasterType":
        return RasterType(storage_type)

    def __arrow_ext_class__(self):
        return RasterArray

    def __arrow_ext_scalar_class__(self):
        return RasterScalar


def register_extension_type():
    """Register the sedona.raster extension type with PyArrow.

    This should be called once at module initialization to enable
    automatic deserialization of sedona.raster arrays from IPC.
    """
    # Create a dummy storage type for registration - the actual storage
    # type will be determined during deserialization
    dummy_storage = pa.struct([("_placeholder", pa.int32())])
    try:
        pa.register_extension_type(RasterType(dummy_storage))
    except pa.ArrowKeyError:
        # Already registered
        pass
