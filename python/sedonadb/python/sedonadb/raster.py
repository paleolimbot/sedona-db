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

import struct
from typing import List, Optional, TYPE_CHECKING, Tuple
import geoarrow.types as gat
import pyarrow as pa

if TYPE_CHECKING:
    import numpy as np

EXTENSION_NAME = "sedona.raster"

# Band data type IDs (matches sedona-schema BandDataType enum discriminants)
BAND_DATA_TYPES = {
    1: "UInt8",
    2: "UInt16",
    3: "Int16",
    4: "UInt32",
    5: "Int32",
    6: "Float32",
    7: "Float64",
    8: "UInt64",
    9: "Int64",
    10: "Int8",
}

# Python struct module format characters for band data types
BAND_DATA_TYPE_STRUCT_CHARS = {
    1: "B",
    2: "H",
    3: "h",
    4: "I",
    5: "i",
    6: "f",
    7: "d",
    8: "Q",
    9: "q",
    10: "b",
}


def _get_binary_view_buffer(
    data_array: pa.BinaryViewArray, index: int = 0
) -> Optional[memoryview]:
    """Extract a zero-copy memoryview from a BinaryViewArray element.

    Decodes the BinaryView format to resolve the correct variadic buffer
    for out-of-line data. Returns None for inline data (≤12 bytes) which
    requires copying.

    Args:
        data_array: A BinaryViewArray (may be sliced).
        index: The element index within the array (after any slicing).

    Returns:
        A memoryview pointing directly into the variadic buffer for out-of-line
        data, or None if the data is inline and must be copied.
    """
    # BinaryView layout: [validity, views, variadic_buffers...]
    # Each view is 16 bytes:
    #   - Inline (len ≤ 12): length:i4, data:12bytes
    #   - Out-of-line (len > 12): length:i4, prefix:4bytes, buf_idx:i4, offset:i4
    buffers = data_array.buffers()
    if len(buffers) < 2 or buffers[1] is None:
        return None

    views_buf = memoryview(buffers[1])
    # Account for array offset (e.g., from slicing) plus the requested index
    array_offset = data_array.offset + index
    view_start = array_offset * 16
    view_bytes = views_buf[view_start : view_start + 16]

    # Decode length from first 4 bytes
    length = struct.unpack_from("<I", view_bytes, 0)[0]

    if length <= 12:
        # Inline data - caller must copy
        return None

    # Out-of-line: decode buffer_index and offset
    buf_idx = struct.unpack_from("<I", view_bytes, 8)[0]
    offset = struct.unpack_from("<I", view_bytes, 12)[0]

    # Variadic buffers start at index 2
    variadic_buf_idx = 2 + buf_idx
    if variadic_buf_idx >= len(buffers) or buffers[variadic_buf_idx] is None:
        return None

    data_buf = memoryview(buffers[variadic_buf_idx])
    return data_buf[offset : offset + length]


class Raster:
    """Python representation of a sedona.raster scalar value."""

    def __init__(self, array, i=0):
        """Create a Raster from an Arrow array at index i."""
        if isinstance(array, pa.ExtensionArray):
            array = array.storage

        # Use slice directly - pa.array() would copy
        self._array = array.slice(i, i + 1)

    def _py_field(self, k):
        """Extract a field value as a Python object."""
        return self._array.field(k)[0].as_py()

    @property
    def crs(self) -> gat.Crs:
        """The coordinate reference system of this raster."""
        return gat.type_spec(crs=self._py_field("crs")).crs

    @property
    def width(self) -> int:
        """The width of this raster in pixels."""
        return self._py_field("spatial_shape")[0]

    @property
    def height(self) -> int:
        """The height of this raster in pixels."""
        return self._py_field("spatial_shape")[1]

    @property
    def transform(self) -> List[float]:
        """The affine transform coefficients for this raster."""
        return self._py_field("transform")

    @property
    def bands(self) -> List["Band"]:
        """The list of bands in this raster."""
        bands_array = self._array.field("bands").flatten()
        return [Band(bands_array, i) for i in range(len(bands_array))]

    def __repr__(self) -> str:
        """Return a string representation of this raster."""
        return f"<Raster {self.width}x{self.height}, {len(self.bands)} band(s)>"

    def __arrow_c_array__(self, requested_schema=None):
        """Implement the array protocol so this works with lit()"""
        extension_type = RasterType(self._array.type)
        extension_array = extension_type.wrap_array(self._array)
        return extension_array.__arrow_c_array__(requested_schema=requested_schema)


class Band:
    """Python representation of a raster band."""

    def __init__(self, array, i=0):
        """Create a Band from an Arrow array at index i."""
        # Use slice directly - pa.array() would copy
        self._array = array.slice(i, i + 1)

    def _py_field(self, k):
        """Extract a field value as a Python object."""
        return self._array.field(k)[0].as_py()

    @property
    def name(self) -> Optional[str]:
        """The name of this band, if any."""
        return self._py_field("name")

    @property
    def shape(self) -> Tuple[int, ...]:
        """The shape of this band's data after applying any views."""
        views = self._py_field("view")
        if views:
            raise NotImplementedError("Lazy views are not yet supported")

        return self.source_shape

    @property
    def source_shape(self) -> Tuple[int, ...]:
        """The shape of this band's source data."""
        return tuple(self._py_field("source_shape"))

    @property
    def outdb_uri(self) -> Optional[str]:
        """The URI for out-of-database storage, if any."""
        return self._py_field("outdb_uri")

    @property
    def data_type(self) -> str:
        """The pixel data type name (e.g., 'uint8', 'float32')."""
        type_id = self._py_field("data_type")
        return BAND_DATA_TYPES[type_id].lower()

    @property
    def source_data(self) -> memoryview:
        """The raw source data buffer as a memoryview.

        Zero-copy for out-of-line BinaryView data by decoding the view descriptor
        and resolving the correct variadic buffer. Falls back to copying for
        inline data (≤12 bytes).
        """
        if self.source_data_size == 0:
            return memoryview(b"")

        data_array = self._array.field("data")

        # Try zero-copy path for out-of-line data
        result = _get_binary_view_buffer(data_array, index=0)
        if result is not None:
            return result

        # Fallback: inline data or missing buffer - must copy
        return memoryview(data_array[0].as_buffer())

    @property
    def source_data_size(self) -> int:
        """The size of the source data buffer in bytes."""
        data_array = self._array.field("data")
        return len(data_array[0].as_buffer())

    @property
    def data(self) -> memoryview:
        """The band data as a typed, shaped memoryview."""
        if self.outdb_uri is not None:
            raise ValueError("Can't extract buffer from a reference to external data.")

        # When views are supported, we would need to calculate the striding
        # to export a zero copy view.
        views = self._py_field("view")
        if views:
            raise NotImplementedError("Lazy views are not yet supported")

        buffer_type_id = self._py_field("data_type")
        buffer_type_char = BAND_DATA_TYPE_STRUCT_CHARS[buffer_type_id]
        return self.source_data.cast(buffer_type_char, self.shape)

    def to_numpy(self) -> "np.ndarray":
        """Convert this band's data to a numpy array (zero-copy when possible)."""
        import numpy as np

        if self.source_data_size == 0:
            return np.empty(self.shape, dtype=self.data_type)

        # Use frombuffer for zero-copy
        return np.frombuffer(self.source_data, dtype=self.data_type).reshape(self.shape)

    def __repr__(self) -> str:
        """Return a string representation of this band."""
        name_part = f" {self.name!r}" if self.name else ""
        return f"<Band{name_part} {self.data_type} {'x'.join(map(str, self.shape))}>"


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
