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

use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt::{Debug, Display, Formatter};
use std::ptr::null_mut;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{DataFusionError, Result};
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use sedona_common::sedona_internal_err;

use crate::extension::SedonaCObjectStore;

/// Wrapper around a [SedonaCObjectStore] that implements [ObjectStore]
///
/// This is the means by which an ObjectStore implementation may be imported from a
/// C implementation.
pub struct ImportedObjectStore {
    inner: SedonaCObjectStore,
}

impl TryFrom<SedonaCObjectStore> for ImportedObjectStore {
    type Error = DataFusionError;

    fn try_from(value: SedonaCObjectStore) -> Result<Self> {
        match (&value.display, &value.debug, &value.release) {
            (Some(_), Some(_), Some(_)) => Ok(Self { inner: value }),
            _ => {
                sedona_internal_err!("Can't import released or uninitialized SedonaCObjectStore")
            }
        }
    }
}

impl Display for ImportedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(display_fn) = self.inner.display {
            let c_str = unsafe { display_fn(&self.inner) };
            if c_str.is_null() {
                write!(f, "<null>")
            } else {
                let rust_str = unsafe { CStr::from_ptr(c_str) }.to_string_lossy();
                write!(f, "{}", rust_str)
            }
        } else {
            write!(f, "ImportedObjectStore(<invalid>)")
        }
    }
}

impl Debug for ImportedObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(debug_fn) = self.inner.debug {
            let c_str = unsafe { debug_fn(&self.inner) };
            if c_str.is_null() {
                f.debug_struct("ImportedObjectStore")
                    .field("inner", &"<null>")
                    .finish()
            } else {
                let rust_str = unsafe { CStr::from_ptr(c_str) }.to_string_lossy();
                f.debug_struct("ImportedObjectStore")
                    .field("inner", &rust_str)
                    .finish()
            }
        } else {
            f.debug_struct("ImportedObjectStore")
                .field("inner", &"<invalid>")
                .finish()
        }
    }
}

#[async_trait]
impl ObjectStore for ImportedObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        todo!()
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        todo!()
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _options: GetOptions,
    ) -> object_store::Result<GetResult> {
        todo!()
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        todo!()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        todo!()
    }
}

/// Wrapper around an [Arc<dyn ObjectStore>] that may be used to export an existing
/// ObjectStore across an FFI boundary using the [SedonaCObjectStore]
pub struct ExportedObjectStore {
    inner: Arc<dyn ObjectStore>,
    display_cache: CString,
    debug_cache: CString,
}

impl From<Arc<dyn ObjectStore>> for ExportedObjectStore {
    fn from(value: Arc<dyn ObjectStore>) -> Self {
        let display_str = format!("{}", value);
        let debug_str = format!("{:?}", value);
        ExportedObjectStore {
            inner: value,
            display_cache: CString::new(display_str).unwrap_or_default(),
            debug_cache: CString::new(debug_str).unwrap_or_default(),
        }
    }
}

impl From<ExportedObjectStore> for SedonaCObjectStore {
    fn from(value: ExportedObjectStore) -> Self {
        let box_value = Box::new(value);
        Self {
            display: Some(c_object_store_display),
            debug: Some(c_object_store_debug),
            release: Some(c_object_store_release),
            private_data: Box::leak(box_value) as *mut ExportedObjectStore as *mut c_void,
        }
    }
}

/// C callable wrapper to get the display string
unsafe extern "C" fn c_object_store_display(self_: *const SedonaCObjectStore) -> *const c_char {
    assert!(!self_.is_null());
    let self_ref = self_.as_ref().unwrap();

    assert!(!self_ref.private_data.is_null());
    let private_data = (self_ref.private_data as *mut ExportedObjectStore)
        .as_ref()
        .unwrap();
    private_data.display_cache.as_ptr()
}

/// C callable wrapper to get the debug string
unsafe extern "C" fn c_object_store_debug(self_: *const SedonaCObjectStore) -> *const c_char {
    assert!(!self_.is_null());
    let self_ref = self_.as_ref().unwrap();

    assert!(!self_ref.private_data.is_null());
    let private_data = (self_ref.private_data as *mut ExportedObjectStore)
        .as_ref()
        .unwrap();
    private_data.debug_cache.as_ptr()
}

/// C callable wrapper called when this value is dropped via FFI
unsafe extern "C" fn c_object_store_release(self_: *mut SedonaCObjectStore) {
    assert!(!self_.is_null());
    let self_ref = self_.as_mut().unwrap();

    assert!(!self_ref.private_data.is_null());
    let boxed = Box::from_raw(self_ref.private_data as *mut ExportedObjectStore);
    drop(boxed);

    self_ref.private_data = null_mut();
    self_ref.release = None;
}

#[cfg(test)]
mod test {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn test_export_import_display_debug() {
        // Create an in-memory object store and export it
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let original_display = format!("{}", store);
        let original_debug = format!("{:?}", store);

        let exported = ExportedObjectStore::from(store);
        let ffi_store: SedonaCObjectStore = exported.into();

        // Import it back
        let imported = ImportedObjectStore::try_from(ffi_store).unwrap();

        // Verify Display works correctly (passes through unchanged)
        let imported_display = format!("{}", imported);
        assert_eq!(original_display, imported_display);

        // Verify Debug wraps in ImportedObjectStore
        let imported_debug = format!("{:?}", imported);
        assert!(imported_debug.contains("ImportedObjectStore"));
        assert!(imported_debug.contains(&original_debug));
    }

    #[test]
    fn test_invalid_import() {
        // Create an empty/released SedonaCObjectStore
        let empty = SedonaCObjectStore::default();
        let result = ImportedObjectStore::try_from(empty);
        assert!(result.is_err());
    }
}
