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

#![deny(missing_docs)]

//! Import and export [`sedona_catalog`] implementations through the Sedona C ABI.

use std::ffi::{c_char, c_int, CString};
use std::fmt::{Debug, Formatter};
use std::ptr::null_mut;
use std::sync::{Arc, Weak};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{not_impl_err, Result};
use datafusion_physical_plan::ExecutionPlan;
use sedona_catalog::{
    CreateTableOptions, DropSchemaOptions, DropTableOptions, SedonaCatalog, SedonaCatalogList,
    SedonaSchema,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::execution_plan::{ExportedExecutionPlan, ImportedSedonaCExec};
use crate::extension::{
    SedonaCCatalogProvider, SedonaCCatalogProviderList, SedonaCError, SedonaCExecutionPlan,
    SedonaCSchemaProvider, SedonaCTableProvider,
};
use crate::runtime::RuntimeHandle;
use crate::set_ffi_error;
use crate::table_provider::{ExportedTableProvider, ImportedTableProvider};
use crate::utils::{
    call_get_json_property_impl, cstr_from_ptr_or_empty, parse_json_c_args, write_json_property,
    write_utf8_property_schema, ERRNO_OK,
};

/// Exports a [`SedonaCatalogList`] through [`SedonaCCatalogProviderList`].
pub struct ExportedCatalogProviderList {
    inner: Arc<dyn SedonaCatalogList>,
    session: Arc<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl Debug for ExportedCatalogProviderList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedCatalogProviderList")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ExportedCatalogProviderList {
    /// Create an exported catalog list using `session` and `runtime` for its providers.
    pub fn new(
        inner: Arc<dyn SedonaCatalogList>,
        session: Arc<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Self {
        Self {
            inner,
            session,
            runtime,
        }
    }
}

impl From<ExportedCatalogProviderList> for SedonaCCatalogProviderList {
    fn from(value: ExportedCatalogProviderList) -> Self {
        Self {
            get_property_schema: Some(c_catalog_list_property_schema),
            get_property: Some(c_catalog_list_property),
            catalog: Some(c_catalog_list_catalog),
            create_catalog: Some(c_catalog_list_create),
            reserved: null_mut(),
            release: Some(c_catalog_list_release),
            private_data: Box::into_raw(Box::new(value)).cast(),
        }
    }
}

unsafe extern "C" fn c_catalog_list_property_schema(
    _self_: *const SedonaCCatalogProviderList,
    _property: *const c_char,
    out: *mut FFI_ArrowSchema,
    err: *mut SedonaCError,
) -> c_int {
    write_utf8_property_schema(out, err)
}

unsafe extern "C" fn c_catalog_list_property(
    self_: *const SedonaCCatalogProviderList,
    property: *const c_char,
    _args: *const c_char,
    out: *mut FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProviderList);
    match cstr_from_ptr_or_empty(property).as_ref() {
        "catalog_names" => write_json_property(&exported.inner.catalog_names(), out, err),
        property => {
            set_ffi_error!(err, "Unknown catalog list property: {}", property);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_catalog_list_catalog(
    self_: *const SedonaCCatalogProviderList,
    name: *const c_char,
    out: *mut SedonaCCatalogProvider,
    _err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProviderList);
    let result = exported
        .inner
        .catalog(&cstr_from_ptr_or_empty(name))
        .map(|inner| {
            ExportedCatalogProvider::new(inner, exported.session.clone(), exported.runtime.clone())
                .into()
        })
        .unwrap_or_default();
    std::ptr::write(out, result);
    ERRNO_OK
}

unsafe extern "C" fn c_catalog_list_create(
    self_: *const SedonaCCatalogProviderList,
    name: *const c_char,
    out: *mut SedonaCCatalogProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProviderList);
    match exported.inner.create(&cstr_from_ptr_or_empty(name)) {
        Ok(inner) => {
            std::ptr::write(
                out,
                ExportedCatalogProvider::new(
                    inner,
                    exported.session.clone(),
                    exported.runtime.clone(),
                )
                .into(),
            );
            ERRNO_OK
        }
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_catalog_list_release(self_: *mut SedonaCCatalogProviderList) {
    let this = &mut *self_;
    if !this.private_data.is_null() {
        drop(Box::from_raw(
            this.private_data as *mut ExportedCatalogProviderList,
        ));
        this.private_data = null_mut();
    }
    this.release = None;
}

/// Imports a [`SedonaCCatalogProviderList`] as a [`SedonaCatalogList`].
pub struct ImportedCatalogProviderList {
    inner: SedonaCCatalogProviderList,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl Debug for ImportedCatalogProviderList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedCatalogProviderList").finish()
    }
}

impl ImportedCatalogProviderList {
    /// Import a catalog list after validating its required C callbacks.
    pub fn try_new(
        inner: SedonaCCatalogProviderList,
        session: Weak<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Result<Self> {
        if inner.release.is_none() {
            return sedona_common::sedona_internal_err!(
                "SedonaCCatalogProviderList does not have a release callback"
            );
        }
        if inner.get_property_schema.is_none()
            || inner.get_property.is_none()
            || inner.catalog.is_none()
        {
            return sedona_common::sedona_internal_err!(
                "SedonaCCatalogProviderList is missing a required callback"
            );
        }
        Ok(Self {
            inner,
            session,
            runtime,
        })
    }

    /// Create a catalog, preserving any error returned by the FFI callback.
    pub fn try_create_catalog(&self, name: String) -> Result<Arc<dyn SedonaCatalog>> {
        let Some(callback) = self.inner.create_catalog else {
            return not_impl_err!("Creating catalogs is not supported by the foreign catalog list");
        };
        let name = c_string(name)?;
        let mut out = SedonaCCatalogProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to create catalog: {error}");
        }
        optional_catalog(out, self.session.clone(), self.runtime.clone())?.ok_or_else(|| {
            datafusion_common::DataFusionError::External(
                "Create catalog callback returned no catalog".into(),
            )
        })
    }

    /// Return catalog names, preserving any property error from FFI.
    pub fn try_catalog_names(&self) -> Result<Vec<String>> {
        let callback = self.inner.get_property.expect("validated in try_new");
        let schema_callback = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            "catalog_names",
            "SedonaCCatalogProviderList",
            None::<&()>,
            |property, out, err| unsafe { schema_callback(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { callback(&self.inner, property, args, out, err) },
        )
    }

    /// Look up a catalog, preserving any error returned by the FFI callback.
    pub fn try_catalog(&self, name: &str) -> Result<Option<Arc<dyn SedonaCatalog>>> {
        let callback = self.inner.catalog.expect("validated in try_new");
        let name = c_string(name)?;
        let mut out = SedonaCCatalogProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to get catalog: {error}");
        }
        optional_catalog(out, self.session.clone(), self.runtime.clone())
    }
}

impl SedonaCatalogList for ImportedCatalogProviderList {
    fn catalog_names(&self) -> Vec<String> {
        self.try_catalog_names().unwrap_or_default()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn SedonaCatalog>> {
        self.try_catalog(name).ok().flatten()
    }

    fn create(&self, name: &str) -> Result<Arc<dyn SedonaCatalog>> {
        self.try_create_catalog(name.to_owned())
    }
}

/// Exports a [`SedonaCatalog`] through [`SedonaCCatalogProvider`].
pub struct ExportedCatalogProvider {
    inner: Arc<dyn SedonaCatalog>,
    session: Arc<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl Debug for ExportedCatalogProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedCatalogProvider")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ExportedCatalogProvider {
    /// Create an exported catalog using `session` and `runtime` for its providers.
    pub fn new(
        inner: Arc<dyn SedonaCatalog>,
        session: Arc<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Self {
        Self {
            inner,
            session,
            runtime,
        }
    }
}

impl From<ExportedCatalogProvider> for SedonaCCatalogProvider {
    fn from(value: ExportedCatalogProvider) -> Self {
        Self {
            get_property_schema: Some(c_catalog_property_schema),
            get_property: Some(c_catalog_property),
            schema: Some(c_catalog_schema),
            create_schema: Some(c_catalog_create_schema),
            drop_schema: Some(c_catalog_drop_schema),
            reserved: null_mut(),
            release: Some(c_catalog_release),
            private_data: Box::into_raw(Box::new(value)).cast(),
        }
    }
}

unsafe extern "C" fn c_catalog_property_schema(
    _self_: *const SedonaCCatalogProvider,
    _property: *const c_char,
    out: *mut FFI_ArrowSchema,
    err: *mut SedonaCError,
) -> c_int {
    write_utf8_property_schema(out, err)
}

unsafe extern "C" fn c_catalog_property(
    self_: *const SedonaCCatalogProvider,
    property: *const c_char,
    _args: *const c_char,
    out: *mut FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProvider);
    match cstr_from_ptr_or_empty(property).as_ref() {
        "schema_names" => write_json_property(&exported.inner.schema_names(), out, err),
        property => {
            set_ffi_error!(err, "Unknown catalog property: {}", property);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_catalog_schema(
    self_: *const SedonaCCatalogProvider,
    name: *const c_char,
    out: *mut SedonaCSchemaProvider,
    _err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProvider);
    let result = exported
        .inner
        .schema(&cstr_from_ptr_or_empty(name))
        .map(|inner| {
            ExportedSchemaProvider::new(inner, exported.session.clone(), exported.runtime.clone())
                .into()
        })
        .unwrap_or_default();
    std::ptr::write(out, result);
    ERRNO_OK
}

unsafe extern "C" fn c_catalog_create_schema(
    self_: *const SedonaCCatalogProvider,
    name: *const c_char,
    out: *mut SedonaCSchemaProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProvider);
    match exported.inner.create(&cstr_from_ptr_or_empty(name)) {
        Ok(inner) => {
            std::ptr::write(
                out,
                ExportedSchemaProvider::new(
                    inner,
                    exported.session.clone(),
                    exported.runtime.clone(),
                )
                .into(),
            );
            ERRNO_OK
        }
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_catalog_drop_schema(
    self_: *const SedonaCCatalogProvider,
    name: *const c_char,
    options: *const u8,
    options_len: usize,
    out: *mut SedonaCSchemaProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedCatalogProvider);
    let options = match parse_json_options(options, options_len) {
        Ok(options) => options,
        Err(error) => {
            set_ffi_error!(err, "Failed to parse drop schema options: {}", error);
            return libc::EINVAL;
        }
    };
    match exported
        .inner
        .drop_schema(&cstr_from_ptr_or_empty(name), &options)
    {
        Ok(result) => {
            let result = result
                .map(|inner| {
                    ExportedSchemaProvider::new(
                        inner,
                        exported.session.clone(),
                        exported.runtime.clone(),
                    )
                    .into()
                })
                .unwrap_or_default();
            std::ptr::write(out, result);
            ERRNO_OK
        }
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_catalog_release(self_: *mut SedonaCCatalogProvider) {
    let this = &mut *self_;
    if !this.private_data.is_null() {
        drop(Box::from_raw(
            this.private_data as *mut ExportedCatalogProvider,
        ));
        this.private_data = null_mut();
    }
    this.release = None;
}

/// Imports a [`SedonaCCatalogProvider`] as a [`SedonaCatalog`].
pub struct ImportedCatalogProvider {
    inner: SedonaCCatalogProvider,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl Debug for ImportedCatalogProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedCatalogProvider").finish()
    }
}

impl ImportedCatalogProvider {
    /// Import a catalog after validating its required C callbacks.
    pub fn try_new(
        inner: SedonaCCatalogProvider,
        session: Weak<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Result<Self> {
        if inner.release.is_none() {
            return sedona_common::sedona_internal_err!(
                "SedonaCCatalogProvider does not have a release callback"
            );
        }
        if inner.get_property_schema.is_none()
            || inner.get_property.is_none()
            || inner.schema.is_none()
        {
            return sedona_common::sedona_internal_err!(
                "SedonaCCatalogProvider is missing a required callback"
            );
        }
        Ok(Self {
            inner,
            session,
            runtime,
        })
    }

    /// Return schema names, preserving any property error from FFI.
    pub fn try_schema_names(&self) -> Result<Vec<String>> {
        let callback = self.inner.get_property.expect("validated in try_new");
        let schema_callback = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            "schema_names",
            "SedonaCCatalogProvider",
            None::<&()>,
            |property, out, err| unsafe { schema_callback(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { callback(&self.inner, property, args, out, err) },
        )
    }

    /// Look up a schema, preserving any error returned by the FFI callback.
    pub fn try_schema(&self, name: &str) -> Result<Option<Arc<dyn SedonaSchema>>> {
        let callback = self.inner.schema.expect("validated in try_new");
        let name = c_string(name)?;
        let mut out = SedonaCSchemaProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to get schema: {error}");
        }
        optional_schema(out, self.session.clone(), self.runtime.clone())
    }

    /// Create a schema, preserving any error returned by the FFI callback.
    pub fn try_create_schema(&self, name: &str) -> Result<Arc<dyn SedonaSchema>> {
        let Some(callback) = self.inner.create_schema else {
            return not_impl_err!("Creating schemas is not supported by the foreign catalog");
        };
        let name = c_string(name)?;
        let mut out = SedonaCSchemaProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to create schema: {error}");
        }
        optional_schema(out, self.session.clone(), self.runtime.clone())?.ok_or_else(|| {
            datafusion_common::DataFusionError::External(
                "Create schema callback returned no schema".into(),
            )
        })
    }
}

impl SedonaCatalog for ImportedCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.try_schema_names().unwrap_or_default()
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SedonaSchema>> {
        self.try_schema(name).ok().flatten()
    }

    fn create(&self, name: &str) -> Result<Arc<dyn SedonaSchema>> {
        self.try_create_schema(name)
    }

    fn drop_schema(
        &self,
        name: &str,
        options: &DropSchemaOptions,
    ) -> Result<Option<Arc<dyn SedonaSchema>>> {
        let Some(callback) = self.inner.drop_schema else {
            return not_impl_err!("Dropping schemas is not supported by the foreign catalog");
        };
        let name = c_string(name)?;
        let options = serialize_json_options(options)?;
        let mut out = SedonaCSchemaProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe {
            callback(
                &self.inner,
                name.as_ptr(),
                options.as_ptr(),
                options.len(),
                &mut out,
                &mut error,
            )
        };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to drop schema: {error}");
        }
        optional_schema(out, self.session.clone(), self.runtime.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableExistArgs {
    name: String,
}

/// Exports a [`SedonaSchema`] through [`SedonaCSchemaProvider`].
pub struct ExportedSchemaProvider {
    inner: Arc<dyn SedonaSchema>,
    session: Arc<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl Debug for ExportedSchemaProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedSchemaProvider")
            .field("inner", &self.inner)
            .finish()
    }
}

impl ExportedSchemaProvider {
    /// Create an exported schema using `session` and `runtime` for its providers.
    pub fn new(
        inner: Arc<dyn SedonaSchema>,
        session: Arc<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Self {
        Self {
            inner,
            session,
            runtime,
        }
    }

    fn table(&self, name: String) -> Result<Option<Arc<dyn TableProvider>>> {
        let inner = self.inner.clone();
        let runtime = self.runtime.clone();
        std::thread::spawn(move || runtime.handle().block_on(inner.table(&name)))
            .join()
            .map_err(|error| {
                datafusion_common::DataFusionError::External(
                    format!("Table lookup thread panicked: {error:?}").into(),
                )
            })?
    }
}

impl From<ExportedSchemaProvider> for SedonaCSchemaProvider {
    fn from(value: ExportedSchemaProvider) -> Self {
        Self {
            get_property_schema: Some(c_schema_property_schema),
            get_property: Some(c_schema_property),
            table: Some(c_schema_table),
            create_table: Some(c_schema_create_table),
            drop_table: Some(c_schema_drop_table),
            reserved: null_mut(),
            release: Some(c_schema_release),
            private_data: Box::into_raw(Box::new(value)).cast(),
        }
    }
}

unsafe extern "C" fn c_schema_property_schema(
    _self_: *const SedonaCSchemaProvider,
    _property: *const c_char,
    out: *mut FFI_ArrowSchema,
    err: *mut SedonaCError,
) -> c_int {
    write_utf8_property_schema(out, err)
}

unsafe extern "C" fn c_schema_property(
    self_: *const SedonaCSchemaProvider,
    property: *const c_char,
    args: *const c_char,
    out: *mut FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedSchemaProvider);
    match cstr_from_ptr_or_empty(property).as_ref() {
        "owner_name" => write_json_property(&exported.inner.owner_name(), out, err),
        "table_names" => write_json_property(&exported.inner.table_names(), out, err),
        "table_exist" => match parse_json_c_args::<TableExistArgs>(args) {
            Ok(args) => write_json_property(&exported.inner.table_exist(&args.name), out, err),
            Err(error) => {
                set_ffi_error!(err, "Failed to parse table_exist arguments: {}", error);
                libc::EINVAL
            }
        },
        property => {
            set_ffi_error!(err, "Unknown schema property: {}", property);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_schema_table(
    self_: *const SedonaCSchemaProvider,
    name: *const c_char,
    out: *mut SedonaCTableProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedSchemaProvider);
    match exported.table(cstr_from_ptr_or_empty(name).into_owned()) {
        Ok(result) => {
            let result = result
                .map(|inner| {
                    ExportedTableProvider::new(
                        inner,
                        exported.session.clone(),
                        exported.runtime.clone(),
                    )
                    .into()
                })
                .unwrap_or_default();
            std::ptr::write(out, result);
            ERRNO_OK
        }
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_schema_create_table(
    self_: *const SedonaCSchemaProvider,
    name: *const c_char,
    options: *const u8,
    options_len: usize,
    plan: *mut SedonaCExecutionPlan,
    out: *mut SedonaCExecutionPlan,
    err: *mut SedonaCError,
) -> c_int {
    if plan.is_null() {
        set_ffi_error!(err, "Input execution plan is null");
        return libc::EINVAL;
    }
    let exported = &*((*self_).private_data as *const ExportedSchemaProvider);
    let options = match parse_json_options(options, options_len) {
        Ok(options) => options,
        Err(error) => {
            set_ffi_error!(err, "Failed to parse create table options: {}", error);
            return libc::EINVAL;
        }
    };
    let input = std::ptr::replace(plan, SedonaCExecutionPlan::default());
    let input = match ImportedSedonaCExec::try_new(input) {
        Ok(input) => Arc::new(input) as Arc<dyn ExecutionPlan>,
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            return libc::EINVAL;
        }
    };
    match exported
        .inner
        .create(&cstr_from_ptr_or_empty(name), &options, input)
    {
        Ok(plan) => {
            std::ptr::write(
                out,
                ExportedExecutionPlan::new(
                    plan,
                    exported.session.task_ctx(),
                    exported.runtime.clone(),
                )
                .into(),
            );
            ERRNO_OK
        }
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_schema_drop_table(
    self_: *const SedonaCSchemaProvider,
    name: *const c_char,
    options: *const u8,
    options_len: usize,
    out: *mut SedonaCTableProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = &*((*self_).private_data as *const ExportedSchemaProvider);
    let options = match parse_json_options(options, options_len) {
        Ok(options) => options,
        Err(error) => {
            set_ffi_error!(err, "Failed to parse drop table options: {}", error);
            return libc::EINVAL;
        }
    };
    match exported
        .inner
        .drop_table(&cstr_from_ptr_or_empty(name), &options)
    {
        Ok(result) => {
            let result = result
                .map(|inner| {
                    ExportedTableProvider::new(
                        inner,
                        exported.session.clone(),
                        exported.runtime.clone(),
                    )
                    .into()
                })
                .unwrap_or_default();
            std::ptr::write(out, result);
            ERRNO_OK
        }
        Err(error) => {
            set_ffi_error!(err, "{}", error);
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_schema_release(self_: *mut SedonaCSchemaProvider) {
    let this = &mut *self_;
    if !this.private_data.is_null() {
        drop(Box::from_raw(
            this.private_data as *mut ExportedSchemaProvider,
        ));
        this.private_data = null_mut();
    }
    this.release = None;
}

/// Imports a [`SedonaCSchemaProvider`] as a [`SedonaSchema`].
pub struct ImportedSchemaProvider {
    inner: SedonaCSchemaProvider,
    owner_name: Option<String>,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl Debug for ImportedSchemaProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedSchemaProvider").finish()
    }
}

impl ImportedSchemaProvider {
    /// Import a schema after validating its required C callbacks and properties.
    pub fn try_new(
        inner: SedonaCSchemaProvider,
        session: Weak<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Result<Self> {
        if inner.release.is_none() {
            return sedona_common::sedona_internal_err!(
                "SedonaCSchemaProvider does not have a release callback"
            );
        }
        let Some(callback) = inner.get_property else {
            return sedona_common::sedona_internal_err!(
                "SedonaCSchemaProvider does not have get_property"
            );
        };
        if inner.get_property_schema.is_none() || inner.table.is_none() {
            return sedona_common::sedona_internal_err!(
                "SedonaCSchemaProvider is missing a required callback"
            );
        }
        let owner_name = call_get_json_property_impl(
            "owner_name",
            "SedonaCSchemaProvider",
            None::<&()>,
            |property, out, err| unsafe {
                inner.get_property_schema.expect("validated above")(&inner, property, out, err)
            },
            |property, args, out, err| unsafe { callback(&inner, property, args, out, err) },
        )?;
        Ok(Self {
            inner,
            owner_name,
            session,
            runtime,
        })
    }

    fn property<T: DeserializeOwned>(&self, property: &str) -> Result<T> {
        let callback = self.inner.get_property.expect("validated in try_new");
        let schema_callback = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            property,
            "SedonaCSchemaProvider",
            None::<&()>,
            |property, out, err| unsafe { schema_callback(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { callback(&self.inner, property, args, out, err) },
        )
    }

    /// Return table names, preserving any property error from FFI.
    pub fn try_table_names(&self) -> Result<Vec<String>> {
        self.property("table_names")
    }

    /// Test whether a table exists, preserving any property error from FFI.
    pub fn try_table_exist(&self, name: &str) -> Result<bool> {
        let callback = self.inner.get_property.expect("validated in try_new");
        let schema_callback = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            "table_exist",
            "SedonaCSchemaProvider",
            Some(&TableExistArgs {
                name: name.to_owned(),
            }),
            |property, out, err| unsafe { schema_callback(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { callback(&self.inner, property, args, out, err) },
        )
    }

    /// Create a table by executing `plan` in the foreign schema.
    pub fn try_create_table(
        &self,
        name: String,
        options: &CreateTableOptions,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(callback) = self.inner.create_table else {
            return not_impl_err!("Creating tables is not supported by the foreign schema");
        };
        let name = c_string(name)?;
        let options = serialize_json_options(options)?;
        let session = upgrade_session(&self.session)?;
        let mut input =
            ExportedExecutionPlan::new(plan, session.task_ctx(), self.runtime.clone()).into();
        let mut out = SedonaCExecutionPlan::default();
        let mut error = SedonaCError::default();
        let code = unsafe {
            callback(
                &self.inner,
                name.as_ptr(),
                options.as_ptr(),
                options.len(),
                &mut input,
                &mut out,
                &mut error,
            )
        };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to create table: {error}");
        }
        if out.release.is_none() {
            return sedona_common::sedona_internal_err!(
                "Create table callback returned no execution plan"
            );
        }
        Ok(Arc::new(ImportedSedonaCExec::try_new(out)?))
    }
}

#[async_trait]
impl SedonaSchema for ImportedSchemaProvider {
    fn owner_name(&self) -> Option<&str> {
        self.owner_name.as_deref()
    }
    fn table_names(&self) -> Vec<String> {
        self.try_table_names().unwrap_or_default()
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let callback = self.inner.table.expect("validated in try_new");
        let name = c_string(name)?;
        let mut out = SedonaCTableProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to get table: {error}");
        }
        optional_table(out)
    }
    fn create(
        &self,
        name: &str,
        options: &CreateTableOptions,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.try_create_table(name.to_owned(), options, plan)
    }

    fn drop_table(
        &self,
        name: &str,
        options: &DropTableOptions,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(callback) = self.inner.drop_table else {
            return not_impl_err!("Dropping tables is not supported by the foreign schema");
        };
        let name = c_string(name)?;
        let options = serialize_json_options(options)?;
        let mut out = SedonaCTableProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe {
            callback(
                &self.inner,
                name.as_ptr(),
                options.as_ptr(),
                options.len(),
                &mut out,
                &mut error,
            )
        };
        if code != ERRNO_OK {
            return sedona_common::sedona_internal_err!("Failed to drop table: {error}");
        }
        optional_table(out)
    }
    fn table_exist(&self, name: &str) -> bool {
        self.try_table_exist(name).unwrap_or(false)
    }
}

fn serialize_json_options<T: Serialize>(options: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(options).map_err(|error| {
        datafusion_common::DataFusionError::External(
            format!("Failed to serialize catalog options: {error}").into(),
        )
    })
}

unsafe fn parse_json_options<T: DeserializeOwned + Default>(
    options: *const u8,
    options_len: usize,
) -> Result<T> {
    if options_len == 0 {
        return Ok(T::default());
    }
    if options.is_null() {
        return sedona_common::sedona_internal_err!(
            "Catalog options pointer is null but options_len is non-zero"
        );
    }
    let options = unsafe { std::slice::from_raw_parts(options, options_len) };
    serde_json::from_slice(options).map_err(|error| {
        datafusion_common::DataFusionError::External(
            format!("Failed to parse catalog options: {error}").into(),
        )
    })
}

fn c_string(value: impl Into<Vec<u8>>) -> Result<CString> {
    CString::new(value).map_err(|error| {
        datafusion_common::DataFusionError::External(
            format!("Catalog name contains an interior NUL: {error}").into(),
        )
    })
}

fn optional_catalog(
    raw: SedonaCCatalogProvider,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
) -> Result<Option<Arc<dyn SedonaCatalog>>> {
    if raw.release.is_none() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(ImportedCatalogProvider::try_new(
            raw, session, runtime,
        )?)))
    }
}

fn optional_schema(
    raw: SedonaCSchemaProvider,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
) -> Result<Option<Arc<dyn SedonaSchema>>> {
    if raw.release.is_none() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(ImportedSchemaProvider::try_new(
            raw, session, runtime,
        )?)))
    }
}

fn upgrade_session(session: &Weak<dyn Session>) -> Result<Arc<dyn Session>> {
    session.upgrade().ok_or_else(|| {
        datafusion_common::DataFusionError::External(
            "Cannot export a provider after its session has been dropped".into(),
        )
    })
}

fn optional_table(raw: SedonaCTableProvider) -> Result<Option<Arc<dyn TableProvider>>> {
    if raw.release.is_none() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(ImportedTableProvider::try_new(raw)?)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::RwLock;

    use super::*;
    use arrow_schema::Schema;
    use datafusion::catalog::CatalogProviderList;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
    use sedona_catalog::{CatalogObjectType, DataFusionCatalog, DataFusionCatalogList};

    #[derive(Debug, Default)]
    struct TestCatalogList {
        catalogs: RwLock<HashMap<String, Arc<dyn SedonaCatalog>>>,
    }

    impl SedonaCatalogList for TestCatalogList {
        fn catalog_names(&self) -> Vec<String> {
            self.catalogs.read().unwrap().keys().cloned().collect()
        }

        fn catalog(&self, name: &str) -> Option<Arc<dyn SedonaCatalog>> {
            self.catalogs.read().unwrap().get(name).cloned()
        }

        fn create(&self, name: &str) -> Result<Arc<dyn SedonaCatalog>> {
            let catalog: Arc<dyn SedonaCatalog> = Arc::new(TestCatalog::default());
            self.catalogs
                .write()
                .unwrap()
                .insert(name.to_owned(), catalog.clone());
            Ok(catalog)
        }
    }

    #[derive(Debug, Default)]
    struct TestCatalog {
        schemas: RwLock<HashMap<String, Arc<dyn SedonaSchema>>>,
    }

    impl SedonaCatalog for TestCatalog {
        fn schema_names(&self) -> Vec<String> {
            self.schemas.read().unwrap().keys().cloned().collect()
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SedonaSchema>> {
            self.schemas.read().unwrap().get(name).cloned()
        }

        fn create(&self, name: &str) -> Result<Arc<dyn SedonaSchema>> {
            let schema: Arc<dyn SedonaSchema> = Arc::new(TestSchema::default());
            self.schemas
                .write()
                .unwrap()
                .insert(name.to_owned(), schema.clone());
            Ok(schema)
        }

        fn drop_schema(
            &self,
            name: &str,
            options: &DropSchemaOptions,
        ) -> Result<Option<Arc<dyn SedonaSchema>>> {
            if name == "schema_two" && !options.cascade {
                return sedona_common::sedona_internal_err!(
                    "schema_two must be dropped with cascade"
                );
            }
            Ok(self.schemas.write().unwrap().remove(name))
        }
    }

    #[derive(Debug, Default)]
    struct TestSchema {
        tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    }

    #[async_trait]
    impl SedonaSchema for TestSchema {
        fn table_names(&self) -> Vec<String> {
            self.tables.read().unwrap().keys().cloned().collect()
        }

        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            Ok(self.tables.read().unwrap().get(name).cloned())
        }

        fn create(
            &self,
            _name: &str,
            options: &CreateTableOptions,
            input: Arc<dyn ExecutionPlan>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if !options.temporary {
                return sedona_common::sedona_internal_err!(
                    "test tables must be created as temporary"
                );
            }
            Ok(input)
        }

        fn drop_table(
            &self,
            name: &str,
            options: &DropTableOptions,
        ) -> Result<Option<Arc<dyn TableProvider>>> {
            if name == "table_one"
                && (!options.purge || options.object_type != Some(CatalogObjectType::Table))
            {
                return sedona_common::sedona_internal_err!(
                    "table_one must be dropped as a purged table"
                );
            }
            if name == "view_one" && options.object_type != Some(CatalogObjectType::View) {
                return sedona_common::sedona_internal_err!("view_one must be dropped as a view");
            }
            Ok(self.tables.write().unwrap().remove(name))
        }

        fn table_exist(&self, name: &str) -> bool {
            self.tables.read().unwrap().contains_key(name)
        }
    }

    unsafe extern "C" fn failing_create_catalog(
        _self_: *const SedonaCCatalogProviderList,
        _name: *const c_char,
        _out: *mut SedonaCCatalogProvider,
        error: *mut SedonaCError,
    ) -> c_int {
        crate::extension::write_ffi_error(error, "catalog creation failed");
        libc::EIO
    }

    unsafe extern "C" fn passthrough_create_table(
        _self_: *const SedonaCSchemaProvider,
        _name: *const c_char,
        options: *const u8,
        options_len: usize,
        plan: *mut SedonaCExecutionPlan,
        out: *mut SedonaCExecutionPlan,
        error: *mut SedonaCError,
    ) -> c_int {
        if plan.is_null() {
            crate::extension::write_ffi_error(error, "execution plan is null");
            return libc::EINVAL;
        }
        let options: CreateTableOptions = match parse_json_options(options, options_len) {
            Ok(options) => options,
            Err(error_value) => {
                crate::extension::write_ffi_error(error, &error_value.to_string());
                return libc::EINVAL;
            }
        };
        if !options.temporary {
            crate::extension::write_ffi_error(error, "temporary option is false");
            return libc::EINVAL;
        }
        let plan = std::ptr::replace(plan, SedonaCExecutionPlan::default());
        std::ptr::write(out, plan);
        ERRNO_OK
    }

    fn runtime() -> Arc<RuntimeHandle> {
        Arc::new(RuntimeHandle::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        ))
    }

    fn empty_table() -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
    }

    #[test]
    fn empty_json_options_use_defaults() {
        let create: CreateTableOptions =
            unsafe { parse_json_options(std::ptr::null(), 0) }.unwrap();
        let drop_schema: DropSchemaOptions =
            unsafe { parse_json_options(std::ptr::null(), 0) }.unwrap();
        let drop_table: DropTableOptions =
            unsafe { parse_json_options(std::ptr::null(), 0) }.unwrap();

        assert_eq!(create, CreateTableOptions::default());
        assert_eq!(drop_schema, DropSchemaOptions::default());
        assert_eq!(drop_table, DropTableOptions::default());
    }

    fn round_trip() -> (
        ImportedCatalogProviderList,
        Arc<dyn Session>,
        Arc<RuntimeHandle>,
        Arc<RuntimeHandle>,
    ) {
        let schema = Arc::new(TestSchema::default());
        schema
            .tables
            .write()
            .unwrap()
            .insert("table_one".to_owned(), empty_table());

        let catalog = Arc::new(TestCatalog::default());
        catalog
            .schemas
            .write()
            .unwrap()
            .insert("schema_one".to_owned(), schema);

        let catalogs = Arc::new(TestCatalogList::default());
        catalogs
            .catalogs
            .write()
            .unwrap()
            .insert("catalog_one".to_owned(), catalog);

        let producer_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let consumer_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let producer_runtime = runtime();
        let consumer_runtime = runtime();
        let raw =
            ExportedCatalogProviderList::new(catalogs, producer_session, producer_runtime.clone())
                .into();
        let imported = ImportedCatalogProviderList::try_new(
            raw,
            Arc::downgrade(&consumer_session),
            consumer_runtime.clone(),
        )
        .unwrap();
        (
            imported,
            consumer_session,
            consumer_runtime,
            producer_runtime,
        )
    }

    #[test]
    fn round_trips_the_catalog_hierarchy() {
        let (catalogs, _consumer_session, runtime, _producer_runtime) = round_trip();
        assert_eq!(catalogs.catalog_names(), vec!["catalog_one"]);

        let catalog = catalogs.catalog("catalog_one").unwrap();
        assert_eq!(catalog.schema_names(), vec!["schema_one"]);

        let schema = catalog.schema("schema_one").unwrap();
        assert_eq!(schema.table_names(), vec!["table_one"]);
        assert!(schema.table_exist("table_one"));
        assert!(!schema.table_exist("missing"));

        let table = runtime
            .block_on(schema.table("table_one"))
            .unwrap()
            .unwrap();
        assert_eq!(table.schema().fields().len(), 0);
        assert!(runtime.block_on(schema.table("missing")).unwrap().is_none());

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
        let create = schema
            .create(
                "temporary_table",
                &CreateTableOptions { temporary: true },
                input,
            )
            .unwrap();
        assert!(create.name().contains("PlaceholderRowExec"));

        let drop_options = DropTableOptions {
            object_type: Some(CatalogObjectType::Table),
            purge: true,
        };
        assert!(schema
            .drop_table("table_one", &drop_options)
            .unwrap()
            .is_some());
        assert!(!schema.table_exist("table_one"));
        assert!(schema
            .drop_table("table_one", &drop_options)
            .unwrap()
            .is_none());

        let view_drop_options = DropTableOptions {
            object_type: Some(CatalogObjectType::View),
            purge: false,
        };
        assert!(schema
            .drop_table("view_one", &view_drop_options)
            .unwrap()
            .is_none());
    }

    #[test]
    fn creates_catalogs_and_schemas() {
        let (catalogs, consumer_session, runtime, _producer_runtime) = round_trip();
        let weak_consumer_session = Arc::downgrade(&consumer_session);
        let catalog = catalogs
            .try_create_catalog("catalog_two".to_owned())
            .unwrap();
        assert!(catalogs.catalog("catalog_two").is_some());

        catalog.create("schema_two").unwrap();
        assert!(catalog.schema("schema_two").is_some());
        let drop_options = DropSchemaOptions { cascade: true };
        assert!(catalog
            .drop_schema("schema_two", &drop_options)
            .unwrap()
            .is_some());
        assert!(catalog.schema("schema_two").is_none());
        assert!(catalog
            .drop_schema("schema_two", &drop_options)
            .unwrap()
            .is_none());
        drop(consumer_session);
        assert!(weak_consumer_session.upgrade().is_none());
        drop(runtime);
    }

    #[test]
    fn datafusion_adapter_reads_imported_catalogs() {
        let (catalogs, _consumer_session, runtime, _producer_runtime) = round_trip();
        let catalogs = DataFusionCatalogList::new(Arc::new(catalogs));
        let catalog = catalogs.catalog("catalog_one").unwrap();
        let schema = catalog.schema("schema_one").unwrap();
        assert!(runtime
            .block_on(schema.table("table_one"))
            .unwrap()
            .is_some());
    }

    #[test]
    fn create_table_returns_execution_plan_without_executing_it() {
        let producer_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let consumer_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let producer_runtime = runtime();
        let consumer_runtime = runtime();
        let mut raw: SedonaCSchemaProvider = ExportedSchemaProvider::new(
            Arc::new(TestSchema::default()),
            producer_session,
            producer_runtime,
        )
        .into();
        raw.create_table = Some(passthrough_create_table);
        let schema = ImportedSchemaProvider::try_new(
            raw,
            Arc::downgrade(&consumer_session),
            consumer_runtime,
        )
        .unwrap();

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
        let create_plan = schema
            .try_create_table(
                "created".to_owned(),
                &CreateTableOptions { temporary: true },
                input,
            )
            .unwrap();

        assert!(create_plan.name().contains("PlaceholderRowExec"));
        assert!(!schema.table_exist("created"));
    }

    #[test]
    fn imported_catalog_does_not_retain_host_session() {
        let host = SessionContext::new();
        let session: Arc<dyn Session> = Arc::new(host.state());
        let weak_session = Arc::downgrade(&session);
        let runtime = runtime();
        let plugin_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let raw = ExportedCatalogProvider::new(
            Arc::new(TestCatalog::default()),
            plugin_session,
            runtime.clone(),
        )
        .into();
        let imported =
            ImportedCatalogProvider::try_new(raw, weak_session.clone(), runtime.clone()).unwrap();
        host.register_catalog(
            "foreign",
            Arc::new(DataFusionCatalog::new(Arc::new(imported))),
        );

        drop(session);
        drop(runtime);
        drop(host);

        assert!(weak_session.upgrade().is_none());
    }

    #[test]
    fn rejects_invalid_raw_providers() {
        let context = SessionContext::new();
        let session: Arc<dyn Session> = Arc::new(context.state());
        let runtime = runtime();

        assert!(ImportedCatalogProviderList::try_new(
            SedonaCCatalogProviderList::default(),
            Arc::downgrade(&session),
            runtime.clone(),
        )
        .is_err());
        assert!(ImportedCatalogProvider::try_new(
            SedonaCCatalogProvider::default(),
            Arc::downgrade(&session),
            runtime.clone(),
        )
        .is_err());
        assert!(ImportedSchemaProvider::try_new(
            SedonaCSchemaProvider::default(),
            Arc::downgrade(&session),
            runtime,
        )
        .is_err());
    }

    #[test]
    fn fallible_catalog_list_methods_preserve_ffi_errors() {
        let context = SessionContext::new();
        let session: Arc<dyn Session> = Arc::new(context.state());
        let runtime = runtime();
        let mut raw: SedonaCCatalogProviderList = ExportedCatalogProviderList::new(
            Arc::new(TestCatalogList::default()),
            session.clone(),
            runtime.clone(),
        )
        .into();
        raw.create_catalog = Some(failing_create_catalog);
        let imported =
            ImportedCatalogProviderList::try_new(raw, Arc::downgrade(&session), runtime).unwrap();

        let error = imported
            .try_create_catalog("catalog".to_owned())
            .unwrap_err();
        assert!(error.to_string().contains("catalog creation failed"));
    }

    #[test]
    fn create_table_callback_invalidates_transferred_plan() {
        let context = SessionContext::new();
        let session = Arc::new(context.state());
        let producer_runtime = runtime();
        let consumer_runtime = runtime();
        let name = CString::new("created").unwrap();
        let mut error = SedonaCError::default();

        let mut raw_schema: SedonaCSchemaProvider = ExportedSchemaProvider::new(
            Arc::new(TestSchema::default()),
            session.clone(),
            producer_runtime,
        )
        .into();
        raw_schema.create_table = Some(passthrough_create_table);
        let plan: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
        let mut input_plan: SedonaCExecutionPlan =
            ExportedExecutionPlan::new(plan, session.task_ctx(), consumer_runtime).into();
        let mut output_plan = SedonaCExecutionPlan::default();
        let options = serde_json::to_vec(&CreateTableOptions { temporary: true }).unwrap();
        let code = unsafe {
            raw_schema.create_table.unwrap()(
                &raw_schema,
                name.as_ptr(),
                options.as_ptr(),
                options.len(),
                &mut input_plan,
                &mut output_plan,
                &mut error,
            )
        };
        assert_eq!(code, ERRNO_OK);
        assert!(input_plan.release.is_none());
        assert!(output_plan.release.is_some());
    }
}
