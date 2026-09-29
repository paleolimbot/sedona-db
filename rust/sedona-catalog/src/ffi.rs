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

//! Import and export [`crate`] catalog implementations through sedona-extension's C ABI.

use std::ffi::{CString, c_char, c_int};
use std::fmt::{Debug, Formatter};
use std::ptr::null_mut;
use std::sync::{Arc, Weak};

use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result, not_impl_err};
use datafusion_physical_plan::ExecutionPlan;
use sedona_extension::execution_plan::{ExportedExecutionPlan, ImportedSedonaCExec};
use sedona_extension::extension::{
    SedonaCCatalogProvider, SedonaCCatalogProviderList, SedonaCError, SedonaCExecutionPlan,
    SedonaCSchemaProvider, SedonaCTableProvider,
};
use sedona_extension::runtime::RuntimeHandle;
use sedona_extension::table_provider::{ExportedTableProvider, ImportedTableProvider};
use sedona_extension::utils::{
    ERRNO_OK, call_get_json_property_impl, cstr_from_ptr_or_empty, parse_json_c_args,
    write_json_property, write_utf8_property_schema,
};
use serde::{Deserialize, Serialize};

use crate::{
    SedonaCatalog, SedonaCatalogList, SedonaCatalogListRef, SedonaCatalogRef, SedonaSchema,
    SedonaSchemaRef,
};

/// A Sedona catalog list exported through [`SedonaCCatalogProviderList`].
pub struct ExportedCatalogList {
    inner: SedonaCatalogListRef,
    session: Arc<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl ExportedCatalogList {
    pub fn new(
        inner: SedonaCatalogListRef,
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

impl Debug for ExportedCatalogList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedCatalogList")
            .field("inner", &self.inner)
            .finish()
    }
}

impl From<ExportedCatalogList> for SedonaCCatalogProviderList {
    fn from(value: ExportedCatalogList) -> Self {
        Self {
            get_property_schema: Some(c_list_property_schema),
            get_property: Some(c_list_property),
            catalog: Some(c_list_catalog),
            create_catalog: Some(c_list_create),
            reserved: null_mut(),
            release: Some(c_list_release),
            private_data: Box::into_raw(Box::new(value)).cast(),
        }
    }
}

unsafe extern "C" fn c_list_property_schema(
    _self_: *const SedonaCCatalogProviderList,
    _property: *const c_char,
    out: *mut FFI_ArrowSchema,
    err: *mut SedonaCError,
) -> c_int {
    unsafe { write_utf8_property_schema(out, err) }
}

unsafe extern "C" fn c_list_property(
    self_: *const SedonaCCatalogProviderList,
    property: *const c_char,
    _args: *const c_char,
    out: *mut FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalogList) };
    match unsafe { cstr_from_ptr_or_empty(property) }.as_ref() {
        "catalog_names" => unsafe {
            write_json_property(&exported.inner.catalog_names(), out, err)
        },
        property => {
            unsafe {
                sedona_extension::set_ffi_error!(err, "Unknown catalog list property: {}", property)
            };
            libc::EINVAL
        }
    }
}

unsafe extern "C" fn c_list_catalog(
    self_: *const SedonaCCatalogProviderList,
    name: *const c_char,
    out: *mut SedonaCCatalogProvider,
    _err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalogList) };
    let value = exported
        .inner
        .catalog(&unsafe { cstr_from_ptr_or_empty(name) })
        .map(|inner| {
            ExportedCatalog::new(inner, exported.session.clone(), exported.runtime.clone()).into()
        })
        .unwrap_or_default();
    unsafe { std::ptr::write(out, value) };
    ERRNO_OK
}

unsafe extern "C" fn c_list_create(
    self_: *const SedonaCCatalogProviderList,
    name: *const c_char,
    out: *mut SedonaCCatalogProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalogList) };
    match exported
        .inner
        .create(&unsafe { cstr_from_ptr_or_empty(name) })
    {
        Ok(inner) => {
            let value =
                ExportedCatalog::new(inner, exported.session.clone(), exported.runtime.clone())
                    .into();
            unsafe { std::ptr::write(out, value) };
            ERRNO_OK
        }
        Err(error) => ffi_error(err, error),
    }
}

unsafe extern "C" fn c_list_release(self_: *mut SedonaCCatalogProviderList) {
    let this = unsafe { &mut *self_ };
    if !this.private_data.is_null() {
        drop(unsafe { Box::from_raw(this.private_data as *mut ExportedCatalogList) });
        this.private_data = null_mut();
    }
    this.release = None;
}

/// A [`SedonaCCatalogProviderList`] imported as a [`SedonaCatalogList`].
pub struct ImportedCatalogList {
    inner: SedonaCCatalogProviderList,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl ImportedCatalogList {
    pub fn try_new(
        inner: SedonaCCatalogProviderList,
        session: Weak<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Result<Self> {
        if inner.release.is_none()
            || inner.get_property_schema.is_none()
            || inner.get_property.is_none()
            || inner.catalog.is_none()
        {
            return sedona_common::sedona_internal_err!(
                "SedonaCCatalogProviderList is released or missing a required callback"
            );
        }
        Ok(Self {
            inner,
            session,
            runtime,
        })
    }

    fn property<T: serde::de::DeserializeOwned>(&self, property: &str) -> Result<T> {
        let get = self.inner.get_property.expect("validated in try_new");
        let schema = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            property,
            "SedonaCCatalogProviderList",
            None::<&()>,
            |property, out, err| unsafe { schema(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { get(&self.inner, property, args, out, err) },
        )
    }
}

impl Debug for ImportedCatalogList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedCatalogList").finish()
    }
}

impl SedonaCatalogList for ImportedCatalogList {
    fn catalog_names(&self) -> Vec<String> {
        self.property("catalog_names").unwrap_or_default()
    }

    fn catalog(&self, name: &str) -> Option<SedonaCatalogRef> {
        self.try_catalog(name).ok().flatten()
    }

    fn create(&self, name: &str) -> Result<SedonaCatalogRef> {
        let callback = self.inner.create_catalog.ok_or_else(|| {
            DataFusionError::NotImplemented(
                "Creating catalogs is not supported by the foreign catalog list".to_string(),
            )
        })?;
        let name = c_string(name)?;
        let mut out = SedonaCCatalogProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        check_code(code, error, "create catalog")?;
        import_catalog(out, self.session.clone(), self.runtime.clone())?.ok_or_else(|| {
            DataFusionError::External("Create catalog callback returned no catalog".into())
        })
    }
}

impl ImportedCatalogList {
    pub fn try_catalog(&self, name: &str) -> Result<Option<SedonaCatalogRef>> {
        let callback = self.inner.catalog.expect("validated in try_new");
        let name = c_string(name)?;
        let mut out = SedonaCCatalogProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        check_code(code, error, "get catalog")?;
        import_catalog(out, self.session.clone(), self.runtime.clone())
    }
}

/// A Sedona catalog exported through [`SedonaCCatalogProvider`].
pub struct ExportedCatalog {
    inner: SedonaCatalogRef,
    session: Arc<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl ExportedCatalog {
    pub fn new(
        inner: SedonaCatalogRef,
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

impl Debug for ExportedCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedCatalog")
            .field("inner", &self.inner)
            .finish()
    }
}

impl From<ExportedCatalog> for SedonaCCatalogProvider {
    fn from(value: ExportedCatalog) -> Self {
        Self {
            get_property_schema: Some(c_catalog_property_schema),
            get_property: Some(c_catalog_property),
            schema: Some(c_catalog_schema),
            create_schema: Some(c_catalog_create),
            deregister_schema: Some(c_catalog_deregister),
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
    unsafe { write_utf8_property_schema(out, err) }
}

unsafe extern "C" fn c_catalog_property(
    self_: *const SedonaCCatalogProvider,
    property: *const c_char,
    _args: *const c_char,
    out: *mut FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalog) };
    match unsafe { cstr_from_ptr_or_empty(property) }.as_ref() {
        "schema_names" => unsafe { write_json_property(&exported.inner.schema_names(), out, err) },
        property => {
            unsafe {
                sedona_extension::set_ffi_error!(err, "Unknown catalog property: {}", property)
            };
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
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalog) };
    let value = exported
        .inner
        .schema(&unsafe { cstr_from_ptr_or_empty(name) })
        .map(|inner| {
            ExportedSchema::new(inner, exported.session.clone(), exported.runtime.clone()).into()
        })
        .unwrap_or_default();
    unsafe { std::ptr::write(out, value) };
    ERRNO_OK
}

unsafe extern "C" fn c_catalog_create(
    self_: *const SedonaCCatalogProvider,
    name: *const c_char,
    out: *mut SedonaCSchemaProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalog) };
    match exported
        .inner
        .create(&unsafe { cstr_from_ptr_or_empty(name) })
    {
        Ok(inner) => {
            let value =
                ExportedSchema::new(inner, exported.session.clone(), exported.runtime.clone())
                    .into();
            unsafe { std::ptr::write(out, value) };
            ERRNO_OK
        }
        Err(error) => ffi_error(err, error),
    }
}

unsafe extern "C" fn c_catalog_deregister(
    self_: *const SedonaCCatalogProvider,
    name: *const c_char,
    cascade: bool,
    out: *mut SedonaCSchemaProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedCatalog) };
    match exported
        .inner
        .deregister(&unsafe { cstr_from_ptr_or_empty(name) }, cascade)
    {
        Ok(value) => {
            let value = value
                .map(|inner| {
                    ExportedSchema::new(inner, exported.session.clone(), exported.runtime.clone())
                        .into()
                })
                .unwrap_or_default();
            unsafe { std::ptr::write(out, value) };
            ERRNO_OK
        }
        Err(error) => ffi_error(err, error),
    }
}

unsafe extern "C" fn c_catalog_release(self_: *mut SedonaCCatalogProvider) {
    let this = unsafe { &mut *self_ };
    if !this.private_data.is_null() {
        drop(unsafe { Box::from_raw(this.private_data as *mut ExportedCatalog) });
        this.private_data = null_mut();
    }
    this.release = None;
}

/// A [`SedonaCCatalogProvider`] imported as a [`SedonaCatalog`].
pub struct ImportedCatalog {
    inner: SedonaCCatalogProvider,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl ImportedCatalog {
    pub fn try_new(
        inner: SedonaCCatalogProvider,
        session: Weak<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Result<Self> {
        if inner.release.is_none()
            || inner.get_property_schema.is_none()
            || inner.get_property.is_none()
            || inner.schema.is_none()
        {
            return sedona_common::sedona_internal_err!(
                "SedonaCCatalogProvider is released or missing a required callback"
            );
        }
        Ok(Self {
            inner,
            session,
            runtime,
        })
    }

    fn property<T: serde::de::DeserializeOwned>(&self, property: &str) -> Result<T> {
        let get = self.inner.get_property.expect("validated in try_new");
        let schema = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            property,
            "SedonaCCatalogProvider",
            None::<&()>,
            |property, out, err| unsafe { schema(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { get(&self.inner, property, args, out, err) },
        )
    }

    fn try_schema(&self, name: &str) -> Result<Option<SedonaSchemaRef>> {
        let callback = self.inner.schema.expect("validated in try_new");
        let name = c_string(name)?;
        let mut out = SedonaCSchemaProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        check_code(code, error, "get schema")?;
        import_schema(out, self.session.clone(), self.runtime.clone())
    }
}

impl Debug for ImportedCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedCatalog").finish()
    }
}

impl SedonaCatalog for ImportedCatalog {
    fn schema_names(&self) -> Vec<String> {
        self.property("schema_names").unwrap_or_default()
    }

    fn schema(&self, name: &str) -> Option<SedonaSchemaRef> {
        self.try_schema(name).ok().flatten()
    }

    fn create(&self, name: &str) -> Result<SedonaSchemaRef> {
        let callback = self.inner.create_schema.ok_or_else(|| {
            DataFusionError::NotImplemented(
                "Creating schemas is not supported by the foreign catalog".to_string(),
            )
        })?;
        let name = c_string(name)?;
        let mut out = SedonaCSchemaProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        check_code(code, error, "create schema")?;
        import_schema(out, self.session.clone(), self.runtime.clone())?.ok_or_else(|| {
            DataFusionError::External("Create schema callback returned no schema".into())
        })
    }

    fn deregister(&self, name: &str, cascade: bool) -> Result<Option<SedonaSchemaRef>> {
        let Some(callback) = self.inner.deregister_schema else {
            return not_impl_err!("Deregistering schemas is not supported by the foreign catalog");
        };
        let name = c_string(name)?;
        let mut out = SedonaCSchemaProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), cascade, &mut out, &mut error) };
        check_code(code, error, "deregister schema")?;
        import_schema(out, self.session.clone(), self.runtime.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TableExistArgs {
    name: String,
}

/// A Sedona schema exported through [`SedonaCSchemaProvider`].
pub struct ExportedSchema {
    inner: SedonaSchemaRef,
    session: Arc<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl ExportedSchema {
    pub fn new(
        inner: SedonaSchemaRef,
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
                DataFusionError::External(format!("Table lookup thread panicked: {error:?}").into())
            })?
    }
}

impl Debug for ExportedSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExportedSchema")
            .field("inner", &self.inner)
            .finish()
    }
}

impl From<ExportedSchema> for SedonaCSchemaProvider {
    fn from(value: ExportedSchema) -> Self {
        Self {
            get_property_schema: Some(c_schema_property_schema),
            get_property: Some(c_schema_property),
            table: Some(c_schema_table),
            create_table: Some(c_schema_create),
            deregister_table: Some(c_schema_deregister),
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
    unsafe { write_utf8_property_schema(out, err) }
}

unsafe extern "C" fn c_schema_property(
    self_: *const SedonaCSchemaProvider,
    property: *const c_char,
    args: *const c_char,
    out: *mut FFI_ArrowArray,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedSchema) };
    match unsafe { cstr_from_ptr_or_empty(property) }.as_ref() {
        "owner_name" => unsafe { write_json_property(&exported.inner.owner_name(), out, err) },
        "table_names" => unsafe { write_json_property(&exported.inner.table_names(), out, err) },
        "table_exist" => match unsafe { parse_json_c_args::<TableExistArgs>(args) } {
            Ok(args) => unsafe {
                write_json_property(&exported.inner.table_exist(&args.name), out, err)
            },
            Err(error) => ffi_error(err, error),
        },
        property => {
            unsafe {
                sedona_extension::set_ffi_error!(err, "Unknown schema property: {}", property)
            };
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
    let exported = unsafe { &*((*self_).private_data as *const ExportedSchema) };
    match exported.table(unsafe { cstr_from_ptr_or_empty(name) }.into_owned()) {
        Ok(value) => {
            let value = value
                .map(|inner| {
                    ExportedTableProvider::new(
                        inner,
                        exported.session.clone(),
                        exported.runtime.clone(),
                    )
                    .into()
                })
                .unwrap_or_default();
            unsafe { std::ptr::write(out, value) };
            ERRNO_OK
        }
        Err(error) => ffi_error(err, error),
    }
}

unsafe extern "C" fn c_schema_create(
    self_: *const SedonaCSchemaProvider,
    name: *const c_char,
    plan: *mut SedonaCExecutionPlan,
    out: *mut SedonaCExecutionPlan,
    err: *mut SedonaCError,
) -> c_int {
    if plan.is_null() {
        unsafe { sedona_extension::set_ffi_error!(err, "Input execution plan is null") };
        return libc::EINVAL;
    }
    let exported = unsafe { &*((*self_).private_data as *const ExportedSchema) };
    let input = unsafe { std::ptr::replace(plan, SedonaCExecutionPlan::default()) };
    let input = match ImportedSedonaCExec::try_new(input) {
        Ok(input) => Arc::new(input) as Arc<dyn ExecutionPlan>,
        Err(error) => return ffi_error(err, error),
    };
    match exported
        .inner
        .create(&unsafe { cstr_from_ptr_or_empty(name) }, input)
    {
        Ok(plan) => {
            let value = ExportedExecutionPlan::new(
                plan,
                exported.session.task_ctx(),
                exported.runtime.clone(),
            )
            .into();
            unsafe { std::ptr::write(out, value) };
            ERRNO_OK
        }
        Err(error) => ffi_error(err, error),
    }
}

unsafe extern "C" fn c_schema_deregister(
    self_: *const SedonaCSchemaProvider,
    name: *const c_char,
    out: *mut SedonaCTableProvider,
    err: *mut SedonaCError,
) -> c_int {
    let exported = unsafe { &*((*self_).private_data as *const ExportedSchema) };
    match exported
        .inner
        .deregister(&unsafe { cstr_from_ptr_or_empty(name) })
    {
        Ok(value) => {
            let value = value
                .map(|inner| {
                    ExportedTableProvider::new(
                        inner,
                        exported.session.clone(),
                        exported.runtime.clone(),
                    )
                    .into()
                })
                .unwrap_or_default();
            unsafe { std::ptr::write(out, value) };
            ERRNO_OK
        }
        Err(error) => ffi_error(err, error),
    }
}

unsafe extern "C" fn c_schema_release(self_: *mut SedonaCSchemaProvider) {
    let this = unsafe { &mut *self_ };
    if !this.private_data.is_null() {
        drop(unsafe { Box::from_raw(this.private_data as *mut ExportedSchema) });
        this.private_data = null_mut();
    }
    this.release = None;
}

/// A [`SedonaCSchemaProvider`] imported as a [`SedonaSchema`].
pub struct ImportedSchema {
    inner: SedonaCSchemaProvider,
    owner_name: Option<String>,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
}

impl ImportedSchema {
    pub fn try_new(
        inner: SedonaCSchemaProvider,
        session: Weak<dyn Session>,
        runtime: Arc<RuntimeHandle>,
    ) -> Result<Self> {
        if inner.release.is_none()
            || inner.get_property_schema.is_none()
            || inner.get_property.is_none()
            || inner.table.is_none()
        {
            return sedona_common::sedona_internal_err!(
                "SedonaCSchemaProvider is released or missing a required callback"
            );
        }
        let get = inner.get_property.expect("validated above");
        let schema = inner.get_property_schema.expect("validated above");
        let owner_name = call_get_json_property_impl(
            "owner_name",
            "SedonaCSchemaProvider",
            None::<&()>,
            |property, out, err| unsafe { schema(&inner, property, out, err) },
            |property, args, out, err| unsafe { get(&inner, property, args, out, err) },
        )?;
        Ok(Self {
            inner,
            owner_name,
            session,
            runtime,
        })
    }

    fn property<T: serde::de::DeserializeOwned>(&self, property: &str) -> Result<T> {
        let get = self.inner.get_property.expect("validated in try_new");
        let schema = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            property,
            "SedonaCSchemaProvider",
            None::<&()>,
            |property, out, err| unsafe { schema(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { get(&self.inner, property, args, out, err) },
        )
    }
}

impl Debug for ImportedSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImportedSchema").finish()
    }
}

#[async_trait]
impl SedonaSchema for ImportedSchema {
    fn owner_name(&self) -> Option<&str> {
        self.owner_name.as_deref()
    }

    fn table_names(&self) -> Vec<String> {
        self.property("table_names").unwrap_or_default()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let callback = self.inner.table.expect("validated in try_new");
        let name = c_string(name)?;
        let mut out = SedonaCTableProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        check_code(code, error, "get table")?;
        if out.release.is_none() {
            Ok(None)
        } else {
            Ok(Some(Arc::new(ImportedTableProvider::try_new(out)?)))
        }
    }

    fn create(&self, name: &str, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(callback) = self.inner.create_table else {
            return not_impl_err!("Creating tables is not supported by the foreign schema");
        };
        let name = c_string(name)?;
        let session = self.session.upgrade().ok_or_else(|| {
            DataFusionError::External(
                "Cannot export a plan after its host session has been dropped".into(),
            )
        })?;
        let mut input =
            ExportedExecutionPlan::new(input, session.task_ctx(), self.runtime.clone()).into();
        let mut out = SedonaCExecutionPlan::default();
        let mut error = SedonaCError::default();
        let code =
            unsafe { callback(&self.inner, name.as_ptr(), &mut input, &mut out, &mut error) };
        check_code(code, error, "create table")?;
        if out.release.is_none() {
            return sedona_common::sedona_internal_err!(
                "Create table callback returned no execution plan"
            );
        }
        Ok(Arc::new(ImportedSedonaCExec::try_new(out)?))
    }

    fn deregister(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(callback) = self.inner.deregister_table else {
            return not_impl_err!("Deregistering tables is not supported by the foreign schema");
        };
        let name = c_string(name)?;
        let mut out = SedonaCTableProvider::default();
        let mut error = SedonaCError::default();
        let code = unsafe { callback(&self.inner, name.as_ptr(), &mut out, &mut error) };
        check_code(code, error, "deregister table")?;
        if out.release.is_none() {
            Ok(None)
        } else {
            Ok(Some(Arc::new(ImportedTableProvider::try_new(out)?)))
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let get = self.inner.get_property.expect("validated in try_new");
        let schema = self
            .inner
            .get_property_schema
            .expect("validated in try_new");
        call_get_json_property_impl(
            "table_exist",
            "SedonaCSchemaProvider",
            Some(&TableExistArgs {
                name: name.to_string(),
            }),
            |property, out, err| unsafe { schema(&self.inner, property, out, err) },
            |property, args, out, err| unsafe { get(&self.inner, property, args, out, err) },
        )
        .unwrap_or(false)
    }
}

fn import_catalog(
    raw: SedonaCCatalogProvider,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
) -> Result<Option<SedonaCatalogRef>> {
    if raw.release.is_none() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(ImportedCatalog::try_new(
            raw, session, runtime,
        )?)))
    }
}

fn import_schema(
    raw: SedonaCSchemaProvider,
    session: Weak<dyn Session>,
    runtime: Arc<RuntimeHandle>,
) -> Result<Option<SedonaSchemaRef>> {
    if raw.release.is_none() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(ImportedSchema::try_new(
            raw, session, runtime,
        )?)))
    }
}

fn c_string(value: impl Into<Vec<u8>>) -> Result<CString> {
    CString::new(value).map_err(|error| {
        DataFusionError::External(format!("Catalog name contains an interior NUL: {error}").into())
    })
}

fn check_code(code: c_int, error: SedonaCError, operation: &str) -> Result<()> {
    if code == ERRNO_OK {
        Ok(())
    } else {
        sedona_common::sedona_internal_err!("Failed to {operation}: {error}")
    }
}

fn ffi_error(err: *mut SedonaCError, error: impl std::fmt::Display) -> c_int {
    unsafe { sedona_extension::set_ffi_error!(err, "{}", error) };
    libc::EINVAL
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::RwLock;

    use arrow_schema::Schema;
    use datafusion::catalog::{CatalogProviderList, Session};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;

    use super::*;

    #[derive(Debug, Default)]
    struct TestList {
        catalogs: RwLock<HashMap<String, SedonaCatalogRef>>,
    }

    impl SedonaCatalogList for TestList {
        fn catalog_names(&self) -> Vec<String> {
            self.catalogs.read().unwrap().keys().cloned().collect()
        }

        fn catalog(&self, name: &str) -> Option<SedonaCatalogRef> {
            self.catalogs.read().unwrap().get(name).cloned()
        }

        fn create(&self, name: &str) -> Result<SedonaCatalogRef> {
            let catalog: SedonaCatalogRef = Arc::new(TestCatalog::default());
            self.catalogs
                .write()
                .unwrap()
                .insert(name.to_string(), catalog.clone());
            Ok(catalog)
        }
    }

    #[derive(Debug, Default)]
    struct TestCatalog {
        schemas: RwLock<HashMap<String, SedonaSchemaRef>>,
    }

    impl SedonaCatalog for TestCatalog {
        fn schema_names(&self) -> Vec<String> {
            self.schemas.read().unwrap().keys().cloned().collect()
        }

        fn schema(&self, name: &str) -> Option<SedonaSchemaRef> {
            self.schemas.read().unwrap().get(name).cloned()
        }

        fn create(&self, name: &str) -> Result<SedonaSchemaRef> {
            let schema: SedonaSchemaRef = Arc::new(TestSchema::default());
            self.schemas
                .write()
                .unwrap()
                .insert(name.to_string(), schema.clone());
            Ok(schema)
        }

        fn deregister(&self, name: &str, _cascade: bool) -> Result<Option<SedonaSchemaRef>> {
            Ok(self.schemas.write().unwrap().remove(name))
        }
    }

    #[derive(Debug, Default)]
    struct TestSchema {
        tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    }

    #[async_trait]
    impl SedonaSchema for TestSchema {
        fn owner_name(&self) -> Option<&str> {
            Some("test-owner")
        }

        fn table_names(&self) -> Vec<String> {
            self.tables.read().unwrap().keys().cloned().collect()
        }

        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            Ok(self.tables.read().unwrap().get(name).cloned())
        }

        fn create(
            &self,
            _name: &str,
            input: Arc<dyn ExecutionPlan>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(input)
        }

        fn deregister(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            Ok(self.tables.write().unwrap().remove(name))
        }

        fn table_exist(&self, name: &str) -> bool {
            self.tables.read().unwrap().contains_key(name)
        }
    }

    fn runtime() -> Arc<RuntimeHandle> {
        Arc::new(RuntimeHandle::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        ))
    }

    #[tokio::test]
    async fn round_trip_preserves_create_semantics() {
        let producer_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let consumer_session: Arc<dyn Session> = Arc::new(SessionContext::new().state());
        let producer_runtime = runtime();
        let consumer_runtime = runtime();
        let source: SedonaCatalogListRef = Arc::new(TestList::default());
        let raw = ExportedCatalogList::new(source, producer_session, producer_runtime).into();
        let imported =
            ImportedCatalogList::try_new(raw, Arc::downgrade(&consumer_session), consumer_runtime)
                .unwrap();

        let catalog = imported.create("foreign").unwrap();
        assert_eq!(imported.catalog_names(), vec!["foreign"]);

        let schema = catalog.create("public").unwrap();
        assert_eq!(catalog.schema_names(), vec!["public"]);
        assert_eq!(schema.owner_name(), Some("test-owner"));

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
        let output = schema.create("new_table", input).unwrap();
        assert!(output.name().contains("PlaceholderRowExec"));

        assert!(catalog.deregister("public", false).unwrap().is_some());
    }

    #[tokio::test]
    async fn datafusion_adapter_reads_tables() {
        let schema = Arc::new(TestSchema::default());
        schema.tables.write().unwrap().insert(
            "items".to_string(),
            Arc::new(EmptyTable::new(Arc::new(Schema::empty()))),
        );
        let catalog = Arc::new(TestCatalog::default());
        catalog
            .schemas
            .write()
            .unwrap()
            .insert("public".to_string(), schema);
        let catalogs = Arc::new(TestList::default());
        catalogs
            .catalogs
            .write()
            .unwrap()
            .insert("foreign".to_string(), catalog);

        let adapter = crate::DataFusionCatalogList::new(catalogs);
        let table = adapter
            .catalog("foreign")
            .unwrap()
            .schema("public")
            .unwrap()
            .table("items")
            .await
            .unwrap();
        assert!(table.is_some());
    }
}
