SEXP savvy_init_r_runtime__ffi(DllInfo *c_arg___dll_info);
SEXP savvy_init_r_runtime_interrupts__ffi(SEXP c_arg__interrupts_call,
                                          SEXP c_arg__pkg_env);
SEXP savvy_int_times_int__ffi(SEXP c_arg__x, SEXP c_arg__y);
SEXP savvy_sedonadb_adbc_init_func__ffi(void);
SEXP savvy_to_upper__ffi(SEXP c_arg__x);

// methods and associated functions for InternalContext
SEXP savvy_InternalContext_data_frame_from_array_stream__ffi(
    SEXP self__, SEXP c_arg__stream_xptr, SEXP c_arg__collect_now);
SEXP savvy_InternalContext_deregister_table__ffi(SEXP self__,
                                                 SEXP c_arg__table_ref);
SEXP savvy_InternalContext_new__ffi(void);
SEXP savvy_InternalContext_read_parquet__ffi(SEXP self__, SEXP c_arg__paths);
SEXP savvy_InternalContext_sql__ffi(SEXP self__, SEXP c_arg__query);
SEXP savvy_InternalContext_view__ffi(SEXP self__, SEXP c_arg__table_ref);

// methods and associated functions for InternalDataFrame
SEXP savvy_InternalDataFrame_collect__ffi(SEXP self__, SEXP c_arg__out);
SEXP savvy_InternalDataFrame_compute__ffi(SEXP self__, SEXP c_arg__ctx);
SEXP savvy_InternalDataFrame_count__ffi(SEXP self__);
SEXP savvy_InternalDataFrame_limit__ffi(SEXP self__, SEXP c_arg__n);
SEXP savvy_InternalDataFrame_primary_geometry_column_index__ffi(SEXP self__);
SEXP savvy_InternalDataFrame_show__ffi(SEXP self__, SEXP c_arg__ctx,
                                       SEXP c_arg__width_chars,
                                       SEXP c_arg__ascii, SEXP c_arg__limit);
SEXP savvy_InternalDataFrame_to_arrow_schema__ffi(SEXP self__, SEXP c_arg__out);
SEXP savvy_InternalDataFrame_to_arrow_stream__ffi(SEXP self__, SEXP c_arg__out);
SEXP savvy_InternalDataFrame_to_view__ffi(SEXP self__, SEXP c_arg__ctx,
                                          SEXP c_arg__table_ref,
                                          SEXP c_arg__overwrite);

// methods and associated functions for Person
SEXP savvy_Person_associated_function__ffi(void);
SEXP savvy_Person_name__ffi(SEXP self__);
SEXP savvy_Person_new__ffi(void);
SEXP savvy_Person_set_name__ffi(SEXP self__, SEXP c_arg__name);
