
pub async fn write_parquet(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<TableParquetOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.insert_op != InsertOp::Append {
            return not_impl_err!(
                "{} is not implemented for DataFrame::write_parquet.",
                options.insert_op
            );
        }

        let format = if let Some(parquet_opts) = writer_options {
            Arc::new(ParquetFormatFactory::new_with_options(parquet_opts))
        } else {
            Arc::new(ParquetFormatFactory::new())
        };

        let file_type = format_as_file_type(format);

        let plan = if options.sort_by.is_empty() {
            self.plan
        } else {
            LogicalPlanBuilder::from(self.plan)
                .sort(options.sort_by)?
                .build()?
        };

        let plan = LogicalPlanBuilder::copy_to(
            plan,
            path.into(),
            file_type,
            Default::default(),
            options.partition_by,
        )?
        .build()?;
        DataFrame {
            session_state: self.session_state,
            plan,
            projection_requires_validation: self.projection_requires_validation,
        }
        .collect()
        .await
    }
