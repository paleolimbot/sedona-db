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

use std::collections::HashMap;

use arrow_schema::Schema;
use datafusion_common::{DFSchema, Result};

use crate::{datatypes::SedonaType, matchers::ArgMatcher};

pub trait SedonaSchema {
    fn geometry_column_indices(&self) -> Result<Vec<usize>>;
    fn primary_geometry_column_index(&self) -> Result<Option<usize>>;
}

impl SedonaSchema for DFSchema {
    fn geometry_column_indices(&self) -> Result<Vec<usize>> {
        self.as_arrow().geometry_column_indices()
    }

    fn primary_geometry_column_index(&self) -> Result<Option<usize>> {
        self.as_arrow().primary_geometry_column_index()
    }
}

impl SedonaSchema for Schema {
    fn geometry_column_indices(&self) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        let matcher = ArgMatcher::is_geometry_or_geography();
        for (i, field) in self.fields().iter().enumerate() {
            if matcher.match_type(&SedonaType::from_storage_field(field)?) {
                indices.push(i);
            }
        }

        Ok(indices)
    }

    fn primary_geometry_column_index(&self) -> Result<Option<usize>> {
        let indices = self.geometry_column_indices()?;
        if indices.is_empty() {
            return Ok(None);
        }

        let names_map = indices
            .iter()
            .rev()
            .map(|i| (self.field(*i).name().to_lowercase(), *i))
            .collect::<HashMap<_, _>>();

        for special_name in ["geometry", "geography", "geom", "geog"] {
            if let Some(i) = names_map.get(special_name) {
                return Ok(Some(*i));
            }
        }

        Ok(Some(indices[0]))
    }
}
