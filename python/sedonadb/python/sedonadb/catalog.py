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

"""Interfaces for Python-backed catalog implementations."""

from typing import Optional, Sequence

from sedonadb._lib import PyExecutionPlan, py_catalog_list


class CatalogList:
    """Top-level collection of catalogs exposed to a SedonaDB context."""

    def catalog_names(self) -> Sequence[str]:
        raise NotImplementedError()

    def catalog(self, name: str) -> Optional["Catalog"]:
        raise NotImplementedError()

    def create(self, name: str) -> "Catalog":
        """Create and return a catalog in the backing system."""
        raise NotImplementedError()

    def __sedonadb_catalog_list__(self):
        return py_catalog_list(self)


class Catalog:
    """Collection of schemas in a Python-backed catalog."""

    def schema_names(self) -> Sequence[str]:
        raise NotImplementedError()

    def schema(self, name: str) -> Optional["Schema"]:
        raise NotImplementedError()

    def create(self, name: str) -> "Schema":
        """Create and return a schema in the backing system."""
        raise NotImplementedError()

    def deregister(self, name: str, cascade: bool = False) -> Optional["Schema"]:
        raise NotImplementedError()


class Schema:
    """Collection of tables in a Python-backed catalog."""

    def owner_name(self) -> Optional[str]:
        return None

    def table_names(self) -> Sequence[str]:
        raise NotImplementedError()

    def table(self, name: str):
        """Return an Arrow-compatible object or SedonaDB DataFrame."""
        raise NotImplementedError()

    def create(self, name: str, input: PyExecutionPlan) -> PyExecutionPlan:
        """Create a table and return the physical plan that performs the work."""
        raise NotImplementedError()

    def deregister(self, name: str):
        raise NotImplementedError()

    def table_exist(self, name: str) -> bool:
        return name in self.table_names()
