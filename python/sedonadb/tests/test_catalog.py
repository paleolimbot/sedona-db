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

import pyarrow as pa
import pyarrow.parquet as pq

import sedonadb
from sedonadb.catalog import Catalog, CatalogList, Schema


class DictSchema(Schema):
    def __init__(self, tables):
        self.tables = tables
        self.create_calls = []

    def owner_name(self):
        return "python"

    def table_names(self):
        return list(self.tables)

    def table(self, name):
        return self.tables.get(name)

    def deregister(self, name):
        return self.tables.pop(name, None)

    def create(self, name, input):
        self.create_calls.append((name, input.name))
        return input


class DictCatalog(Catalog):
    def __init__(self, schemas):
        self.schemas = schemas

    def schema_names(self):
        return list(self.schemas)

    def schema(self, name):
        return self.schemas.get(name)

    def create(self, name):
        schema = DictSchema({})
        self.schemas[name] = schema
        return schema

    def deregister(self, name, cascade=False):
        return self.schemas.pop(name, None)


class DictCatalogList(CatalogList):
    def __init__(self, catalogs):
        self.catalogs = catalogs

    def catalog_names(self):
        return list(self.catalogs)

    def catalog(self, name):
        return self.catalogs.get(name)

    def create(self, name):
        catalog = DictCatalog({})
        self.catalogs[name] = catalog
        return catalog


def test_python_catalog_query_and_builtin_fallback():
    items = pa.table({"id": [1, 2], "name": ["one", "two"]})
    catalogs = DictCatalogList(
        {"foreign": DictCatalog({"public": DictSchema({"items": items})})}
    )

    sd = sedonadb.connect()
    sd.register(catalogs)

    result = sd.sql("SELECT name FROM foreign.public.items ORDER BY id").to_arrow_table()
    assert result.column("name").to_pylist() == ["one", "two"]

    # Registering a foreign list overlays, rather than replaces, the built-ins.
    assert sd.sql("SELECT 42 AS answer").to_arrow_table()["answer"].to_pylist() == [42]


def test_python_catalog_create_hierarchy():
    catalogs = DictCatalogList({})
    catalog = catalogs.create("foreign")
    schema = catalog.create("public")

    assert catalogs.catalog("foreign") is catalog
    assert catalog.schema("public") is schema


def test_sql_create_uses_python_create_methods(tmp_path):
    catalogs = DictCatalogList({})
    catalogs.create("foreign")
    sd = sedonadb.connect()
    sd.register(catalogs)

    sd.sql("CREATE SCHEMA foreign.public").execute()

    source = tmp_path / "source.parquet"
    pq.write_table(pa.table({"value": [1, 2]}), source)
    result = sd.sql(
        "CREATE EXTERNAL TABLE foreign.public.created "
        f"STORED AS PARQUET LOCATION '{source}'"
    ).to_arrow_table()

    schema = catalogs.catalog("foreign").schema("public")
    assert schema.create_calls == [("created", "DataSourceExec")]
    assert result["value"].to_pylist() == [1, 2]
