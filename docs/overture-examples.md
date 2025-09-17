<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# SedonaDB Overture Examples

This notebook shows how to query the Overture data with SedonaDB!


```python
import sedona.db
import os

os.environ["AWS_SKIP_SIGNATURE"] = "true"
os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

sd = sedona.db.connect()
```

## Overture buildings table


```python
df = sd.read_parquet(
    "s3://overturemaps-us-west-2/release/2025-08-20.0/theme=buildings/type=building/"
)
```


```python
df.limit(10).show()
```

    ┌──────────────────────────────────────┬─────────────────────────────────────────┬───┬─────────────┐
    │                  id                  ┆                 geometry                ┆ … ┆ roof_height │
    │               utf8view               ┆           wkb_view <ogc:crs84>          ┆   ┆   float64   │
    ╞══════════════════════════════════════╪═════════════════════════════════════════╪═══╪═════════════╡
    │ 06533301-f2ec-42e0-8138-732ac25a7497 ┆ POLYGON((-58.4757066 -34.7389169,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ cc0c048c-088d-4cb3-9982-3961edfdf416 ┆ POLYGON((-58.4755777 -34.7389131,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ e52a0dbc-fb93-40e2-b1df-03626855299c ┆ POLYGON((-58.4754112 -34.7394253,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 28526977-9920-4cec-9840-5dd409a7cded ┆ POLYGON((-58.4752088 -34.7394754,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 0bc4c042-52ea-4ae7-9200-56221805fa2f ┆ POLYGON((-58.475273 -34.7394421,-58.47… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ c21dfee1-f5d9-4e0a-91cf-796f117518d4 ┆ POLYGON((-58.4750977 -34.7394357,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 3fe5efdd-1739-4088-8c8e-6f7f1b7cfcfe ┆ POLYGON((-58.4751684 -34.7394288,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ c144becc-fc8a-4bbc-aeef-359ac56a925a ┆ POLYGON((-58.4751787 -34.739396,-58.47… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 79d6c10a-2ff2-429d-a0e7-6eefc8939d14 ┆ POLYGON((-58.4753719 -34.7393189,-58.4… ┆ … ┆             │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ c1664c2d-2c0f-44c4-af08-58176e360613 ┆ POLYGON((-58.4753269 -34.7391919,-58.4… ┆ … ┆             │
    └──────────────────────────────────────┴─────────────────────────────────────────┴───┴─────────────┘



```python
df.to_view("buildings")
```


```python
# the buildings table is large and contains millions of rows
sd.sql("""
SELECT
    COUNT(*)
FROM
    buildings
""").show()
```

    ┌────────────┐
    │  count(*)  │
    │    int64   │
    ╞════════════╡
    │ 2539170484 │
    └────────────┘



```python
# check out the schema of the buildings table to see what it contains
df.schema
```




    SedonaSchema with 24 fields:
      id: Utf8View
      geometry: wkb_view <ogc:crs84>
      bbox: Struct(xmin Float32, xmax Float32, ymin Float32, ymax Float32)
      version: Int32
      sources: List(Field { name: "element", data_type: Struct([Field { name: "property", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "dataset", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "record_id", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "update_time", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "confidence", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "between", data_type: List(Field { name: "element", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })
      level: Int32
      subtype: Utf8View
      class: Utf8View
      height: Float64
      names: Struct(primary Utf8, common Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), rules List(Field { name: "element", data_type: Struct([Field { name: "variant", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "language", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "perspectives", data_type: Struct([Field { name: "mode", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "countries", data_type: List(Field { name: "element", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "between", data_type: List(Field { name: "element", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "side", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }))
      has_parts: Boolean
      is_underground: Boolean
      num_floors: Int32
      num_floors_underground: Int32
      min_height: Float64
      min_floor: Int32
      facade_color: Utf8View
      facade_material: Utf8View
      roof_material: Utf8View
      roof_shape: Utf8View
      roof_direction: Float64
      roof_orientation: Utf8View
      roof_color: Utf8View
      roof_height: Float64




```python
# find all the buildings in New York city that are taller than 20 meters
nyc_bbox_wkt = "POLYGON((-74.2591 40.4774, -74.2591 40.9176, -73.7004 40.9176, -73.7004 40.4774, -74.2591 40.4774))"
sd.sql(f"""
SELECT
    id,
    height,
    num_floors,
    roof_shape,
    ST_Centroid(geometry) as centroid
FROM
    buildings
WHERE
    is_underground = FALSE
    AND height IS NOT NULL
    AND height > 20
    AND ST_Intersects(geometry, ST_SetSRID(ST_GeomFromText('{nyc_bbox_wkt}'), 4326))
LIMIT 5;
""").show()
```

    ┌─────────────────────────┬────────────────────┬────────────┬────────────┬─────────────────────────┐
    │            id           ┆       height       ┆ num_floors ┆ roof_shape ┆         centroid        │
    │         utf8view        ┆       float64      ┆    int32   ┆  utf8view  ┆     wkb <ogc:crs84>     │
    ╞═════════════════════════╪════════════════════╪════════════╪════════════╪═════════════════════════╡
    │ 1b9040c2-2e79-4f56-aba… ┆               22.4 ┆            ┆            ┆ POINT(-74.230407502993… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 1b5e1cd2-d697-489e-892… ┆               21.5 ┆            ┆            ┆ POINT(-74.231451103592… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ c1afdf78-bf84-4b8f-ae1… ┆               20.9 ┆            ┆            ┆ POINT(-74.232593032240… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 88f36399-b09f-491b-bb6… ┆               24.5 ┆            ┆            ┆ POINT(-74.231878209597… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ df37a283-f5bd-4822-a05… ┆ 24.154542922973633 ┆            ┆            ┆ POINT(-74.241910239840… │
    └─────────────────────────┴────────────────────┴────────────┴────────────┴─────────────────────────┘


## Overture divisions table


```python
df = sd.read_parquet(
    "s3://overturemaps-us-west-2/release/2025-08-20.0/theme=divisions/type=division_area/"
)
```


```python
# take a look at a few rows of data
df.show(10)
```

    ┌────────────────┬────────────────┬────────────────┬───┬────────────────┬──────────┬───────────────┐
    │       id       ┆    geometry    ┆      bbox      ┆ … ┆ is_territorial ┆  region  ┆  division_id  │
    │    utf8view    ┆ wkb_view <ogc… ┆ struct(xmin f… ┆   ┆     boolean    ┆ utf8view ┆    utf8view   │
    ╞════════════════╪════════════════╪════════════════╪═══╪════════════════╪══════════╪═══════════════╡
    │ 61912ffd-060b… ┆ POLYGON((23.3… ┆ {xmin: 22.735… ┆ … ┆ true           ┆ ZA-EC    ┆ 2711d6ca-ac1… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 7647b992-e0d6… ┆ POLYGON((26.5… ┆ {xmin: 26.521… ┆ … ┆ true           ┆ ZA-EC    ┆ 0e8a08eb-6f2… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 4058785b-82c9… ┆ MULTIPOLYGON(… ┆ {xmin: 22.735… ┆ … ┆ false          ┆ ZA-EC    ┆ 2711d6ca-ac1… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ cd9389b7-3451… ┆ POLYGON((26.5… ┆ {xmin: 26.373… ┆ … ┆ true           ┆ ZA-EC    ┆ 9d59ea5e-408… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ a60ae908-e6fa… ┆ POLYGON((26.6… ┆ {xmin: 26.541… ┆ … ┆ true           ┆ ZA-EC    ┆ f49ef082-3c2… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ fd070cbb-4aaa… ┆ POLYGON((26.1… ┆ {xmin: 26.084… ┆ … ┆ true           ┆ ZA-EC    ┆ 513b5a9c-c29… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 70479601-dc12… ┆ POLYGON((26.4… ┆ {xmin: 26.222… ┆ … ┆ true           ┆ ZA-EC    ┆ 2ade34e5-955… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 3a294b23-f674… ┆ POLYGON((24.5… ┆ {xmin: 24.503… ┆ … ┆ true           ┆ ZA-EC    ┆ 4f63d19f-2ca… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 48b3e344-10a4… ┆ POLYGON((26.6… ┆ {xmin: 26.557… ┆ … ┆ true           ┆ ZA-EC    ┆ 20d890bb-1a4… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 92e71cf6-fa94… ┆ POLYGON((25.8… ┆ {xmin: 25.799… ┆ … ┆ true           ┆ ZA-EC    ┆ 4202ec06-188… │
    └────────────────┴────────────────┴────────────────┴───┴────────────────┴──────────┴───────────────┘



```python
df.to_view("division_area")
```


```python
sd.sql("""
SELECT
    COUNT(*)
FROM division_area
""").show()
```

    ┌──────────┐
    │ count(*) │
    │   int64  │
    ╞══════════╡
    │  1035749 │
    └──────────┘



```python
df.schema
```




    SedonaSchema with 13 fields:
      id: Utf8View
      geometry: wkb_view <ogc:crs84>
      bbox: Struct(xmin Float32, xmax Float32, ymin Float32, ymax Float32)
      country: Utf8View
      version: Int32
      sources: List(Field { name: "element", data_type: Struct([Field { name: "property", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "dataset", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "record_id", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "update_time", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "confidence", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "between", data_type: List(Field { name: "element", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} })
      subtype: Utf8View
      class: Utf8View
      names: Struct(primary Utf8, common Map(Field { name: "key_value", data_type: Struct([Field { name: "key", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, false), rules List(Field { name: "element", data_type: Struct([Field { name: "variant", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "language", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "perspectives", data_type: Struct([Field { name: "mode", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "countries", data_type: List(Field { name: "element", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "value", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "between", data_type: List(Field { name: "element", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "side", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }))
      is_land: Boolean
      is_territorial: Boolean
      region: Utf8View
      division_id: Utf8View




```python
# get all the divisions in Nova Scotia and save them in memory with to_memtable()
nova_scotia_bbox_wkt = (
    "POLYGON((-66.5 43.4, -66.5 47.1, -59.8 47.1, -59.8 43.4, -66.5 43.4))"
)
ns = sd.sql(f"""
SELECT
    country, region, names, geometry
FROM division_area
WHERE
    ST_Intersects(geometry, ST_SetSRID(ST_GeomFromText('{nova_scotia_bbox_wkt}'), 4326))
""").to_memtable()
```


```python
ns.to_view("ns_divisions")
```


```python
df = sd.sql("""
SELECT UNNEST(names), geometry
FROM ns_divisions
WHERE region = 'CA-NS'
""")
```


```python
%%time
# this executes quickly because the Nova Scotia data was persisted in memory with to_memtable()
df.show(2)
```

    ┌────────────────────────┬────────────────────────┬────────────────────────┬───────────────────────┐
    │ __unnest_placeholder(n ┆ __unnest_placeholder(n ┆ __unnest_placeholder(n ┆        geometry       │
    │ s_divisions.names).pr… ┆ s_divisions.names).co… ┆ s_divisions.names).ru… ┆  wkb_view <ogc:crs84> │
    ╞════════════════════════╪════════════════════════╪════════════════════════╪═══════════════════════╡
    │ Seal Island            ┆                        ┆                        ┆ POLYGON((-66.0528452… │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Mud Island             ┆                        ┆                        ┆ POLYGON((-66.0222822… │
    └────────────────────────┴────────────────────────┴────────────────────────┴───────────────────────┘
    CPU times: user 8.75 ms, sys: 2.41 ms, total: 11.2 ms
    Wall time: 8.47 ms
