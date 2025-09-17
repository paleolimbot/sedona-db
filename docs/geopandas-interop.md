# GeoPandas interoperability

This example shows how to read a GeoJSON file with GeoPandas and then convert the GeoPandas DataFrame to a SedonaDB DataFrame.

Any file type that can be read by GeoPandas can also be read into a SedonaDB DataFrame!


```python
import sedona.db
import geopandas as gpd

sd = sedona.db.connect()
```

### Read a GeoJSON file with GeoPandas


```python
gdf = gpd.read_file("some_data.json")
```


```python
gdf
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>prop0</th>
      <th>prop1</th>
      <th>geometry</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>value0</td>
      <td>None</td>
      <td>POINT (102 0.5)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>value1</td>
      <td>0.0</td>
      <td>LINESTRING (102 0, 103 1, 104 0, 105 1)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>value2</td>
      <td>{ "this": "that" }</td>
      <td>POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))</td>
    </tr>
  </tbody>
</table>
</div>




```python
gdf.info()
```

    <class 'geopandas.geodataframe.GeoDataFrame'>
    RangeIndex: 3 entries, 0 to 2
    Data columns (total 3 columns):
     #   Column    Non-Null Count  Dtype   
    ---  ------    --------------  -----   
     0   prop0     3 non-null      object  
     1   prop1     2 non-null      object  
     2   geometry  3 non-null      geometry
    dtypes: geometry(1), object(2)
    memory usage: 204.0+ bytes


### Convert the GeoPandas DataFrame to a SedonaDB DataFrame


```python
df = sd.create_data_frame(gdf)
```


```python
df.show()
```

    ┌────────┬────────────────────┬──────────────────────────────────────────┐
    │  prop0 ┆        prop1       ┆                 geometry                 │
    │  utf8  ┆        utf8        ┆                 geometry                 │
    ╞════════╪════════════════════╪══════════════════════════════════════════╡
    │ value0 ┆                    ┆ POINT(102 0.5)                           │
    ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ value1 ┆ 0.0                ┆ LINESTRING(102 0,103 1,104 0,105 1)      │
    ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ value2 ┆ { "this": "that" } ┆ POLYGON((100 0,101 0,101 1,100 1,100 0)) │
    └────────┴────────────────────┴──────────────────────────────────────────┘


## Read a FlatGeobuf file

This code demonstrates how to read a FlatGeobuf file with GeoPandas and then convert it to a SedonaDB DataFrame.


```python
path = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities.fgb"
gdf = gpd.read_file(path)
```


```python
df = sd.create_data_frame(gdf)
```


```python
df.show(3)
```

    ┌──────────────┬──────────────────────────────┐
    │     name     ┆           geometry           │
    │     utf8     ┆           geometry           │
    ╞══════════════╪══════════════════════════════╡
    │ Vatican City ┆ POINT(12.4533865 41.9032822) │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ San Marino   ┆ POINT(12.4417702 43.9360958) │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Vaduz        ┆ POINT(9.5166695 47.1337238)  │
    └──────────────┴──────────────────────────────┘

