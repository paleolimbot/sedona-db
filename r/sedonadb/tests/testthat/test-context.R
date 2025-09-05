
test_that("sd_read_parquet() works", {
  path <- system.file("files/natural-earth_cities_geo.parquet", package = "sedonadb")
  expect_identical(sd_count(sd_read_parquet(path)), 243)

  expect_identical(sd_count(sd_read_parquet(c(path, path))), 243 * 2)
})

test_that("views can be created and dropped", {
  df <- sd_sql("SELECT 1 as one")
  expect_true(rlang::is_reference(sd_to_view(df, "foofy"), df))
  expect_identical(
    sd_sql("SELECT * FROM foofy") |> sd_collect(),
    data.frame(one = 1)
  )

  expect_identical(
    sd_view("foofy") |> sd_collect(),
    data.frame(one = 1)
  )

  sd_drop_view("foofy")
  expect_error(sd_sql("SELECT * FROM foofy"), "table '(.*?)' not found")
  expect_error(sd_view("foofy"), "No table named 'foofy'")
})
