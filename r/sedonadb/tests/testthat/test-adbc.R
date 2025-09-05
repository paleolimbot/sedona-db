
test_that("adbc driver works", {
  con <- sedonadb_adbc() |>
    adbcdrivermanager::adbc_database_init() |>
    adbcdrivermanager::adbc_connection_init()

  df <-  con |>
    adbcdrivermanager::read_adbc("SELECT ST_Point(0, 1) as geometry") |>
    as.data.frame()

  expect_identical(
    wk::as_wkt(df$geometry),
    wk::wkt("POINT (0 1)")
  )
})
