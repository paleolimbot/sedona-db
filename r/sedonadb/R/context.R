
#' Create a DataFrame from one or more Parquet files
#'
#' The query will only be executed when requested.
#'
#' @param path One or more paths or URIs to Parquet files
#'
#' @returns A sedonadb_dataframe
#' @export
#'
#' @examples
#' path <- system.file("files/natural-earth_cities_geo.parquet", package = "sedonadb")
#' sd_read_parquet(path) |> head(5) |> sd_preview()
#'
sd_read_parquet <- function(path) {
  ctx <- ctx()
  df <- ctx$read_parquet(path)
  new_sedonadb_dataframe(ctx, df)
}

#' Create a DataFrame from SQL
#'
#' The query will only be executed when requested.
#'
#' @param sql A SQL string to execute
#'
#' @returns A sedonadb_dataframe
#' @export
#'
#' @examples
#' sd_sql("SELECT ST_Point(0, 1) as geom") |> sd_preview()
#'
sd_sql <- function(sql) {
  ctx <- ctx()
  df <- ctx$sql(sql)
  new_sedonadb_dataframe(ctx, df)
}

#' Create or Drop a named view
#'
#' Remove a view created with [sd_to_view()] from the context.
#'
#' @param table_ref The name of the view reference
#' @returns The context, invisibly
#' @export
#'
#' @examples
#' sd_sql("SELECT 1 as one") |> sd_to_view("foofy")
#' sd_view("foofy")
#' sd_drop_view("foofy")
#' try(sd_view("foofy"))
#'
sd_drop_view <- function(table_ref) {
  ctx <- ctx()
  ctx$deregister_table(table_ref)
  invisible(ctx)
}

#' @rdname sd_drop_view
#' @export
sd_view <- function(table_ref) {
  ctx <- ctx()
  df <- ctx$view(table_ref)
  new_sedonadb_dataframe(ctx, df)
}

# We use just one context for now. In theory we could support multiple
# contexts with a shared runtime, which would scope the registration
# of various components more cleanly from the runtime.
ctx <- function() {
  if (is.null(global_ctx$ctx)) {
    global_ctx$ctx <- InternalContext$new()
  }

  global_ctx$ctx
}

global_ctx <- new.env(parent = emptyenv())
global_ctx$ctx <- NULL
