
#' SedonaDB ADBC Driver
#'
#' @returns An [adbcdrivermanager::adbc_driver()] of class
#'   'sedonadb_driver_sedonadb'
#' @export
#'
#' @examples
#' library(adbcdrivermanager)
#'
#' con <- sedonadb_adbc() |>
#'   adbc_database_init() |>
#'   adbc_connection_init()
#' con |>
#'   read_adbc("SELECT ST_Point(0, 1) as geometry") |>
#'   as.data.frame()
#'
sedonadb_adbc <- function() {
    init_func <- structure(sedonadb_adbc_init_func(), class = "adbc_driver_init_func")
    adbcdrivermanager::adbc_driver(init_func, subclass = "sedonadb_driver_sedonadb")
}
