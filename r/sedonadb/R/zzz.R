
.onLoad <- function(...) {
  # Load geoarrow to manage conversion of arrow results to/from spatial objects
  requireNamespace("geoarrow", quietly = TRUE)

  # Call at least one function for R CMD check
  geoarrow::as_geoarrow_array("POINT (0 1")

  # Inject what we need to reduce the Rust code to a simple Rf_eval()
  ns <- asNamespace("sedonadb")
  call <- call("check_interrupts")
  init_r_runtime_interrupts(call, ns)
}

# The function we call from Rust to check for interrupts. R checks for
# interrupts automatically when evaluating regular R code and signals
# an interrupt condition,
check_interrupts <- function() {
  tryCatch({
    FALSE
  }, interrupt = function(...) TRUE, error = function(...) TRUE)
}
