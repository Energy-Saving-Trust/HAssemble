###### Wrapper functions ######

###### EPC Scotland Commerical cleaning wrapper ######
#' EPC cleaning 'master' wrapper for Scotland EPCs
#'
#' Very simple wrapper that runs through each EPC cleaning function for the commercial Scotland HA update.
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with EPC data to be cleaned
#' @param func_list a list of the functions to be run (optional)
#' @param mute an option to toggle print statements for each function. Defaults to NULL and will always print all tables and statements. Set to TRUE to mute these.
#' @return Dataframes with UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' epc_clean_commerical()
#' epc_clean_commerical(EPC_DOWNLOAD)
#' @export
epc_clean_commerical <- function(data, func_list = NULL, mute = NULL){

  #TODO when rolled into a package properly use below:
  namespace = "HAssemble"

  if (is.null(func_list)) {
    # Retrieve available function names in the package namespace
    func_list <- grep("^SComm_", ls(envir = asNamespace(namespace)), value = TRUE)
  }

  for (func_name in func_list) {
    if (exists(func_name, envir = asNamespace(namespace))) {
      # Dynamically call the function by its name and tell it where to look for said function (in this case in the correct package)
      func <- get(func_name, envir = asNamespace(namespace))
      func(data, mute = mute)
    } else {
      stop(paste("\u274C A function named", paste0('"', func_name, '"'), "is not a valid function for the epc cleaning wrapper in the", namespace, "package. "))
    }
  }

  cat("\u2728 EPC cleaning functions have finished processing. \u2728\n")
}
