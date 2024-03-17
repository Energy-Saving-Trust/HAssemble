###### Neighbhourhood Measures (continuous) wrapper ######
#' Round up any numbers with .5 - R will default round to an even number with the round function.
#'
#' @param data The column with data to be rounded
#' @return Data rounded up if at .5 or above and down if below .5
#' @examples
#' custom_round((BUILDING_HEIGHT/3), 0)
#' @export
#'
custom_round <- function(data, digits = 0) {
  rounded_values <- ifelse(data - floor(data) == 0.5, ceiling(data), round(data, digits = digits))
  return(rounded_values)
}

