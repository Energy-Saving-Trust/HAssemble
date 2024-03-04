######### TOID and Building Block inference logic #########
#' TOID and Building Block inference logic
#'
#' Function to infer more "trusted" property characteristics based on proportions of characteristics in other properties within the TOID/Building Block
#' @import dplyr
#' @param data the dataframe with data to be processed.
#' @param var the column with the property characteristics of interest.
#' @param group the spatial unit - either TOID or Building Block.
#' @param trust the proportion of which to "trust" that a certain characteristics is the majority of known records. Default is 0.5 - i.e. needs to be above 50 percent of one label to be classed as the trusted label for that spatial unit.
#' @param join option to join the results back to the original input dataset. Set to FALSE to keep the output as a stand alone data table.
#' @return a dataframe with the UPRN and trusted label for property characteristics of interest or the original dataset with a column containing the trusted label.
#' @examples
#' TOID_BB_logic(dataset, FINAL_PROPERTY_AGE, BB_ID)
#' TOID_BB_logic(dataset, FINAL_PROPERTY_AGE, OS_TOPO_TOID, trust = 0.75, join = FALSE)
#' @export
TOID_BB_logic <- function(data, var, group, trust = 0.5, join = TRUE){

  dataset_name <- deparse(substitute(data))
  group_name <- deparse(substitute(group))
  var_name <- deparse(substitute(var))

  cat(paste0("Processing data for ", group_name, " and ", var_name, " \n"))
  trusted_props <- data %>%
    # Only calculate proportions of known data - drop the NAs and make sure we're not aggregating spatial aggregates with NA
    # So if the building block ID is "NA" then dont group all of those NAs across the country up together
    drop_na({{var}}, {{group}}) %>%
    group_by({{group}}) %>%
    # Count n homes then with data so proportion is not based on also including NAs
    mutate(count_properties_with_data = n()) %>%
    ungroup() %>%
    # Calculate proportions when grouping the spatial aggregation with the variable of choice
    # e.g. n (using n()) of all the pre-1919 in TOID XYZ against the total number of known data in TOID XYZ
    group_by({{group}}, {{var}}) %>%
    reframe(proportion = n() / count_properties_with_data) %>%
    # line above returns results for each row of that spatial aggregation
    # (so if 10 properties in BLOCK_ID itll return 10 rows even if they are all the same spatial aggregate/variable combo)
    distinct({{group}}, {{var}}, .keep_all = TRUE) %>%
    # Grab only the groups we'd trust the most as they make up over 50% of the groups for that TOID
    filter(proportion > {{trust}}) %>%
    select({{group}}, {{var}}) %>%
    rename(!!sym(paste0(group_name, "_", var_name)) := {{var}})

  if(join == TRUE){
    cat("Joining groups with trusted proportions to input dataset\n")
    final <- data %>%
      left_join(trusted_props, join_by({{group}}))
    assign(dataset_name, final, envir = .GlobalEnv)
  } else {
    cat(paste0("Loading trusted proportions for ", group_name, " and ", var_name, " into environment.\n"))
    assign(paste0("trusted_proportions_", var_name, "_", group_name), trusted_props, envir = .GlobalEnv)
  }
}
