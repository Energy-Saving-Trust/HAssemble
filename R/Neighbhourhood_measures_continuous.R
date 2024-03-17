###### Neighbhourhood Measures (continuous) wrapper ######
#' Wrapper for building out neighbhourhood measures for Home Analytics
#'
#' Very simple wrapper that runs through each EPC cleaning function for the commercial Scotland HA update. Works for building he continuous variables
#' @import dplyr
#' @import tidyr
#' @import rlang
#' @param df The dataframe with EPC data to be cleaned
#' @param var the variable column to be used with continuous, numeric data
#' @param geogs an option to provide a list of geographies to compute neighbhourhood measure's proportion/uncertainty for
#' @param ptype the property type subgroup to compute neighbhourhood measures against. Expects column called PTYPE_SUBGROUP, but can provide your own.
#' @return Dataframes with UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' neigh_meas(EPCs, TOTAL_FLOOR_ARE, list, PROPERTY_TYPE_GROUPS)
#' @export

#Write small function wrapper for the loop so we can plant anywhere we like
neigh_meas_cont <- function(df, var, geogs = NULL, ptype = PTYPE_SUBGROUP) {

  if(is.null(geogs)){
    geogs = c('BLOCK_ID', 'COA_CODE', 'LSOA_CODE', "POSTCODE", "POSTCODE_SECTOR")
  }

  # Doesnt play well with factors in the geography columns so we're going to flag up a need to change the data types
  column_classes <- sapply(df[geogs], class)

  if (any(column_classes == "factor")) {
    stop(paste0("Function stopped: One or more of the neighbhourhood measure columns are factors. Convert ", paste(geogs, collapse = ", "), " to character for stability."))
  }

  # Grab the data table input name and make the final dataset name for output
  final_df_name <- paste0(deparse(substitute(df)), "_neigh_meas_cont")

  for (i in 1:length(geogs)){
    print(paste0("Started processing ", ensym(var), " for ", geogs[i]))

    if(i == 1){
      nhm <- df %>%
        group_by(!!sym(geogs[i]), {{ptype}}) %>%
        mutate(
          !!paste0("median_", geogs[i]) := median(log({{var}}), na.rm = T),
          !!paste0("sd_", geogs[i]) := sd(log({{var}}), na.rm = T)
        ) %>%
        ungroup()

      print(paste0("Finished processing ", ensym(var), " for ", geogs[i]))

    } else {

      nhm <- nhm %>%
        group_by(!!sym(geogs[i]), {{ptype}}) %>%
        mutate(
          !!paste0("median_", geogs[i]) := median(log({{var}}), na.rm = T),
          !!paste0("sd_", geogs[i]) := sd(log({{var}}), na.rm = T)
        ) %>%
        ungroup()

      print(paste0("Finished processing ", ensym(var), " for ", geogs[i]))
    }

    if(i == length(geogs)){
      nhm <- nhm %>%
        mutate(across(matches("^median_|^sd_"), ~replace(., is.na(.), 0)))

      assign(final_df_name,
             nhm,
             envir = .GlobalEnv)

      print(paste0("Run complete - data is available in the Global Environment as: ", final_df_name))
    }
  }
}
