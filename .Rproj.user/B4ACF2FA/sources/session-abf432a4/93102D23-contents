###### Neighbhourhood Measures wrapper ######
#' Wrapper for building out neighbhourhood measures for Home Analytics
#'
#' Very simple wrapper that runs through each EPC cleaning function for the commercial Scotland HA update.
#' @import dplyr
#' @import tidyr
#' @import rlang
#' @param df The dataframe with EPC data to be cleaned
#' @param var the variable column to be used
#' @param geogs an option to provide a list of geographies to compute neighbhourhood measure's proportion/uncertainty for
#' @param ptype the property type subgroup to compute neighbhourhood measures against. Expects column called PTYPE_SUBGROUP, but can provide your own.
#' @return Dataframes with UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' neigh_meas(EPCs, FINAL_GLAZ_TYPE, list, PROPERTY_TYPE_GROUPS)
#' @export

#Write small function wrapper for the loop so we can plant anywhere we like
neigh_meas <- function(df, var, geogs = NULL, ptype = PTYPE_SUBGROUP) {

  if(is.null(geogs)){
    geogs = c('OS_TOPO_TOID', 'BLOCK_ID', 'COA', 'LSOA', "POSTCODE", "POSTCODE_SECTOR")
  }

  # Doesnt play well with factors in the geography columns so we're going to flag up a need to change the data types
  column_classes <- sapply(df[geogs], class)

  if (any(column_classes == "factor")) {
    stop(paste0("Function stopped: One or more of the neighbhourhood measure columns are factors. Convert ", paste(geogs, collapse = ", "), " to character for stability."))
  }

  # Grab the data table input name
  input_df_name <- deparse(substitute(df))

  for (i in 1:length(geogs)){

    # TODO this is actually a little convoluted - do we really need gather and prop_type seperatley?
    #Assing variable name for a table based on i
    print(paste0("Doing geographic area number ", i, " for: ", rlang::ensym(var)))

    name = paste0(rlang::ensym(var), "_", geogs[i])

    print(paste0("First gather of data for ", geogs[i], " and ", rlang::ensym(var)))

    gather <- df %>%
      drop_na({{var}}, !!sym(geogs[i])) %>%
      group_by(!!sym(geogs[i]), {{var}}, {{ptype}}) %>%
      summarise(count_var = n()) %>%
      mutate(ID = paste0(!!sym(geogs[i]), "_", {{ptype}}))

    print(paste0("Property type counts for ", geogs[i], " and ", rlang::ensym(var)))
    prop_type <- df %>%
      drop_na({{var}}, !!sym(geogs[i])) %>%
      group_by(!!sym(geogs[i]), {{ptype}}) %>%
      summarise(count_ptype = n()) %>%
      mutate(ID = paste0(!!sym(geogs[i]), "_", {{ptype}}))

    print("First merge of data - look at optimising this in later versions.")
    merge <- left_join(gather, prop_type[, c("ID", "count_ptype")],
                       by = "ID") %>%
      mutate(prop = count_var/count_ptype) %>%
      mutate(unc = (prop*(1-prop))/count_ptype)

    print("Pivoting uncertainty data from long to wide")
    merge_unc_wide <- merge %>%
      ungroup() %>%
      mutate(Group = paste0(geogs[i], ".", {{var}}, ".Un")) %>%
      select(Group, ID, unc) %>%
      tidyr::pivot_wider(names_from = Group, values_from = unc, values_fill = 0)

    print("Pivoting proportion data from long to wide and merge")
    merge_wide <- merge %>%
      ungroup() %>%
      mutate(Group = paste0(geogs[i], ".", {{var}}, ".Pr")) %>%
      select(Group, ID, prop) %>%
      tidyr::pivot_wider(names_from = Group, values_from = prop, values_fill = 0) %>%
      left_join(merge_unc_wide,
                by = "ID")

    list <- colnames(merge_wide); list <- list[-1]

    print("Merge proportions and uncertainty data to original dataset")
    if(i==1){
      temp <- df %>%
        mutate(ID = paste0(!!sym(geogs[i]), "_", {{ptype}})) %>%
        left_join(merge_wide,
                  by = "ID") %>%
        # If the column names exists in the previous ones (i.e. the pr and unc columns) replace NAs
        # What is this actually for Dai?
        mutate_at(
          c(list), ~replace_na(.,0)
        ) %>%
        select(-ID)

      print(paste0("Finished run for:", geogs[i]))

    } else if (i > 1 & i != length(geogs)) {
      #print("here")
      temp <- temp %>%
        mutate(ID = paste0(!!sym(geogs[i]), "_", {{ptype}})) %>%
        left_join(merge_wide,
                  by = "ID") %>%
        mutate_at(
          c(list), ~replace_na(.,0)
        ) %>%
        select(-ID)

      print(paste0("Finished run for:", geogs[i]))

    } else if(i == length(geogs)) {

      final_df_name <- paste0(input_df_name, "_neigh_meas")

      assign(final_df_name,
             temp %>%
               mutate(ID = paste0(!!sym(geogs[i]), "_", {{ptype}})) %>%
               left_join(merge_wide,
                         by = "ID") %>%
               mutate_at(c(list), ~replace_na(.,0)) %>%
               select(-ID),
             envir = .GlobalEnv)
    }

    #Then drop the "Group" name from the main dataset or it'll just keep making groups based on the inputs called "Group"

    print(paste(geogs[i], "neighbourhood measures complete"))

  }

  print(paste0("Run complete - data is available in the Global Environment as: ", final_df_name))
}
