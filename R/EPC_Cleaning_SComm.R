####################################################################
#                                                                  #
# Energy Saving Trust                                              #
#                                                                  #
# Client: EST Commercial Project                                   #
# Projet Title: HA Scotland Comm. v1                               #
# Project Lead: Dai Grady                                          #
# Model: EPC mapping to HA                                         #
#                                                                  #
# Version Date: 29/01/2024                                         #
#                                                                  #
####################################################################

#Script Overview: functions for mapping EPC data variables to HA

###################################################################################################################

######### Property Type #########
#' Property type cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @param data The dataframe with data to be cleaned
#' @param type The property type column in the dataframe
#' @param form The property form column in the dataframe
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_type(data)
#' scomm_type(data, type = PTYPE_NEW, form = B_FORM, mute = TRUE)
#' @export
scomm_type <- function(data, type = PROPERTY_TYPE, form = BUILT_FORM, mute = NULL, ...) {
  cat("\U231B Cleaning property type for Scotland EPCs.\n")

  # Bring property type and built form together
  cleaning <- data %>%
    mutate(EPC_PROPERTY_TYPE_COMB = paste({{type}}, {{form}}, sep = '.'))

  # Main cleaning block
  cleaning <- cleaning %>%
    dplyr::mutate(EPC_PROPERTY_TYPE = case_when(
      ({{form}} == "Detached"
       & {{type}} %in% c("House", "Bungalow")) ~ "Detached house",
      ({{form}} == "Semi-Detached"
       & {{type}} %in% c("House", "Bungalow")) ~ "Semi-detached house",
      ({{form}} %in% c("Mid-Terrace", "Enclosed Mid-Terrace")
       & {{type}} %in% c("House", "Bungalow")) ~ "Mid-terraced house",
      # How do we treat enclosed terrace homes? Quite rare... Not an enormous difference in SAP but there is a difference
      ({{form}} %in% c("End-Terrace", "Enclosed End-Terrace")
       & {{type}} %in% c("House", "Bungalow")) ~ "End-terraced house",
      {{type}} %in% c("Flat", "Maisonette") ~ "Block of flats",
      {{type}} == "Park home" ~ "Park home",
      # Then all others categorised as unknown
      TRUE ~ "Unknown"
      ),
      EPC_BUILT_FORM = case_when(
        {{type}} == "House" ~ "House",
        {{type}} == "Bungalow" ~ "Bungalow",
        {{type}} %in% c("Flat", "Maisonette") ~ "Flat",
        {{type}} == "Park home" ~ "Park home",
        TRUE ~ "Unknown"
      )
    ) %>%
    dplyr::select(UPRN, EPC_PROPERTY_TYPE, EPC_BUILT_FORM, {{type}}, EPC_PROPERTY_TYPE_COMB)

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed property type cleaning. Adding "prop_type" object to the environment.\n')
  assign("prop_type", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned data vs. original EPC data from data input of ", ensym(type), " and ", ensym(form), ":\n", sep = "")
    print(table(cleaning$EPC_PROPERTY_TYPE, cleaning$EPC_PROPERTY_TYPE_COMB, useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned data vs. original EPC data from data input of ", ensym(type), ":\n", sep = "")
    print(table(cleaning$EPC_BUILT_FORM, cleaning[[ensym(type)]], useNA = "ifany"))
  }
}

######### Property Age #########
#' Property age cleaning for England & Wales data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @param data The dataframe with data to be cleaned
#' @param age The property age column in the dataframe to be cleaned
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_age(data)
#' scomm_age(data, AGE_OF_PROPERTY, BANDS)
#' @export
scomm_age <- function(data, age = CONSTRUCTION_AGE_BAND, epc = CURRENT_ENERGY_RATING, mute = NULL, ...) {
  cat("\U231B Cleaning property age for Scotland EPCs.\n")

  # Main cleaning block
  cleaning <- data %>%
    dplyr::mutate(EPC_PROPERTY_AGE = case_when(
      {{age}} == "before 1919" ~ "Pre_1919",
      {{age}} %in% c("1919-1929",
                        "1930-1949") ~ "1919_1949",
      {{age}} %in% c("1950-1964",
                        "1965-1975",
                        "1976-1983") ~ "1950_1983",
      {{age}} == "1984-1991" ~ "1984_1991",
      {{age}} %in% c("1992-1998",
                        "1999-2002") ~ "1992_2002",
      {{age}} %in% c("2003-2007",
                        "2008 onwards") ~ "Post_2002",
      {{age}} %in% c("2003-2007",
                        "2008 onwards") &
        {{epc}} %in% c("E", "F", "G") ~ "Assumed EPC Error",
      # Then all others categorised as unknown
      TRUE ~ "Unknown"
      )
    ) %>%
    dplyr::select(UPRN, EPC_PROPERTY_AGE, {{age}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed property age cleaning. Adding "prop_age" object to the environment.\n')
  assign("prop_age", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned data vs. original EPC data from data input of ", ensym(age), ":\n", sep = "")
    # TODO maybe a better way of having these tables print out
    print(table(cleaning$EPC_PROPERTY_AGE, cleaning[[ensym(age)]], useNA = "ifany"))
  }

}

######### Property Tenure #########
#' Property tenure cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @param data The dataframe with data to be cleaned
#' @param tenure The column in the dataframe to be cleaned
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_tenure(data)
#' scomm_tenure(data, PROP_TENURE)
#' @export
scomm_tenure <- function(data, tenure = TENURE, mute = NULL, ...) {
  cat("\U231B Cleaning Property Tenure for Scotland EPCs.\n")

  cleaning <- data %>%
    dplyr::mutate(EPC_TENURE_PRIMARY = case_when(
      str_detect({{tenure}}, regex("owner-occupied|owned|owner|homeowner|rented \\(private\\)|private tenant|privately rented|private rented", ignore_case = T)) ~ "Private",
      str_detect({{tenure}}, regex("registered social landlord|rsl|social tenant|rented \\(social\\)|council|local authority|housing association|house association|sheltered", ignore_case = T)) ~ "Social",
      # Then all others categorised as unknown
      TRUE ~ "Unknown"
      )
    ) %>%
    dplyr::mutate(EPC_TENURE_SECONDARY = case_when(
      str_detect({{tenure}}, regex("owner-occupied|owned|owner|homeowner", ignore_case = T)) ~ "Owner Occupied",
      str_detect({{tenure}}, regex("rented \\(private\\)|private tenant|privately rented|private rented", ignore_case = T)) ~ "Private",
      str_detect({{tenure}}, regex("housing association|house association", ignore_case = T)) ~ "Housing association",
      str_detect({{tenure}}, regex("council|local authority", ignore_case = T)) ~ "Local Authority",
      # Then all others categorised as unknown
      TRUE ~ "Unknown"
    )
    ) %>%
    dplyr::select(UPRN, EPC_TENURE_PRIMARY, EPC_TENURE_SECONDARY, {{tenure}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed tenure cleaning. Adding "prop_tenure" object to the environment.\n')
  assign("prop_tenure", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned data vs. original EPC data from data input of ", ensym(tenure), ":\n", sep = "")
    print(table(cleaning$EPC_TENURE_PRIMARY, cleaning[[ensym(tenure)]], useNA = "ifany"))

    cat("Cross-tab of cleaned data vs. original EPC data from data input of ", ensym(tenure), ":\n", sep = "")
    print(table(cleaning$EPC_TENURE_SECONDARY, cleaning[[ensym(tenure)]], useNA = "ifany"))
  }
}

######### Flat floor level #########
#' Flat floor level cleaning for Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param floor The column in the dataframe to be cleaned with flat floor info.
#' @param ptype The column in the dataframe with property type data.
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_flatfloor(data)
#' scomm_flatfloor(data, FLOORS, PTYPE)
#' @export
scomm_flatfloor <- function(data, floor = FLOOR_LEVEL, ptype = PROPERTY_TYPE, mute = NULL, ...) {
  cat("\U231B Cleaning flat floor data for Scotland EPCs.\n")

  cleaning <- data %>%
    dplyr::mutate(EPC_FLOOR_LEVEL = case_when(
      # TODO add in basement floor as it's own floor rather than ground floor on top of basement floor
      # Not many but you can still get them where it's obvious
      {{floor}} %in% c("ground floor", "basement") ~ "Ground Floor",
      {{floor}} == "mid floor" ~ "Mid Floor",
      {{floor}} == "top floor" ~ "Top Floor",
      {{ptype}} %in% c("Bungalow", "House", "Park home") ~ "Not Applicable",
      # TODO look into using flat top storey data and if we can use floor type and roof type to infer errors
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    dplyr::select(UPRN, EPC_FLOOR_LEVEL, FLOOR_LEVEL)

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed floor level cleaning. Adding "flat_floor_level" object to the environment.\n')
  assign("flat_floor_level", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned data vs. original EPC data from data input of ", ensym(floor), ":\n", sep = "")
    print(table(cleaning$EPC_FLOOR_LEVEL, cleaning[[ensym(floor)]], useNA = "ifany"))
  }

}

######### Habitable Rooms and TFA #########
#' Habitable rooms and total floor area cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @param data The dataframe with data to be cleaned
#' @param rooms The column in the dataframe to be cleaned with habitable room counts. Expects number strings (e.g. "7" not "seven")
#' @param tfa The column in the dataframe containing floor area.
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_rooms(data)
#' scomm_rooms(data, HABITABLE_ROOMS_NEW, TFA)
#' @export
scomm_rooms <- function(data, rooms = NUMBER_HABITABLE_ROOMS, tfa = TOTAL_FLOOR_AREA, mute = NULL, ...) {
  cat("\U231B Cleaning habitable room data for Scotland EPCs.\n")

  cleaning <- data %>%
    dplyr::mutate({{rooms}} := as.numeric({{rooms}}),
                  TOTAL_FLOOR_AREA = as.numeric({{tfa}})) %>%
    dplyr::mutate(EPC_HAB_ROOMS = case_when(
      {{rooms}} %in% c(1, 2) ~ "0-2",
      {{rooms}} == 3 ~ "3",
      {{rooms}} == 4 ~ "4",
      {{rooms}} == 5 ~ "5",
      {{rooms}} == 6 ~ "6",
      {{rooms}} == 7 |
        {{rooms}} == 8 ~ "7-8",
      {{rooms}} >= 9 &
        {{rooms}} <= max({{rooms}}) ~ ">9",
      # Then all others categorised as unknown
      TRUE ~ "Unknown"
      )
    ) %>%
    dplyr::select(UPRN, EPC_HAB_ROOMS, {{rooms}}, {{tfa}})

    # Assign the new table to the current table or a new data table
    cat('\u2705 - Completed habitable rooms cleaning. Adding "hab_rooms_tfa" object to the environment.\n')
    assign("hab_rooms_tfa", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Assign the new table to the current table or a new data table
    cat('Completed habitable rooms cleaning. Adding "hab_rooms_tfa" object to the environment.\n')
    assign("hab_rooms_tfa", cleaning, envir = .GlobalEnv)
  }

}

######### Wall Construction #########
#' Wall Construction cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param walls The column in the dataframe to be cleaned.
#' @param imp The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_walls(data)
#' scomm_walls(data, WALL_TYPE)
#' @export
scomm_walls <- function(data, walls = WALL_DESCRIPTION, imp = IMPROVEMENTS, mute = NULL, ...) {
  cat("\U231B Cleaning wall details for Scotland EPCs.\n")

  # Count the maximum number of elements in any row
  # This counts the number of | elements and adds 1 (assumes there is always a wall type after the final |)
  # This helps to number the amount of columns needed to place the seperated strings into
  max_elements <- max(str_count(data[[ensym(walls)]], "\\|")) + 1

  # Create column names sequentially based on the number of elements
  column_names <- paste0("WALL_DETAILS_", seq_len(max_elements))

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    tidyr::separate({{walls}}, into = column_names, sep = " \\| ",
             fill = "right", remove = FALSE) %>%
    dplyr::mutate(EPC_WALL_CONST_1 = case_when(
                      str_detect(WALL_DETAILS_1, regex("cavity wall", ignore_case = TRUE)) ~ "Cavity Construction",
                      str_detect(WALL_DETAILS_1, regex("timber frame", ignore_case = TRUE)) ~ "Timber Frame",
                      str_detect(WALL_DETAILS_1, regex("system built", ignore_case = TRUE)) ~ "System Built",
                      str_detect(WALL_DETAILS_1, regex("granite|whinstone|sandstone|solid brick|limestone|cob",
                                                     ignore_case = TRUE)) ~ "Solid Brick or Stone",
                      str_detect(WALL_DETAILS_1, regex("system built", ignore_case = TRUE)) ~ "System Built",
                      str_detect(WALL_DETAILS_1, regex("Park home wall", ignore_case = TRUE)) ~ "Park Home Wall",
                      str_detect(WALL_DETAILS_1, regex("Cob", ignore_case = TRUE)) ~ "Cob",
                      # Then all others categorised as unknown
                      TRUE ~ "Unknown")
                    ) %>%
    dplyr::mutate(EPC_WALL_CONST_1 = case_when(
      EPC_WALL_CONST_1 == "Unknown" &
        str_detect({{imp}}, regex("Cavity wall insulation")) ~ "Cavity Construction",
      TRUE ~ EPC_WALL_CONST_1
      )
    ) %>%
    # Extract numbers from the 'strings' column using dplyr and regular expressions
    dplyr::mutate(WALL_U_VALUES_1 = ifelse(grepl("Average thermal transmittance ([0-9.]+)", WALL_DETAILS_1),
                                           as.double(stringr::str_extract(WALL_DETAILS_1, "(?<=Average thermal transmittance )\\d+\\.?\\d*")),
                                           NA_real_)) %>%
    dplyr::mutate(EPC_WALL_INS_1 = case_when(
      # As in rdSAP for Scotland anything below 0.22 must be improved beyond the standard as built assumption
      # TODO add in any logic we can to include age band as well - maybe more we can squeeze
      WALL_U_VALUES_1 <= 0.21 |
      str_detect(WALL_DETAILS_1, regex("additional insulation|external insulation|insulated \\(assumed\\)|filled cavity|internal insulation|partial insulation \\(assumed\\)", ignore_case = TRUE)) ~ "Insulated",
      # As in rdSAP for Scotland anything above must be as built assumption (highest is timber frame Age Band A - 2.5)
      WALL_U_VALUES_1 >= 2.6 |
      str_detect(WALL_DETAILS_1, regex("no insulation|, as built$", ignore_case = TRUE)) ~ "Uninsulated",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    dplyr::mutate(EPC_WALL_INS_1 = case_when(
      EPC_WALL_INS_1 == "Unknown" &
        str_detect({{imp}}, regex("Cavity wall insulation|Internal or external wall insulation")) ~ "Uninsulated",
      TRUE ~ EPC_WALL_INS_1
      )
    ) %>%
    dplyr::select(UPRN, EPC_WALL_CONST_1, EPC_WALL_INS_1, WALL_DETAILS_1, WALL_U_VALUES_1)

    # Assign the new table to the current table or a new data table
    cat('\u2705 - Completed wall type and insulation cleaning. Adding "wall_details" object to the environment.\n')
    assign("wall_details", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned wall construction data vs. original EPC data for first referenced wall element from data input of ", ensym(walls), ":\n", sep = "")
    print(table(cleaning$EPC_WALL_CONST_1, cleaning$WALL_DETAILS_1, useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned wall insulation data vs. original EPC data for first referenced wall element from data input of ", ensym(walls), ":\n", sep = "")
    print(table(cleaning$EPC_WALL_INS_1, cleaning$WALL_DETAILS_1, useNA = "ifany"))
  }

}

######### Roof and Loft Details #########
#' Roof Construction and Loft insulation cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param roof The column in the dataframe containing roof details (expecting long, pipe (|) delimited string in each row)
#' @param ages The property age column in the dataframe to help infer insulation thickness.
#' @param imp The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_roof(data)
#' scomm_roof(data, ROOF_TYPE, AGE_BANDS, RETROFIT_OPTIONS)
#' @export
scomm_roof <- function(data, roofs = ROOF_DESCRIPTION, ages = CONSTRUCTION_AGE_BAND, imp = IMPROVEMENTS, mute = NULL, ...) {
  cat("\U231B Cleaning roof details for Scotland EPCs.\n")

  # TODO needs major thought on how to deal with insulation in RIR or flat roof. Right now the loft insulation thickness only really applies for those with a loft
  # Flat roof can be labelled as "no loft" but then where does the insulation level go for that?
  # Maybe a column for insulated "yes/no" and then another for thickness?

  # Count the maximum number of elements in any row
  # This counts the number of | elements and adds 1 (assumes there is always a wall type after the final |)
  # This helps to number the amount of columns needed to place the seperated strings into
  max_elements <- max(str_count(data[[ensym(roofs)]], "\\|")) + 1

  # Create column names sequentially based on the number of elements
  column_names <- paste0("ROOF_DETAILS_", seq_len(max_elements))

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    tidyr::separate({{roofs}}, into = column_names, sep = " \\| ",
                    fill = "right", remove = FALSE) %>%
    dplyr::mutate(EPC_ROOF_CONST_1 = case_when(
      grepl("(?i)roof room", ROOF_DETAILS_1) &
        grepl("(?i)thatched", ROOF_DETAILS_1) ~ "Thatched room in roof",
      grepl("(?i)roof room", ROOF_DETAILS_1) ~ "Room in roof",
      grepl("(?i)flat", ROOF_DETAILS_1) ~ "Flat roof",
      grepl("(?i)pitched", ROOF_DETAILS_1) ~ "Pitched roof",
      grepl("(?i)thatched", ROOF_DETAILS_1) ~ "Thatched roof",
      grepl("(?i)another dwelling above|other premises above", ROOF_DETAILS_1) ~ "Another dwelling above",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    dplyr::mutate(EPC_ROOF_CONST_1 = case_when(
      EPC_ROOF_CONST_1 == "Unknown" &
        stringr::str_detect({{imp}}, regex("room-in-roof", ignore_case = TRUE)) ~ "Room in roof",
      EPC_ROOF_CONST_1 == "Unknown" &
        stringr::str_detect({{imp}}, regex("flat roof insulation", ignore_case = TRUE)) ~ "Flat roof",
      EPC_ROOF_CONST_1 == "Unknown" &
        stringr::str_detect({{imp}}, regex("loft insulation", ignore_case = TRUE)) ~ "Pitched roof",
      TRUE ~ EPC_ROOF_CONST_1
      )
    ) %>%
    # Extract numbers from the 'strings' column using dplyr and regular expressions
    dplyr::mutate(ROOF_U_VALUES_1 = ifelse(grepl("Average thermal transmittance ([0-9.]+)", ROOF_DETAILS_1),
                                      as.double(stringr::str_extract(ROOF_DETAILS_1, "(?<=Average thermal transmittance )\\d+\\.?\\d*")),
                                      NA_real_)) %>%
    dplyr::mutate(EPC_ROOF_INS_1 = case_when(
      # TODO insulated flat roof or RIR is missing here or needs more thought?
      EPC_ROOF_CONST_1 %in% c("Flat roof", "Another dwelling above") ~ "No loft",
      # As in rdSAP for Scotland anything below 0.35 wouldn't be retrofitted according to Item A; Appendix T
      # TODO add in any logic we can to include age band as well - maybe more we can squeeze
      ROOF_U_VALUES_1 <= 0.17 |
      # PEAT will then assume that all roof space is loft space if it's 250mm+. Probably fine for the very low ones, unlikely to be flat roof if it's very low
        grepl("250|270|300|350|400|400+", ROOF_DETAILS_1) ~ ">250 mm",
      grepl("100|150|200", ROOF_DETAILS_1) ~ "100-249 mm",
      # As in rdSAP for Scotland anything above 2.3 must be uninsulated
      ROOF_U_VALUES_1 >= 2.3 |
        grepl("(?i)0|12|25|50|75|no insulation|limited insulation", ROOF_DETAILS_1) ~ "0-99 mm",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    # Add in what we can and be conservative with estimates where there is limited data
    # If description is "insulated" then we can go by age band to infer some thickness or u value based on type
      dplyr::mutate(EPC_ROOF_INS_1 = case_when(
        EPC_ROOF_INS_1 == "Unknown" &
        # Loft insulation is actually anything below 150 mm but we can be a little conservative here I think
          grepl("(?i)room-in-roof|flat roof insulation|loft insulation", {{imp}}) ~ "0-99 mm",
        # Anything Age band F and below is assumed to have >0.68 U-value which would be in the 0-99 mm range
        # It may have been retrofitted since being built, but we dont know that...
        EPC_ROOF_INS_1 == "Unknown" &
          EPC_ROOF_CONST_1 %in% c("Flat roof", "Room in roof", "Pitched roof") &
          {{ages}} %in% c("before 1919", "1919-1929", "1930-1949", "1950-1964",
                          "1965-1975", "1976-1983") &
          stringr::str_detect(ROOF_DETAILS_1, regex("insulated|\\bloft insulation\\b", ignore_case = TRUE)) ~ "0-99 mm",
        # Pitched, insulated at rafters has slightly different assumptions compared to if its not known after a certain point. It never reaches 0.17 U value
        EPC_ROOF_INS_1 == "Unknown" &
          (EPC_ROOF_CONST_1 %in% c("Flat roof", "Room in roof") |
          stringr::str_detect(ROOF_DETAILS_1, regex("insulated at rafters", ignore_case = TRUE))) &
          {{ages}} %in% c("1984-1991", "1992-1998", "1999-2002",
                          "2003-2007", "2008 onwards") &
          stringr::str_detect(ROOF_DETAILS_1, regex("insulated|\\bloft insulation\\b", ignore_case = TRUE)) ~ "100-249 mm",
        # Anything Age band J and above is assumed to have <0.16 U-value which would be in the >250 mm range
        EPC_ROOF_INS_1 == "Unknown" &
          EPC_ROOF_CONST_1 == "Pitched roof" &
          {{ages}} %in% c("2003-2007", "2008 onwards") &
          stringr::str_detect(ROOF_DETAILS_1, regex("insulated|\\bloft insulation\\b", ignore_case = TRUE)) ~ ">250 mm",
        TRUE ~ EPC_ROOF_INS_1)
    ) %>%
    dplyr::select(UPRN, EPC_ROOF_CONST_1, EPC_ROOF_INS_1, ROOF_DETAILS_1, ROOF_U_VALUES_1, {{ages}})

    # Assign the new table to the current table or a new data table
    cat('\u2705 - Completed roof type and insulation cleaning. Adding "roof_details" object to the environment.\n')
    assign("roof_details", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    cat("Cross-tab of cleaned roof construction data vs. original EPC data for first referenced roof element from data input of ", ensym(roofs), ":\n", sep = "")
    print(table(cleaning$EPC_ROOF_CONST_1, cleaning$ROOF_DETAILS_1, useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned roof insulation data vs. original EPC data for first referenced roof element from data input of ", ensym(roofs), ":\n", sep = "")
    print(table(cleaning$EPC_ROOF_INS_1, cleaning$ROOF_DETAILS_1, useNA = "ifany"))
  }
}

######### Floor Details #########
#' Floor Construction and insulation cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param floors The column in the dataframe to be cleaned.
#' @param ages The property age column in the dataframe to help infer insulation thickness.
#' @param imp The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_floor(data)
#' scomm_floor(data, FLOOR_TYPE, AGE_BANDS)
#' @export
scomm_floor <- function(data, floors = FLOOR_DESCRIPTION, ages = CONSTRUCTION_AGE_BAND, imp = IMPROVEMENTS, mute = NULL, ...) {
  cat("\U231B Cleaning floor details for Scotland EPCs.\n")

  # Count the maximum number of elements in any row
  # This counts the number of | elements and adds 1 (assumes there is always a wall type after the final |)
  # This helps to number the amount of columns needed to place the seperated strings into
  max_elements <- max(str_count(data[[ensym(floors)]], "\\|")) + 1

  # Create column names sequentially based on the number of elements
  column_names <- paste0("FLOOR_DETAILS_", seq_len(max_elements))

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    tidyr::separate({{floors}}, into = column_names, sep = " \\| ",
                    fill = "right", remove = FALSE)

  # TODO a better, less hacky way of dealing with FLOOR_DETAILS_2 not getting created would be great...
  if (!"FLOOR_DETAILS_2" %in% colnames(cleaning)) {
    cleaning <- cleaning %>%
      mutate(FLOOR_DETAILS_2 = NA_character_)
  }

    # Extract numbers from the 'strings' column using dplyr and regular expressions
  cleaning <- cleaning %>%
    dplyr::mutate(FLOOR_U_VALUES_1 = ifelse(grepl("Average thermal transmittance ([0-9.]+)", FLOOR_DETAILS_1),
                                            as.double(stringr::str_extract(FLOOR_DETAILS_1, "(?<=Average thermal transmittance )\\d+\\.?\\d*")),
                                            NA_real_)) %>%
    # TODO age bands to infer anything beyond Age band C is solid floor?
    dplyr::mutate(EPC_FLOOR_CONST_1 = case_when(
      str_detect(FLOOR_DETAILS_1, regex("solid", ignore_case = TRUE)) |
        (FLOOR_DETAILS_1 == "Conservatory" &
           str_detect(FLOOR_DETAILS_2, regex("solid", ignore_case = TRUE))) ~ "Solid",
      str_detect(FLOOR_DETAILS_1, regex("suspended", ignore_case = TRUE)) |
        (FLOOR_DETAILS_1 == "Conservatory" &
           str_detect(FLOOR_DETAILS_2, regex("suspended", ignore_case = TRUE))) ~ "Suspended",
      str_detect(FLOOR_DETAILS_1, regex("dwelling below\\)|\\(other premises below\\)|unheated space|external air", ignore_case = TRUE)) |
        (FLOOR_DETAILS_1 == "Conservatory" &
           str_detect(FLOOR_DETAILS_2, regex("dwelling below\\)|\\(other premises below\\)|unheated space|external air", ignore_case = TRUE))) ~ "Unheated space/other premise below",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    dplyr::mutate(EPC_FLOOR_CONST_1 = case_when(
      EPC_FLOOR_CONST_1 == "Unknown" &
        str_detect({{imp}}, regex("solid floor")) ~ "Solid",
      EPC_FLOOR_CONST_1 == "Unknown" &
        str_detect({{imp}}, regex("suspended floor")) ~ "Suspended",
      TRUE ~ EPC_FLOOR_CONST_1
      )
    ) %>%
    dplyr::mutate(EPC_FLOOR_INS_1 = case_when(
      # As in rdSAP for Scotland anything below 0.18 must be equivalent to not needing a floor insulation retrofit
      # TODO add in any logic we can to include age band as well - maybe more we can squeeze
      FLOOR_U_VALUES_1 <= 0.18 |
        str_detect(FLOOR_DETAILS_1, regex(", insulated|insulated \\(assumed\\)", ignore_case = TRUE)) |
          (FLOOR_DETAILS_1 == "Conservatory" &
           str_detect(FLOOR_DETAILS_2, regex(", insulated|insulated \\(assumed\\)", ignore_case = TRUE))) ~ "Insulated",
      # As in rdSAP for Scotland anything at or above 0.5 must be equivalent to needing a floor insulation retrofit
      # Anything lower rdSAP wont recommend even though it is supposed to get to 0.25/0.18... (W1/W2 in Appendix T)
      FLOOR_U_VALUES_1 >= 0.5 |
      str_detect(FLOOR_DETAILS_1, regex("limited insulation|no insulation|, as built$", ignore_case = TRUE)) |
        (FLOOR_DETAILS_1 == "Conservatory" &
           str_detect(FLOOR_DETAILS_2, regex("limited insulation|no insulation|, as built$", ignore_case = TRUE))) ~ "Uninsulated",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    dplyr::mutate(EPC_FLOOR_INS_1 = case_when(
      EPC_FLOOR_INS_1 == "Unknown" &
        str_detect({{imp}}, regex("Floor insulation")) ~ "Uninsulated",
      TRUE ~ EPC_FLOOR_INS_1
      )
    ) %>%
    dplyr::select(UPRN, EPC_FLOOR_CONST_1, EPC_FLOOR_INS_1, FLOOR_DETAILS_1, FLOOR_U_VALUES_1)

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed floor type and insulation cleaning. Adding "floor_details" object to the environment.\n')
  assign("floor_details", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned floor construction data vs. original EPC data for first referenced wall element from data input of ", ensym(floors), ":\n", sep = "")
    print(table(cleaning$EPC_FLOOR_CONST_1, cleaning$FLOOR_DETAILS_1, useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of cleaned floor insulation data vs. original EPC data for first referenced wall element from data input of ", ensym(floors), ":\n", sep = "")
    print(table(cleaning$EPC_FLOOR_INS_1, cleaning$FLOOR_DETAILS_1, useNA = "ifany"))
  }

}


######### Main Heating Fuel and System Details #########
#' Main Heating Fuel and System cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param m_heat The column in the dataframe to be cleaned.
#' @param meter The column in the dataframe to be cleaned.
#' @param imp The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_heating(data)
#' scomm_heating(data, HEATING_SYSTEM_DETAILS)
#' @export
scomm_heating <- function(data, m_heat = MAINHEAT_DESCRIPTION, meter = ENERGY_TARIFF, imp = IMPROVEMENTS, mute = NULL, ...) {
  cat("\U231B Cleaning main heating system for Scotland EPCs.\n")

  # Count the maximum number of elements in any row
  # This counts the number of | elements and adds 1 (assumes there is always a wall type after the final |)
  # This helps to number the amount of columns needed to place the seperated strings into
  max_elements <- max(str_count(data[[ensym(m_heat)]], "\\|")) + 1

  # Create column names sequentially based on the number of elements
  column_names <- paste0("HEATING_DETAILS_", seq_len(max_elements))

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    tidyr::separate({{m_heat}}, into = column_names, sep = " \\| ",
                    fill = "right", remove = FALSE) %>%
    # TODO are the HEATING_DETAILS_2 systems part of a hybrid system? Or are they considered main heating for another part of the house?
    # May need consideration as a heat pump in HEATING_DETAILS_1 will mask the existence of oil heating as another main heat source
    dplyr::mutate(EPC_HEATING_FUEL_1 = case_when(
      # Always assume heat pumps are electric
      grepl("(?i)electric|electricity|trydan|Electricaire|heat pump", HEATING_DETAILS_1) ~ "Electricity",
      grepl("(?i)mains gas|gas", HEATING_DETAILS_1) &
        !grepl("(?i)lpg|liquid", HEATING_DETAILS_1) ~ "Mains Gas",
      # Word boundary for "oil" so that it doesnt match the "oil" in "boiler"
      grepl("(?i)\\boil\\b|b30k", HEATING_DETAILS_1) ~ "Oil",
      grepl("(?i)lpg|lng", HEATING_DETAILS_1) ~ "LPG",
      grepl("(?i)coal|smokeless fuel|anthracite|dual fuel|mineral", HEATING_DETAILS_1) ~ "Solid",
      grepl("(?i)wood|pellet|log|biomass|biofuel|bioethanol", HEATING_DETAILS_1) ~ "Biomass",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    # TODO explore more improvement data inferences
    dplyr::mutate(EPC_HEATING_FUEL_1 = case_when(
      EPC_HEATING_FUEL_1 == "Unknown" &
        grepl("(?i)storage heater", {{imp}}) ~ "Electricity",
      EPC_HEATING_FUEL_1 == "Unknown" &
        grepl("(?i)with biomass boiler", {{imp}}) ~ "Solid",
      TRUE ~ EPC_HEATING_FUEL_1)
    ) %>%
    dplyr::mutate(EPC_HEATING_SYSTEM_1 = case_when(
      grepl("(?i)no system present", HEATING_DETAILS_1) ~ "No heating or hot water system",
      grepl("(?i)boiler|condensing|combi", HEATING_DETAILS_1) ~ "Boiler",
      grepl("(?i)heat pump", HEATING_DETAILS_1) ~ "Heat Pump",
      grepl("(?i)room heater|room heaters|electric heaters", HEATING_DETAILS_1) ~ "Room Heater",
      grepl("(?i)storage heater|storage heaters", HEATING_DETAILS_1) ~ "Storage Heater",
      grepl("(?i)community|communal", HEATING_DETAILS_1) ~ "Community",
      grepl("(?i)stove|warm air|ceiling heating|micro-cogeneration", HEATING_DETAILS_1) ~ "Other",
      TRUE ~ "Unknown")
    ) %>%
    dplyr::mutate(EPC_HEATING_SYSTEM_1 = case_when(
      EPC_HEATING_SYSTEM_1 == "Unknown" &
        grepl("(?i)storage heater", {{imp}}) ~ "Electricity",
      EPC_HEATING_SYSTEM_1 == "Unknown" &
        grepl("(?i)with biomass boiler", {{imp}}) ~ "Solid",
      TRUE ~ EPC_HEATING_SYSTEM_1)
    ) %>%
    mutate(EPC_METER = case_when(
      {{meter}} %in% c("dual", "dual (24 hour)", "off-peak 18 hour") ~ "Dual",
      {{meter}} == "Single" ~ "Single",
      TRUE ~ "Unknown"
    )) %>%
    dplyr::select(UPRN, EPC_HEATING_FUEL_1, EPC_HEATING_SYSTEM_1, EPC_METER, HEATING_DETAILS_1, {{meter}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed heating and meter cleaning. Adding "main_heating" object to the environment.\n')
  assign("main_heating", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of main fuel type data vs. original EPC data for first referenced main heating description from data input of ", ensym(m_heat), ":\n", sep = "")
    print(table(cleaning$EPC_HEATING_FUEL_1, cleaning$HEATING_DETAILS_1, useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of main heating system data vs. original EPC data for first referenced main heating description from data input of ", ensym(m_heat), ":\n", sep = "")
    print(table(cleaning$EPC_HEATING_SYSTEM_1, cleaning$HEATING_DETAILS_1, useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of meter type data vs. original EPC data from data input of ", ensym(meter), ":\n", sep = "")
    print(table(cleaning$EPC_METER, cleaning[[ensym(meter)]], useNA = "ifany"))
  }

}

######### Secondary Heating Fuel and System Details #########
#' Secondary Heating Fuel and System cleaning for Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param s_heat The column in the dataframe to be cleaned
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_sec_heating(data)
#' scomm_sec_heating(data, SEC_HEATING_SYSTEM_DETAILS)
#' @export
scomm_sec_heating <- function(data, s_heat = SECONDHEAT_DESCRIPTION, mute = NULL, ...) {
  cat("\U231B Cleaning secondary heating system for Scotland EPCs.\n")

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    dplyr::mutate(EPC_SEC_HEATING_FUEL_1 = case_when(
      # Always assume heat pumps are electric
      grepl("(?i)none", {{s_heat}}) |
        {{s_heat}} %in% c("", " ") ~ "No secondary system",
      grepl("(?i)electric|electricity|trydan|Electricaire|heat pump", {{s_heat}}) ~ "Electricity",
      grepl("(?i)mains gas|gas", {{s_heat}}) &
        !grepl("(?i)lpg|liquid", {{s_heat}}) ~ "Mains Gas",
      grepl("(?i)wood|pellet|log|biomass|biofuel|rapeseed oil|bioethanol", {{s_heat}}) ~ "Biomass",
      # Word boundary for "oil" so that it doesnt match the "oil" in "boiler"
      grepl("(?i)\\boil\\b|b30k", {{s_heat}}) ~ "Oil",
      grepl("(?i)lpg|lng", {{s_heat}}) ~ "LPG",
      grepl("(?i)coal|smokeless fuel|anthracite|dual fuel|mineral", {{s_heat}}) ~ "Solid",
      # Then all others categorised as unknown
      TRUE ~ "Unknown")
    ) %>%
    dplyr::mutate(EPC_SEC_HEATING_SYSTEM_1 = case_when(
      grepl("(?i)none", {{s_heat}}) |
        {{s_heat}} %in% c("", " ") ~ "No secondary system",
      grepl("(?i)boiler|condensing|combi", {{s_heat}}) ~ "Boiler",
      grepl("(?i)heat pump", {{s_heat}}) ~ "Heat Pump",
      grepl("(?i)room heater|room heaters|electric heaters", {{s_heat}}) ~ "Room Heater",
      grepl("(?i)storage heater|storage heaters", {{s_heat}}) ~ "Storage Heater",
      grepl("(?i)community|communal", {{s_heat}}) ~ "Community",
      grepl("(?i)stove|warm air|ceiling heating|micro-cogeneration", {{s_heat}}) ~ "Other",
      TRUE ~ "Unknown")
    ) %>%
    dplyr::select(UPRN, EPC_SEC_HEATING_FUEL_1, EPC_SEC_HEATING_SYSTEM_1, {{s_heat}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed secondary heating cleaning. Adding "sec_heating" object to the environment.\n')
  assign("sec_heating", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    cat("Cross-tab of secondary heating fuel data vs. original EPC data for first referenced main heating description from data input of ", ensym(s_heat), ":\n", sep = "")
    print(table(cleaning$EPC_SEC_HEATING_FUEL_1, cleaning[[ensym(s_heat)]], useNA = "ifany"))

    # Some prints to help check data cleaning
    cat("Cross-tab of secondary heating system data vs. original EPC data for first referenced main heating description from data input of ", ensym(s_heat), ":\n", sep = "")
    print(table(cleaning$EPC_SEC_HEATING_SYSTEM_1, cleaning[[ensym(s_heat)]], useNA = "ifany"))
  }

}

######### Gas Grid Details #########
#' Gas grid inference from Scotland EPCs
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param grid The column in the dataframe with gas grid flag (expects "Y" and "N")
#' @param m_heat The column in the dataframe with main heating descriptions (if mains gas is used somewhere then assumed grid connection)
#' @param s_heat The column in the dataframe with secondary heating descriptions(if mains gas is used somewhere then assumed grid connection)
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_gas_grid(data)
#' scomm_gas_grid(data, GAS_GRID_FLAG, MAIN_HEATING_SYSTEM_DETAILS, SEC_HEATING_SYSTEM_DETAILS)
#' @export
scomm_gas_grid <- function(data, grid = MAINS_GAS_FLAG, m_heat = MAINHEAT_DESCRIPTION, s_heat = SECONDHEAT_DESCRIPTION, mute = NULL, ...) {
  cat("\U231B Inferring gas grid connection from EPCs\n")

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    dplyr::mutate(EPC_GAS_GRID_FLAG = case_when(
      grepl("(?i)mains gas", {{m_heat}}) |
        grepl("(?i)mains gas", {{s_heat}}) |
        {{grid}} %in% c("Y") ~ "Yes",
      {{grid}} %in% c("N") ~ "No",
      TRUE ~ "Unknown")
    ) %>%
    dplyr::select(UPRN, EPC_GAS_GRID_FLAG, {{grid}}, {{m_heat}}, {{s_heat}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed gas grid inference. Adding "gas_grid_flag" object to the environment.\n')
  assign("gas_grid_flag", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Some prints to help check data cleaning
    # TODO checks for fuel types in main and secondary heating. Difficult because lots of pipes and different combos of fuels/types
    cat("Cross-tab of gas grid inference data vs. original EPC data from data input of ", ensym(grid), ":\n", sep = "")
    print(table(cleaning$EPC_GAS_GRID_FLAG, cleaning[[ensym(grid)]], useNA = "ifany"))
  }

}

######### Glazing Details #########
#' Glazing data cleaning for Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param glaz The column in the dataframe to be cleaned.
#' @param multi The column in the dataframe to be cleaned with multiple glazing types.
#' @param age The column in the dataframe containing property age data.
#' @param imp The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_glazing(data)
#' scomm_glazing(data, GLAZING, type, multi)
#' @export
scomm_glazing <- function(data, glaz = WINDOWS_DESCRIPTION, type = GLAZED_TYPE, multi = MULTI_GLAZE_PROPORTION, eff = WINDOWS_ENERGY_EFF, age = CONSTRUCTION_AGE_BAND, imp = IMPROVEMENTS, mute = NULL, ...) {
  cat("\U231B Cleaning glazing for Scotland EPC data.\n")

  cleaning <- data %>%
    dplyr::mutate({{multi}} := as.numeric({{multi}}),
                  {{glaz}} := gsub("Description: ", "", {{glaz}})) %>%
    dplyr::mutate(GLAZING = case_when(
      # Single glazing or partial glazing
      grepl("(?i)some|single glazed", {{glaz}}) |
        # only overwrite with single glazing if it's very poor glazing efficiency
        # Might be odd to overwrite where it clearly says "double glazing" in some way with single glazing
        (grepl("(?i)single glazing", {{type}}) &
           {{eff}} == "Very Poor") |
        (grepl("(?i)partial", {{glaz}}) &
           # TODO investigate why some homes get 0 for multiple glazing - doesnt make any sense, but still use as better to be conservative...
           {{multi}} <= 50) ~ "Single/Partial",
      # Triple
      (grepl("(?i)high performance|triple", {{glaz}})) ~ "Triple",
      # Double in some way(that is not already labelled by the single/partial above)
      (grepl("(?i)double|multiple glazing throughout", {{glaz}})) ~ "Double",
      # Secondary
      (grepl("(?i)secondary", {{glaz}})) ~ "Secondary",
      TRUE ~ "Unknown")
    ) %>%
      dplyr::mutate(EPC_GLAZING_TYPE = case_when(
        # Pre 2003 double glazing (or broadly equivalent efficiency)
        GLAZING == "Secondary" ~ "Double Glazing (pre 2003)",
        GLAZING == "Double" &
          {{type}} == "double glazing installed before 2002" ~ "Double Glazing (pre 2003)",
        GLAZING == "Double" &
          {{type}} %in% c("double glazing, unknown install date", "double, known data", "not defined", "", " ") &
            {{age}} %in% c("before 1919", "1919-1929", "1930-1949",
                            "1950-1964", "1965-1975", "1976-1983",
                            "1984-1991", "1992-1998", "1999-2002") ~ "Double Glazing (pre 2003)",
        # If no real signs either way then use the older age bracket
        GLAZING == "Double" &
          {{age}} %in% c("", " ") ~ "Double Glazing (pre 2003)",
        # Post 2003 double glazing
        (GLAZING == "Double" &
          {{type}} == "double glazing installed during or after 2002") |
        (GLAZING == "Double" &
          {{type}} %in% c("double glazing, unknown install date", "double, known data", "not defined", "", " ") &
             {{age}} %in% c("2003-2007", "2008 onwards")) ~ "Double Glazing (post 2003)",
        {{type}} %in% c("triple, known data", "triple glazing") ~ "Triple Glazing",
        # Then all others categorised as unknown
        TRUE ~ GLAZING)
    ) %>%
    # dplyr::mutate(INFERENCE_FLAG = case_when(
    #   {{type}} %in% c("double glazing, unknown install date", "double, known data") ~ "Inferred glazing age",
    #   TRUE ~ "Direct")
    # ) %>%
    # TODO explore more improvement data inferences
    dplyr::mutate(EPC_GLAZING_TYPE = case_when(
      EPC_GLAZING_TYPE == "Unknown" &
        grepl("(?i)single glazed windows", {{imp}}) ~ "Single/Partial",
        {{glaz}} %in% c("double glazing, unknown install date", "double, known data", "not defined", "", " ") &
        grepl("(?i)Replacement glazing units", {{imp}}) ~ "Double Glazing (pre 2003)",
      TRUE ~ EPC_GLAZING_TYPE)
    ) %>%
    dplyr::select(UPRN, EPC_GLAZING_TYPE, GLAZING, {{glaz}}, {{type}}, {{multi}}, {{eff}}, {{age}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed glazing type. Adding "glazing_type" object to the environment.\n')
  assign("glazing_type", cleaning, envir = .GlobalEnv)

  # TODO some tables - but needs thought as the logic isnt just a 1:1 or 2:1 mapping

}

######### SAP Derived Energy Details #########
#' SAP derived variables from Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param sap The column in the dataframe containing SAP score
#' @param energy The column in the dataframe containing energy usage (in this case kwh/m2/year).
#' @param tfa The column in the dataframe containing total floor area (to convert energy/m2 into total energy).
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_energy(data)
#' scomm_energy(data, SAP_SCORE, ENERGY_USAGE, FLOOR_AREA)
#' @export
scomm_energy <- function(data, sap = CURRENT_ENERGY_EFFICIENCY, energy = ENERGY_CONSUMPTION_CURRENT, tfa = TOTAL_FLOOR_AREA, mute = NULL, ...) {
  cat("\U231B Cleaning SAP-derived energy related data for Scotland EPC data.\n")

  cleaning <- data %>%
    mutate(EPC_SAP_SCORE = as.numeric({{sap}}),
           energy_demand = as.numeric({{energy}}),
           floor_area = as.numeric({{tfa}})) %>%
    mutate(EPC_SAP_BAND = case_when(
      EPC_SAP_SCORE <= 20 ~ "G",
      EPC_SAP_SCORE >= 21 &
        EPC_SAP_SCORE <= 38 ~ "F",
      EPC_SAP_SCORE >= 39 &
        EPC_SAP_SCORE <= 54 ~ "E",
      EPC_SAP_SCORE >= 55 &
        EPC_SAP_SCORE <= 68 ~ "D",
      EPC_SAP_SCORE >= 69 &
        EPC_SAP_SCORE <= 80 ~ "C",
      EPC_SAP_SCORE >= 81 &
        EPC_SAP_SCORE <= 91 ~ "B",
      EPC_SAP_SCORE >= 92 ~ "A",
    )) %>%
    mutate(EPC_ENERGY_DEMAND = energy_demand * floor_area) %>%
    dplyr::select(UPRN, EPC_SAP_BAND, {{sap}}, EPC_SAP_SCORE, EPC_ENERGY_DEMAND)

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed SAP energy data cleaning. Adding "SAP_energy" object to the environment.\n')
  assign("SAP_energy", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Visualise SAP score to SAP band
    cat("Cross-tab of final SAP band vs. SAP score input from ", ensym(sap), ":\n", sep = "")
    print(table(cleaning$EPC_SAP_BAND, cleaning$EPC_SAP_SCORE))
  }

}

######### SAP Derived Heat Details #########
#' SAP derived variables from Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param space_heat The column in the dataframe containing space heating demand
#' @param water_heat The column in the dataframe containing water heating demand
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_demand(data)
#' scomm_demand(data, SAP_SCORE, ENERGY_USAGE, FLOOR_AREA)
#' @export
scomm_demand <- function(data, space_heat = SPACE_HEATING_DEMAND, water_heat = WATER_HEATING_DEMAND, mute = NULL, ...) {
  cat("\U231B Cleaning SAP-derived heat related data for Scotland EPC data.\n")

  cleaning <- data %>%
    mutate({{space_heat}} := as.numeric({{space_heat}}),
           {{water_heat}} := as.numeric({{water_heat}})) %>%
    mutate(EPC_HEAT_DEMAND = {{space_heat}} + {{water_heat}}) %>%
    dplyr::select(UPRN, EPC_HEAT_DEMAND)

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed SAP heat demand data cleaning. Adding "SAP_heat" object to the environment.\n')
  assign("SAP_heat", cleaning, envir = .GlobalEnv)

}

######### SAP Derived Bill Details #########
#' SAP derived variables from Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param heat_cost The column in the dataframe containing space heating cost
#' @param water_cost The column in the dataframe containing water heating cost
#' @param lighting_cost The column in the dataframe containing lighting cost
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_bills(data)
#' scomm_bills(data, HEAT_COST, WATER_HEAT_COST, LIGHTING_COST)
#' @export
scomm_bills <- function(data, heat_cost = HEATING_COST_CURRENT, water_cost = HOT_WATER_COST_CURRENT, lighting_cost = LIGHTING_COST_CURRENT, mute = NULL, ...) {
  cat("\U231B Cleaning SAP-derived energy bill related data for Scotland EPC data.\n")

  cleaning <- data %>%
    mutate({{heat_cost}} := as.numeric({{heat_cost}}),
           {{water_cost}} := as.numeric({{water_cost}}),
           {{lighting_cost}} := as.numeric({{lighting_cost}})) %>%
    mutate(EPC_ENERGY_BILL = {{heat_cost}} + {{water_cost}} + {{lighting_cost}}) %>%
    dplyr::select(UPRN, EPC_ENERGY_BILL)

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed SAP energy bill data cleaning. Adding "SAP_bills" object to the environment.\n')
  assign("SAP_bills", cleaning, envir = .GlobalEnv)

}

######### SAP Derived Environmental Variables #########
#' SAP derived variables from Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param carbon The column in the dataframe containing current carbon emissions estimate
#' @param env The column in the dataframe containing environmental impact score
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_environmental(data)
#' scomm_environmental(data, CO2_CURRENT, WATER_HEAT_COST, LIGHTING_COST)
#' @export
scomm_environmental <- function(data, carbon = CO2_EMISSIONS_CURRENT, env = ENVIRONMENT_IMPACT_CURRENT, mute = NULL, ...) {
  cat("\U231B Cleaning SAP-derived energy bill related data for Scotland EPC data.\n")

  cleaning <- data %>%
    mutate(EPC_CARBON_EMISSIONS := as.numeric({{carbon}}),
           EPC_ENVIRONMENTAL_IMPACT_SCORE := as.numeric({{env}})) %>%
    mutate(EPC_ENVIRONMENTAL_IMPACT_BAND = case_when(
      EPC_ENVIRONMENTAL_IMPACT_SCORE <= 20 ~ "G",
      EPC_ENVIRONMENTAL_IMPACT_SCORE >= 21 &
        EPC_ENVIRONMENTAL_IMPACT_SCORE <= 38 ~ "F",
      EPC_ENVIRONMENTAL_IMPACT_SCORE >= 39 &
        EPC_ENVIRONMENTAL_IMPACT_SCORE <= 54 ~ "E",
      EPC_ENVIRONMENTAL_IMPACT_SCORE >= 55 &
        EPC_ENVIRONMENTAL_IMPACT_SCORE <= 68 ~ "D",
      EPC_ENVIRONMENTAL_IMPACT_SCORE >= 69 &
        EPC_ENVIRONMENTAL_IMPACT_SCORE <= 80 ~ "C",
      EPC_ENVIRONMENTAL_IMPACT_SCORE >= 81 &
        EPC_ENVIRONMENTAL_IMPACT_SCORE <= 91 ~ "B",
      EPC_ENVIRONMENTAL_IMPACT_SCORE >= 92 ~ "A",
    )) %>%
    dplyr::select(UPRN, EPC_CARBON_EMISSIONS, EPC_ENVIRONMENTAL_IMPACT_SCORE, EPC_ENVIRONMENTAL_IMPACT_BAND, {{env}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed SAP environmental impact data cleaning. Adding "env_impact" object to the environment.\n')
  assign("env_impact", cleaning, envir = .GlobalEnv)

  if(is.null(mute) || mute == FALSE){
    # Print table to visualise the score to bands
    cat("Cross-tab of final environmental impact band vs. environmental impact score input from ", ensym(env), ":\n", sep = "")
    print(table(cleaning$EPC_ENVIRONMENTAL_IMPACT_BAND, cleaning$EPC_ENVIRONMENTAL_IMPACT_SCORE, useNA = "ifany"))

  }

}

######### Energy Gen Details #########
#' Energy generation data cleaning for Scotland EPC data
#'
#' Standarised cleaning procedure for Home Analytics updates
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param lzc The column in the dataframe containing low zero carbon (LZC) tech
#' @param pv The column in the dataframe containing PV data. Expects numbers to be extractable after certain strings (e.g. ... Peak Power: 3.5 ...)
#' @param therm The column in the dataframe containing solar thermal flags (true, Y, N, false).
#' @param wind The column in the dataframe containing wind turbine counts.
#' @param mute option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_energy_gen(data)
#' scomm_energy_gen(data, LZC_ENERGY, PV_FLAG, THERMAL_FLAG, WIND_FLAG)
#' @export
scomm_energy_gen <- function(data, lzc = LZC_ENERGY_SOURCES, pv = PHOTO_SUPPLY, therm = SOLAR_WATER_HEATING_FLAG, wind = WIND_TURBINE_COUNT, mute = NULL, ...) {
  cat("\U231B Cleaning energy generation data for Scotland EPC data.\n")

  # TODO this is now a little slow with the if statements... fix that.
  cleaning <- data %>%
    mutate(Turbines = case_when(
      {{wind}} == "NULL" ~ "0",
      TRUE ~ {{wind}}
      )
    ) %>%
    mutate(Turbines = as.numeric(Turbines),
           PV_kwh = ifelse(grepl("Peak Power: ([0-9.]+)", {{pv}}),
                           as.double(stringr::str_extract({{pv}}, "(?<=Peak Power: )\\d+\\.?\\d*")),
                           NA_real_),
           PV_perc = ifelse(grepl("Array: Roof Area: ([0-9.]+)%", {{pv}}),
                            as.double(stringr::str_extract({{pv}}, "(?<=Array: Roof Area: )\\d+\\.?\\d*")),
                            NA_real_)
    ) %>%
    mutate(EPC_PV_FLAG = case_when(
      PV_kwh > 0 |
        PV_perc > 0 |
        grepl("(?i)Solar photovoltaics", {{lzc}}) ~ "Solar PV",
      TRUE ~ "No solar PV"
    )) %>%
    mutate(EPC_THERMAL_FLAG = case_when(
      grepl("(?i)Y|true", {{therm}}) |
        grepl("(?i)Solar water heating", {{lzc}}) ~ "Solar PV",
      TRUE ~ "No solar thermal"
    )) %>%
    mutate(EPC_WIND_FLAG = case_when(
      Turbines > 0 |
        grepl("(?i)wind turbine", {{lzc}}) ~ "Wind turbine",
      TRUE ~ "No wind turbine"
    )) %>%
    dplyr::select(UPRN, EPC_PV_FLAG, EPC_THERMAL_FLAG, EPC_WIND_FLAG, {{lzc}}, {{pv}}, {{therm}}, {{wind}})

  # Assign the new table to the current table or a new data table
  cat('\u2705 - Completed energy generation technology data cleaning. Adding "energy_generation" object to the environment.\n')
  assign("energy_generation", cleaning, envir = .GlobalEnv)

}


# TODO CHECK ALTERNATIVE_IMPROVEMENTS for any other mining opportunities
####### Generate full list of possible improvement measure names in EPC data #######
#' EPC Improvements data collection for Scotland EPCs
#'
#' Generates a list of unique EPC improvements from the Scottish EPC register
#' @import dplyr
#' @import tidyr
#' @param data The dataframe with data to be cleaned
#' @param imp The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter
#' @return A dataframe with the UPRN, cleaned data and original EPC data for onward processing
#' @examples
#' scomm_epc_improves(data)
#' scomm_epc_improves(data, "IMPROVEMENTS_EPC")
#' @export
epc_improves_scomm <- function(data, imp = IMPROVEMENTS) {
  cat("\U231B Processing retrofit improvement suggestions from Scotland EPC data.\n")

  # Count the maximum number of elements in any row
  # This counts the number of | elements and adds 1 (assumes there is always a wall type after the final |)
  # This helps to number the amount of columns needed to place the seperated strings into
  max_elements <- max(str_count(data[[imp]], "\\|")) + 1

  # Create column names sequentially based on the number of elements
  column_names <- paste0("IMPROVE_DETAILS_", seq_len(max_elements))

  # Separate the wall elements into multiple columns with seperate()
  cleaning <- data %>%
    tidyr::separate({{imp}}, into = column_names, sep = " \\| ",
                    fill = "right", remove = FALSE)

  # Preprocess specific columns using mutate_at
  df_processed <- cleaning %>%
    mutate_at(column_names, ~ gsub(".*Description:\\s*(.*?);.*", "\\1", .)) %>%
    filter_at(column_names, all_vars(!grepl("Indicative Cost", .)))

  # Apply unique() to each column using lapply()
  unique_values <- lapply(df_processed[column_names], unique)
  # Then unlist to bring into one long, continuous list and apply unique there
  # TODO this is a bit clunky...
  unique_values <- unique(unlist(unique_values))

  # Assign the list of unique measures and print
  print(unique_values)

  # Assign the new table to the current table or a new data table
  assign("unique_improvement_strings", unique_values, envir = .GlobalEnv)

}
