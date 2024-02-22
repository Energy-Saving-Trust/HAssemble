######### Address Matching Prototype #########
#' Matching EPC addresses to Ordnance Survey UPRNs
#'
#' Standarised matching procedure for Home Analytics updates
#' @import dplyr
#' @import stringr
#' @param addressesToMatch The dataframe with the addresses to be matched
#' @param rowID column with unique property key. Important to be able to join address matching results back to original EPC data.
#' @param add1 column with first line of address details. Intended to be building/property identifier (name/number), but often includes street name
#' @param add2 column with second line of address details. Intended to be street name, but often includes building identifiers
#' @param add3 column with third line of address details. Intended to be town/city
#' @param pcode column with post code of address. Full postcode (e.g. EH21 6TU). Key for filtering down relevant addresses to match against.
#' @param country optional column to set the country addresses are from. Options = E, S, W. Multiple allowed, but must be in quotations.
#' @param sql sql server table to pull AddressBase data from. Requires a valid ODBC connection.
#' @param path location to save interim and final files. Provide in quotations. Default is NULL and these will save to a generic location.
#' @return A dataframe with the UPRN for each address
#' @examples
#' epc_address_matching(addressesToMatch)
#' epc_address_matching(addressesToMatch, rowID, add1 = ADD_1, add2 = ADD_2, add3 = ADD_3, pcode = POSTIE, country = "S", sql = R_ADDRBASE_PLUS_20231014, path = "C:/Users/Data/")
#' epc_address_matching(addressesToMatch, rowID, add1 = ADD_1, add2 = ADD_2, add3 = ADD_3, pcode = POSTIE, country = c("E", "W"), sql = R_ADDRBASE_PLUS_20231014, path = "C:/Users/Data/")
#' @export

epc_address_matching <- function(addressesToMatch, rowID = BUILDING_REFERENCE_NUMBER, add1 = ADDRESS1, add2 = ADDRESS2,
                                 add3 = ADDRESS, pcode = POSTCODE, country = "S", sql = R_ADDRBASE_PLUS_20231014, path = NULL) {

  # Make a path if one isnt set by a user
  # Support for Windows and Linux users but it will dump it to your local drive so set a path!
  if (is.null(path)) {
    # Set default path to a generic location
    if (Sys.info()['sysname'] == 'Windows') {
      # Windows default path
      path <- file.path(Sys.getenv("USERPROFILE"), "Documents", "Address_Matching")
    } else {
      # Unix-like systems default path (Linux, macOS)
      path <- file.path(Sys.getenv("HOME"), "Address_Matching")
    }
    # Pathing in R is funny so you need to change the slashes to print the correct way around
    path <- gsub("\\\\", "/", path)
    cat("No path provided so saving files to here:", paste0('"', path, '"'))
  }

  ######### Load AddressBase data for EPCs #########
  cat("Step 1: Loading EPC addresses and master lookup table\n")

  #Reduce to necessary fields
  addresses <- addresses_to_match %>%
    dplyr::select(ROW_NUM, ADDRESS_1, ADDRESS_2, ADDRESS_3, POSTCODE)

  #Cleanse address fields
  # Remove instances of trailing, leading and double(+) spaces
  addresses <- addresses %>%
    # And instances of spaces in POSTCODE and commas in Address
    mutate(POSTCODE = stringr::str_replace_all(POSTCODE, " ", ""),
           across(matches("^ADDRESS"), ~str_replace_all(., ",", ""))) %>%
    mutate(across(where(is.character), str_squish)) %>%
    mutate(across(where(is.character), toupper)) %>%
    # remove period from the short form of saint (st. augustine to st augustine)
    mutate(across(matches("^ADDRESS"), ~str_replace_all(., "ST\\.", "ST ")))

  #Read in master addresses
  # TODO FIX THIS
  # Convert country to a vector of strings
  # Convert sql to a string
  sql_string <- as.character(ensym(sql))

  # Convert the vector of country values into a comma-separated string
  country_values <- paste0("'", country, "'", collapse = ", ")

  # Construct the SQL query string for IN operator
  country_filter <- paste0("COUNTRY IN (", country_values, ")")

  sql_query <- paste0("SELECT UPRN, UDPRN, SUB_BUILDING_NAME, BUILDING_NAME, BUILDING_NUMBER, SAO_START_NUMBER, SAO_TEXT, PAO_START_NUMBER,
                          PAO_START_SUFFIX, PAO_TEXT, STREET_DESCRIPTION, TOWN_NAME, ADMINISTRATIVE_AREA, POSTCODE_LOCATOR, COUNTRY FROM dbo.",
                      sql_string,
                      " WHERE
                      ", country_filter, "AND (CLASS LIKE 'R%' OR CLASS LIKE 'X%');")

  #Read in master addresses
  # TODO FIX THIS
  connect = odbcConnect('EST_RefData_30_Presentation')
  master = sqlQuery(connect, sql_query)

  # Close the ODBC connection when you're done
  odbcClose(connect)

  cat("AddressBase data loaded for", country, ".\n")

  # Convert all to characters
  master <- master %>%
    mutate(across(everything(), as.character),
           AB_POSTCODECOMPACT = str_replace_all(POSTCODE_LOCATOR, " ", "")) %>%
    mutate(across(where(is.character), str_squish)) %>%
    select(-(POSTCODE_LOCATOR))

  #Reduce master lookup down to postcodes held in target dataset
  master <- master %>%
    filter(AB_POSTCODECOMPACT %in% addresses$POSTCODE) %>%
    # Label the postcodes into unique groups
    # e.g. the multiple observations of AB101AU are PC_RANK = 1
    mutate(PC_RANK = dense_rank(AB_POSTCODECOMPACT))

  #Set number of postcodes per batch
  master_pc = length(unique(master$PC_RANK))
  batch = round(master_pc/4, digits = 0)

  #Create an index for each master batch
  master <- master %>%
    mutate(Batch = case_when(
      PC_RANK < batch ~ 1,
      PC_RANK >= batch &
        PC_RANK < batch*2 ~ 2,
      PC_RANK >= batch*2 &
        PC_RANK < batch*3 ~ 3,
      PC_RANK >= batch*3 ~ 4,
      TRUE ~ -999
    )) %>%
    select(-(PC_RANK))

  # TODO IF PATH IS NULL THEN SAVE SOMEWHERE AND TELL THEM WHERE IT'S SAVED
  for(i in 1:length(unique(master$Batch))){

    # check if required path exists
    if (!(file.exists(paste0(path, "RData/")))){
      # if not then make that directory
      print(paste0("Making an 'RData' folder in ", path))
      dir.create(file.path(paste0(path, "RData/")))
    }

    save <- master %>% filter(Batch == i) %>% select(-(Batch))
    saveRDS(save, file=paste0(path, "RData/master_", i, ".rds"))

    if(i == length(unique(master$Batch))){
      print("Batches for master OS files saved.")
      rm(save)
    }
  }

  #Assign EPC addresses to 1 of the 4 batches based on their postcode
  # This ensures that they get loaded in with the same master file below based on Postcode
  PC <- master %>% distinct(AB_POSTCODECOMPACT, .keep_all = TRUE)
  addresses <- addresses %>%
    left_join(PC[c("AB_POSTCODECOMPACT", "Batch")],
              by = c("POSTCODE" = "AB_POSTCODECOMPACT")) %>%
    # remove records with no postcode or matchable postcode to AddressBase
    # Wont be matchable in the current AM script anyway
    filter(!is.na(Batch))

  # Some 1-2k properties have a postcode that wont match - seems like mostly typos in postcodes.
  # Shame to remove entirely so save and see what we can do in future
  not_matched_to_AB <- addresses %>%
    filter(is.na(Batch))

  saveRDS(not_matched_to_AB, paste0(path, "RData/not_AB_RandX_Class_matchable_postcodes.rds"))

  rm(PC, not_matched_to_AB)

  for(i in 1:length(unique(addresses$Batch))){

    # check if required path exists
    if (!(file.exists(paste0(path, "RData/")))){
      # if not then make that directory
      print(paste0("Making an 'RData' folder in ", path))
      dir.create(file.path(paste0(path, "RData/")))
    }

    save <- addresses %>% filter(Batch == i) %>% select(-(Batch))
    saveRDS(save, file=paste0(path, "RData/address_", i, ".rds"))

    if(i == length(unique(addresses$Batch))){
      print("Batches for addresses files saved.")
      rm(save)
    }
  }

  #Clear workspace and memory
  rm(list=setdiff(ls(), "path"))
  gc()

  ######### Address formatting #########

  print("Step 2: Creating possible address formats")

  #Loop through 4 batches of addresses/master pairs for cleaning

  for (b in 1:4) {

    #Load objects
    addresses <- readRDS(paste0(path, "/RData/address_", b, ".rds"))
    master <- readRDS(paste0(path, "/RData/master_", b, ".rds"))

    # General high level cleaning
    # Remove apostrophes from all columns
    addresses[] <- lapply(addresses, function(x) gsub("['’]", "", x))
    addresses[] <- lapply(addresses, function(x) gsub("JOHNSTON GARDENS EAST", "JOHNSTON GARDENS", x))

    addresses <- addresses %>%
      mutate(ADDRESS_2 = case_when(
        str_detect(ADDRESS_1, regex("GLAITNESS FARMHOUSE", ignore_case = TRUE)) &
          POSTCODE == "KW151TN" ~ "GLAITNESS ROAD",
        TRUE ~ ADDRESS_2
      ))

    #EPC
    #Format 0:
    # The most basic concatenation of the address fields as they come - no manipulation or skipping columns
    # Was previously in ADD3 below and so was struggling to get good matches for say
    # ADDRESS_1     ADDRESS_2     ADDRESS_3     POSTCODE
    # 74            Kaimes Road   Edinburgh     EH126LW
    addresses <- addresses %>%
      mutate(ADD0 = case_when(
        ADDRESS_1 != "" &
          ADDRESS_2 != "" &
          ADDRESS_3 != "" &
          POSTCODE != "" ~ paste0(addresses$ADDRESS_1, " ", addresses$ADDRESS_2, ", ", addresses$ADDRESS_3, ", ", addresses$POSTCODE),
        TRUE ~ ""
      ))

    #Format 1:
    addresses <- addresses %>%
      mutate(ADD1 = case_when(
        ADDRESS_1 != "" ~ paste(addresses$ADDRESS_1, addresses$ADDRESS_3, addresses$POSTCODE, sep = ", "),
        TRUE ~ ""
      ))

    #Format 2:
    addresses$ADD2 = paste(addresses$ADDRESS_1, addresses$POSTCODE, sep = ", ")

    #Format 3:
    # FIXME cant we just squash the columns together and if they have a gap(or gaps) then remove the gap(s)?
    # Seems very convoluted - even this below is less convoluted then using which() ind_1, ind_2, ind_X
    addresses <- addresses %>%
      mutate(ADD3 = case_when(
        (ADDRESS_1 == "" | is.na(ADDRESS_1)) &
          (ADDRESS_2 != "" | !is.na(ADDRESS_2)) &
          (ADDRESS_3 != "" | !is.na(ADDRESS_3)) ~ paste(ADDRESS_2, ADDRESS_3, POSTCODE, sep = ", "),
        # 2
        (ADDRESS_1 != "" | !is.na(ADDRESS_1)) &
          (ADDRESS_2 == "" | is.na(ADDRESS_2)) &
          (ADDRESS_3 != "" | !is.na(ADDRESS_3)) ~ paste(ADDRESS_1, ADDRESS_3, POSTCODE, sep = ", "),
        # 3
        (ADDRESS_1 != "" | !is.na(ADDRESS_1)) &
          (ADDRESS_2 != "" | !is.na(ADDRESS_2)) &
          (ADDRESS_3 == "" | is.na(ADDRESS_3)) ~ paste(ADDRESS_1, ADDRESS_2, POSTCODE, sep = ", "),
        # 4
        (ADDRESS_1 == "" | is.na(ADDRESS_1)) &
          (ADDRESS_2 == "" | is.na(ADDRESS_2)) &
          (ADDRESS_3 != "" | !is.na(ADDRESS_3)) ~ paste(ADDRESS_3, POSTCODE, sep = ", "),
        # 5
        (ADDRESS_1 == "" | is.na(ADDRESS_1)) &
          (ADDRESS_2 != "" | !is.na(ADDRESS_2)) &
          (ADDRESS_3 == "" | is.na(ADDRESS_3)) ~ paste(ADDRESS_2, POSTCODE, sep = ", "),
        # 6
        (ADDRESS_1 != "" | !is.na(ADDRESS_1)) &
          (ADDRESS_2 == "" | is.na(ADDRESS_2)) &
          (ADDRESS_3 == "" | is.na(ADDRESS_3)) ~ paste(ADDRESS_1, POSTCODE, sep = ", "),
        # 7
        (ADDRESS_1 == "" | is.na(ADDRESS_1)) &
          (ADDRESS_2 == "" | is.na(ADDRESS_2)) &
          (ADDRESS_3 == "" | is.na(ADDRESS_3)) ~ "",
        # 8
        # TODO why is this in ADD3? Surely the closest match (first pattern) should be if all address parts as is match
        (ADDRESS_1 != "" | !is.na(ADDRESS_1)) &
          (ADDRESS_2 != "" | !is.na(ADDRESS_2)) &
          (ADDRESS_3 != "" | !is.na(ADDRESS_3)) ~ paste(ADDRESS_1, ADDRESS_2, ADDRESS_3, POSTCODE, sep = ", "),
        TRUE ~ "ERROR"
      ))

    #Format 4:
    addresses$ADD4 = paste(addresses$ADDRESS_1, addresses$ADDRESS_2, addresses$POSTCODE, sep=", ")

    #Format 5:
    addresses$ADD5 = paste(addresses$ADDRESS_1, ", ", addresses$ADDRESS_2, ", ", addresses$ADDRESS_3, ", ", addresses$POSTCODE, sep="")

    #Format 6:
    addresses$ADD6 = paste(addresses$ADDRESS_2, ", ", addresses$ADDRESS_3, ", ", addresses$POSTCODE, sep="")

    #Format 7:
    addresses$ADD7 = paste(addresses$ADDRESS_1, " ", addresses$ADDRESS_2, ", ", addresses$POSTCODE, sep="")

    #Format 8:
    addresses <- addresses %>%
      mutate(ADD8 = case_when(
        # Look for starting with a digit (+ means it can be any size number) followed by zero or more letters, and then a space
        # substring() then extracts that string before a space and is concatenated with ADDRESS_2 and the postcode
        grepl("^\\d+[a-zA-Z]*\\s", ADDRESS_1) ~ paste0(substring(ADDRESS_1, 1, regexpr(" ", ADDRESS_1) - 1), ", ", ADDRESS_2, ", ", POSTCODE),
        TRUE ~ ""
      ))

    #Save processed addresses object
    print(paste0("Saving processed address data for batch ", b))
    saveRDS(addresses, file = (paste0(path, "/RData/address_", b,"_processed.rds")))

    # #AB
    print("Starting reformatting for AddressBase data")
    # General high level cleaning
    # Remove apostrophes from all columns
    master[] <- lapply(master, function(x) gsub("'", "", x))
    # Replace all cells with only "" and " " with an NA using replace()
    # . in replace() is used to refer to the data being assigned by across(everything())
    master <- master %>%
      mutate(across(everything(), ~replace(., . %in% c("", " "), NA_character_)))

    # #PREFIX_NUM
    ind_1 = which(is.na(master$PAO_START_NUMBER) == TRUE & is.na(master$BUILDING_NUMBER) == TRUE & is.na(master$SAO_START_NUMBER) == TRUE)
    ind_2 = which(is.na(master$PAO_START_NUMBER) == TRUE & is.na(master$BUILDING_NUMBER) == TRUE & is.na(master$SAO_START_NUMBER) == FALSE)
    ind_3 = which(is.na(master$PAO_START_NUMBER) == TRUE & is.na(master$BUILDING_NUMBER) == FALSE)

    master$PREFIX_NUM = master$PAO_START_NUMBER
    master[ind_1, c("PREFIX_NUM")] = NA_character_
    master[ind_2, c("PREFIX_NUM")] = master[ind_2, c("SAO_START_NUMBER")]
    master[ind_3, c("PREFIX_NUM")] = master[ind_3, c("BUILDING_NUMBER")]

    #PREFIX_NAME
    ind_1 = which(is.na(master$SAO_TEXT) == TRUE & is.na(master$BUILDING_NAME) == TRUE)
    ind_2 = which(is.na(master$SAO_TEXT) == TRUE & is.na(master$BUILDING_NAME) == FALSE)
    ind_3 = which(is.na(master$SAO_TEXT) == FALSE & is.na(master$BUILDING_NAME) == TRUE)

    master$PREFIX_NAME = paste(master$SAO_TEXT, master$BUILDING_NAME, sep=" ")
    master[ind_1, c("PREFIX_NAME")] = NA_character_
    master[ind_2, c("PREFIX_NAME")] = master[ind_2, c("BUILDING_NAME")]
    master[ind_3, c("PREFIX_NAME")] = master[ind_3, c("SAO_TEXT")]

    ind_1 = which(is.na(master$SAO_TEXT) == TRUE & is.na(master$BUILDING_NAME) == TRUE)
    ind_2 = which(is.na(master$SAO_TEXT) == TRUE & is.na(master$BUILDING_NAME) == FALSE)
    ind_3 = which(is.na(master$SAO_TEXT) == FALSE & is.na(master$BUILDING_NAME) == TRUE)

    master$PREFIX_NAME = paste(master$SAO_TEXT, master$BUILDING_NAME, sep=" ")
    master[ind_1, c("PREFIX_NAME")] = NA_character_
    master[ind_2, c("PREFIX_NAME")] = master[ind_2, c("BUILDING_NAME")]
    master[ind_3, c("PREFIX_NAME")] = master[ind_3, c("SAO_TEXT")]

    # If Prefix name is available in the absence of any other data then use that
    # Some cases where a property should be 10A but the "A" is only in the PAO_START_SUFFIX
    # So this also leads to the property just being 10 X Street which means any struggling matches may get matched to the 10 X Street
    # Despite being Ground floor Flat 10 South Fort Street, Edinburgh, EH6 4DN or First Floor Flat 10 South Fort Street, Edinburgh, EH6 4DN or
    master <- master %>% mutate(
      PREFIX_NAME = case_when(
        is.na(PREFIX_NAME) & !is.na(PAO_START_SUFFIX) ~ PAO_START_SUFFIX,
        TRUE ~ PREFIX_NAME
      )
    )

    #FINAL_PREFIX
    # FIXME Doesn't account for where they are the same or if a number is present but not a name
    # Or just completely ignores the number if the name is bigger
    # e.g. if Flat 33 in Name and 29 in Num then it ignores the Num in the final prefix
    master <- master %>%
      # mutate(across(where(is.character), str_squish)) %>%
      # # Is the number prefix the same as the start of the name prefix
      # # If it is like 23 and 23a then we can just take the name prefix later
      # TODO more thought needed as some 1/1 or 1F1 patterns come up in the PREFIX_NAME and if it's 1 1/1 then you could end up only choosing one over the other
      # Catches 7A, 7 ABERCROMBY PLACE, EDINBURGH, EH36LA in pattern 19 now but ideally it would be a lot sooner...
      # mutate(PREFIX_CHECK = case_when(
      #   mutate(extracted_numbers = str_extract(Name, "^\\d+"))
      # )) %>%
      mutate(FINAL_PREFIX = case_when(
        is.na(PREFIX_NUM) & !is.na(PREFIX_NAME) ~ PREFIX_NAME,
        !is.na(PREFIX_NUM) & is.na(PREFIX_NAME) ~ PREFIX_NUM,
        # If PREFIX_NUM is just a number (eg. 26) and PREFIX_NAME is only a letter (eg. B) then smush together to form 26b
        str_detect(PREFIX_NUM, "^\\d+$") & str_length(PREFIX_NAME) == 1 & str_detect(PREFIX_NAME, "^[A-Z]$") ~ paste0(PREFIX_NUM, PREFIX_NAME),
        #7 and 7a example so in cases where the PREFIX_NAME doesnt start with the PREFIX_NUM
        #then take PREFIX_NAME only. So like PREFIX_NAME is 7a
        #Prefix NUM is getting a 7 and then PREFIX_NAME is getting 7 and a for "7a" so being doubled counted potentially
        # and combining to make "7, 7a"
        str_detect(PREFIX_NAME, "\\d[A-Z]$") &
          str_sub(PREFIX_NAME, 1, nchar(PREFIX_NAME) - 1) == PREFIX_NUM ~ PREFIX_NAME,
        !is.na(PREFIX_NUM) & !is.na(PREFIX_NAME) ~ paste0(PREFIX_NAME, " ", PREFIX_NUM),
        TRUE ~ NA_character_
      ))

    # ind_1 = which(nchar(master$PREFIX_NUM,"chars") > nchar(master$PREFIX_NAME,"chars"))
    # ind_2 = which(nchar(master$PREFIX_NUM,"chars") < nchar(master$PREFIX_NAME,"chars"))
    #
    # master$FINAL_PREFIX = ""
    # master[ind_1, c("FINAL_PREFIX")] = master[ind_1, c("PREFIX_NUM")]
    # master[ind_2, c("FINAL_PREFIX")] = master[ind_2, c("PREFIX_NAME")]

    #FINAL_STREET_NAME
    # TODO figure out what is going on here - is it to replace street name with an alternative or is it to ignore street name in certain cases
    # NA is different to "" so its not picking up cases where PAO_TEXT for example is "" so changing the Street name to ""
    ind_1 = which(is.na(master$PAO_TEXT) == FALSE & is.na(master$BUILDING_NAME) == TRUE)
    #ind_2 = which(is.na(master$PAO_TEXT) == TRUE & is.na(master$BUILDING_NAME) == FALSE)
    ind_3 = which(is.na(master$PAO_TEXT) == FALSE & is.na(master$BUILDING_NAME) == FALSE)

    master$FINAL_STREET_NAME = master$STREET_DESCRIPTION
    master[ind_1, c("FINAL_STREET_NAME")] = master[ind_1, c("PAO_TEXT")]
    #master[ind_2, c("FINAL_STREET_NAME")] = master[ind_2, c("BUILDING_NAME")]

    # If there is PAO Text and Building name then effectively take street name out
    # BUILDING_NAME is added through prefix name so should be prefix, town, postcode
    # PAO is added as alternative street name later in a later pattern
    # TODO is this risky? What chance is there of overlaps?
    master[ind_3, c("FINAL_STREET_NAME")] = NA_character_

    #Format 1:
    master$AB_ADDRESS_1 = ifelse(!is.na(master$FINAL_STREET_NAME),
                                 paste(master$FINAL_PREFIX, " ", master$FINAL_STREET_NAME, ", ", master$TOWN_NAME, ", ",master$AB_POSTCODECOMPACT, sep=""),
                                 paste(master$FINAL_PREFIX, master$FINAL_STREET_NAME, ", ", master$TOWN_NAME, ", ",master$AB_POSTCODECOMPACT, sep=""))

    #Format 1a:
    # Take the flat number/letter and put that after the number.
    #So like Flat G 26 James Street to 26G James Street
    master <- master %>%
      mutate(letter = ifelse(str_detect(PREFIX_NAME, "^FLAT [A-Za-z]"),
                             str_extract(PREFIX_NAME, "\\b([A-Za-z])\\b"),
                             NA_character_)) %>%
      mutate(AB_ADDRESS_1a = paste0(PREFIX_NUM, letter, " ", FINAL_STREET_NAME, ", ", TOWN_NAME, ", ", AB_POSTCODECOMPACT))

    #Format 2:
    master$AB_ADDRESS_2 = ifelse(!is.na(master$FINAL_STREET_NAME),
                                 paste(master$FINAL_PREFIX, " ", master$FINAL_STREET_NAME, ", ", master$AB_POSTCODECOMPACT, sep=""),
                                 paste(master$FINAL_PREFIX, master$FINAL_STREET_NAME, ", ",master$AB_POSTCODECOMPACT, sep=""))
    #Format 2a:
    # Take the flat number/letter and put that after the number.
    #So like Flat G 26 James Street to 26G James Street
    master <- master %>%
      mutate(AB_ADDRESS_2a = paste0(PREFIX_NUM, letter, " ", FINAL_STREET_NAME, ", ", AB_POSTCODECOMPACT)) %>%
      select(-(letter))

    #Format 3:
    # ind_1 = which((master$PREFIX_NAME == "" | is.na(master$PREFIX_NAME) == TRUE) & (master$PREFIX_NUM == "" | is.na(master$PREFIX_NUM) == TRUE))
    # ind_2 = which((master$PREFIX_NAME != "" | is.na(master$PREFIX_NAME) == FALSE) & (master$PREFIX_NUM == ""| is.na(master$PREFIX_NUM) == TRUE))
    # ind_3 = which((master$PREFIX_NAME == "" | is.na(master$PREFIX_NAME) == TRUE) & (master$PREFIX_NUM != ""| is.na(master$PREFIX_NUM) == FALSE))
    # ind_4 = which((master$PREFIX_NAME != "" | is.na(master$PREFIX_NAME) == FALSE) & (master$PREFIX_NUM != ""| is.na(master$PREFIX_NUM) == FALSE))

    ind_1 = which(is.na(master$PREFIX_NAME) & is.na(master$PREFIX_NUM))
    ind_2 = which(!is.na(master$PREFIX_NAME) & is.na(master$PREFIX_NUM))
    ind_3 = which(is.na(master$PREFIX_NAME) & !is.na(master$PREFIX_NUM))
    ind_4 = which(!is.na(master$PREFIX_NAME) & !is.na(master$PREFIX_NUM))
    length(ind_1)+length(ind_2)+length(ind_3)+length(ind_4)

    master$AB_ADDRESS_3 = ""
    master[ind_2, c("AB_ADDRESS_3")] = paste(master[ind_2, c("PREFIX_NAME")],master[ind_2, c("FINAL_STREET_NAME")], master[ind_2, c("TOWN_NAME")],
                                             master[ind_2, c("AB_POSTCODECOMPACT")], sep=", ")
    master[ind_3, c("AB_ADDRESS_3")] = paste(master[ind_3, c("PREFIX_NUM")], " ", master[ind_3, c("FINAL_STREET_NAME")], ", ",
                                             master[ind_3, c("TOWN_NAME")], ", ", master[ind_3, c("AB_POSTCODECOMPACT")], sep="")
    master[ind_4, c("AB_ADDRESS_3")] = paste(master[ind_4, c("PREFIX_NAME")], ", ", master[ind_4, c("PREFIX_NUM")]," ", master[ind_4, c("FINAL_STREET_NAME")], ", ",
                                             master[ind_4, c("TOWN_NAME")], ", ", master[ind_4, c("AB_POSTCODECOMPACT")], sep="")

    #Format 4:
    master$AB_ADDRESS_4 = paste(master$SAO_TEXT, ", ", master$PAO_START_NUMBER, " ",  master$STREET_DESCRIPTION, ", ",
                                master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 5:
    master$AB_ADDRESS_5 = paste(master$PAO_TEXT, ", ", master$BUILDING_NUMBER, " ",  master$STREET_DESCRIPTION, ", ",
                                master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 6:
    #master$AB_ADDRESS_6 = paste(master$SAO_START_NUMBER, ", ", master$BUILDING_NAME, " ",  master$STREET_DESCRIPTION, ", ",
    #                            master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    master$AB_ADDRESS_6 = paste(master$SUB_BUILDING_NAME, " ", master$PAO_TEXT, ", ",  master$STREET_DESCRIPTION, ", ",
                                master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 7:
    # This is very risky because it then just takes only the number (e.g. 7) rather than the full number/letter (e.g. 7a) which is in other fields
    master$AB_ADDRESS_7 = paste(master$PAO_START_NUMBER, " ", master$STREET_DESCRIPTION, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 8:
    master$AB_ADDRESS_8 = paste(master$SAO_START_NUMBER, " ", master$PAO_TEXT, ", ", master$STREET_DESCRIPTION, ", ", master$TOWN_NAME, ", ",
                                master$AB_POSTCODECOMPACT, sep="")

    #Format 9:
    master$AB_ADDRESS_9 = paste(master$SAO_TEXT, " ", master$PAO_TEXT, ", ", master$PAO_START_NUMBER, " ", master$STREET_DESCRIPTION, ", ",
                                master$AB_POSTCODECOMPACT, sep="")

    #Format 10:
    master$AB_ADDRESS_10 = paste(master$SAO_START_NUMBER, " ", master$SAO_TEXT, " ", master$PAO_TEXT, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 11:
    master$AB_ADDRESS_11 = paste(master$PAO_TEXT, ", ", master$BUILDING_NUMBER, ", ", master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 12:
    ind_1 = which(is.na(master$PREFIX_NAME) & is.na(master$PREFIX_NUM))
    ind_2 = which(!is.na(master$PREFIX_NAME) & is.na(master$PREFIX_NUM))
    ind_3 = which(is.na(master$PREFIX_NAME) & !is.na(master$PREFIX_NUM))
    ind_4 = which(!is.na(master$PREFIX_NAME) & !is.na(master$PREFIX_NUM))

    master$AB_ADDRESS_12 = ""
    master[ind_2, c("AB_ADDRESS_12")] = paste(master[ind_2, c("PREFIX_NAME")],master[ind_2, c("FINAL_STREET_NAME")], ", ",
                                              master[ind_2, c("AB_POSTCODECOMPACT")], sep=", ")
    master[ind_3, c("AB_ADDRESS_12")] = paste(master[ind_3, c("PREFIX_NUM")], " ", master[ind_3, c("FINAL_STREET_NAME")], ", ",
                                              master[ind_3, c("AB_POSTCODECOMPACT")], sep="")
    master[ind_4, c("AB_ADDRESS_12")] = paste(master[ind_4, c("PREFIX_NAME")], ", ", master[ind_4, c("PREFIX_NUM")]," ", master[ind_4, c("FINAL_STREET_NAME")], ", ",
                                              master[ind_4, c("AB_POSTCODECOMPACT")], sep="")

    #Format 13:
    master$AB_ADDRESS_13 = paste(master$PAO_TEXT, ", ", master$STREET_DESCRIPTION, ", ", master$TOWN_NAME, ", ",
                                 master$AB_POSTCODECOMPACT, sep="")

    #Format 14:
    master$AB_ADDRESS_14 = paste(master$PAO_TEXT, ", ", master$SAO_START_NUMBER, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 15:
    master$AB_ADDRESS_15 = paste(master$PAO_TEXT, ", ", master$PAO_START_NUMBER, " ", master$STREET_DESCRIPTION,
                                 ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 16:
    master$AB_ADDRESS_16 = paste(master$PAO_TEXT, ", ", master$BUILDING_NAME, " ", master$STREET_DESCRIPTION,
                                 ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 17:
    master$AB_ADDRESS_17 = paste(master$PAO_TEXT, ", ", master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 18:
    master$AB_ADDRESS_18 = paste(master$SAO_TEXT, ", ", master$PAO_TEXT, ", ", master$TOWN_NAME,
                                 ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 19:
    master$AB_ADDRESS_19 = paste(master$PAO_START_NUMBER, "", master$PAO_START_SUFFIX, " ", master$STREET_DESCRIPTION,
                                 ", ", master$TOWN_NAME, ", ",master$AB_POSTCODECOMPACT, sep="")

    #Format 19a:
    # Helps grab the ones that are like 12A Main Street, AB53 5XD
    # But arent currently matching because the Town Name is wrong and then it has nothing else to help match on because the
    # master$PAO_START_NUMBER, "", master$PAO_START_SUFFIX doesnt come back in
    master$AB_ADDRESS_19a = paste(master$PAO_START_NUMBER, "", master$PAO_START_SUFFIX, " ", master$STREET_DESCRIPTION,
                                  ", ",master$AB_POSTCODECOMPACT, sep="")

    #Format 20:
    # add space so that it's not like "1F26 Stoneyhill Road" and instead its "1F 26 Stoneyhill Road"
    master$AB_ADDRESS_20 = paste(master$PAO_TEXT, " ", master$PAO_START_NUMBER, " ", master$STREET_DESCRIPTION,
                                 ", ",master$AB_POSTCODECOMPACT, sep="")

    #Format 21:
    master$AB_ADDRESS_21 = paste(master$FINAL_STREET_NAME, ", ", master$TOWN_NAME, ", ",master$AB_POSTCODECOMPACT, sep="")

    # Some addresses being matched to "1, Dumfries, DG20HD" which is very risky as it assumes that postcode in the town of Dumfries only had one number 1
    # PAO Text is being used above but only with things like street name and building name
    # Use prefix final?
    #Format 22a:
    # Needs to take a number from PAO_START_NUMBER or SAO_START_NUMBER - needs figuring out how these interact with building number as well
    master$AB_ADDRESS_22a = paste(master$FINAL_PREFIX, " ", master$PAO_TEXT, ", ", master$TOWN_NAME, ", ",master$AB_POSTCODECOMPACT, sep="")

    #Format 22b:
    master$AB_ADDRESS_22b = paste(master$FINAL_PREFIX, " ", master$PAO_TEXT, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 22c:
    master$AB_ADDRESS_22c = paste(master$FINAL_PREFIX, " ", master$STREET_DESCRIPTION, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 22c:
    master$AB_ADDRESS_22c = paste(master$FINAL_PREFIX, " ", master$STREET_DESCRIPTION, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 23:
    master$AB_ADDRESS_23 = paste(master$BUILDING_NUMBER, ", ", master$TOWN_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    #Format 23a:
    # TODO too risky but could help with some cases (e.g. FIRST FLOOR 6A THISTLE STREET AB421TD) - has so many bits of data in every master AB column
    # IDEA build logic to check how many master matches there are for the test datasets
    # If the pattern match is , Musselburgh, EH21 6TW that kinda sucks, but if there's only one of those in master then it's a better match
    # If then there are multiple 23, , EH21 6TW because it's only grabbed elements of an address and these are overlapping with other then it rejects the match as it's not 1 to 1
    # An example would be if it just grabs the building number town and postcode but multiple flats are in there
    # Another would be 7A ABERCROMBY PLACE, EDINBURGH, EH36LA - using the older logic it was grabbing only the number "7" which meant there were
    # multiple cases of 7 ABERCROMBY PLACE, EDINBURGH, EH36LA in one pattern of the master dataset meaning both 7A and 7 got linked to the first instance of a match
    # Leading to both getting the same UPRN - this could be happening a fair amount when one element is dropped somewhere in "master" leading to homogonisation of an address despite differences
    #master$AB_ADDRESS_23a = paste(master$SUB_BUILDING_NAME, " ", master$BUILDING_NAME, " ", master$FINAL_STREET_NAME, ", ", master$AB_POSTCODECOMPACT, sep="")

    master <- master %>%
      select(starts_with("AB_ADDRESS_"), UPRN, UDPRN, AB_POSTCODECOMPACT) %>%
      mutate(across(where(is.character), str_squish))

    #Save object
    print(paste0("Saving processed master data for batch ", b))
    saveRDS(master, file=(paste0(path, "/RData/master_", b,"_processed.rds")))
    rm(addresses, master)
    gc()

    #Print results
    print(paste("File", b, "completed"))
  }

  #Clear workspace
  rm(list=setdiff(ls(), "path"))
  gc()

  ######### Address matching steps #########
  # TODO clean this step up a lot...
  # TODO explore the use of Jaccard/Levenshtein/Damerau-Levenshtein distances
  print("Step 3: Address matching begins")

  for (b in 1:4){

    #Load objects
    test <- readRDS(paste0(path, "/RData/address_", b, "_processed.rds"))
    # convert from data table as this is less stable for code below
    test <- as.data.frame(test)
    master <- readRDS(paste0(path, "/RData/master_", b, "_processed.rds"))

    print("Objects loaded. Prepping for address matching...")

    #Order and group test set by postcode
    test = test[order(test$POSTCODE),]
    test = transform(test, PC_GROUP=as.numeric(factor(POSTCODE)))

    #Set number of postcodes per batch
    batch = 5000

    #Calculate number of postcodes groups required
    postcodes = test[!duplicated(test$POSTCODE),c("ADD1","POSTCODE")]
    pc_group = ceiling(length(postcodes[,1])/batch)

    #Assign each address to a postcode group
    test$PC_GROUP = ceiling(test$PC_GROUP/batch)
    table(test$PC_GROUP)

    #Remove unnecessary objects to conserve memory
    rm(postcodes)

    #Begin address matching on reduced master lookup
    print(date())

    # Start the clock!
    ptm = proc.time()

    #Set current record = 0
    current_rec = 0
    matches = 0
    records = 0
    print("Beginning address matching loop...")

    for (j in 1: pc_group){

      #Reduce test and master datasets based on pc group (batch)
      test_temp = test[which(test$PC_GROUP == j),]
      pc_temp = test_temp[!duplicated(test_temp$POSTCODE),c("ADDRESS_3","POSTCODE")]
      master_pc = merge(master, pc_temp, by.x = "AB_POSTCODECOMPACT", by.y = "POSTCODE", all.x = FALSE)

      #Create final object to merge results to at the end
      final = test_temp

      print(paste("Batch", j,": Matching", length(test_temp$ROW_NUM),"raw records using master lookup of",length(master_pc[,1]),"records"),sep="")

      #Identify position of matching records

      #Format 0
      #Create match index
      match_pos = match(test_temp$ADD0, master_pc$AB_ADDRESS_1)
      #Reduce master to relevant fields
      output = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_1")]
      #Remove non-matches
      output = output[which(is.na(output$UPRN)==FALSE),]
      if(length(output[,1])>0){
        #Remove any duplicate UPRNs in matches
        output = output[!duplicated(output$UPRN),]
        #Add Match code
        output$MATCH = "0"
        output = merge(output, test_temp[c("ADD0","ROW_NUM")], by.x="AB_ADDRESS_1", by.y="ADD0", all.x=FALSE)
        output = output[c(5,2:3,1,4)]
        colnames(output)[4] = "AB_ADDRESS_FINAL"
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, output[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD0", by.y=3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 1
      #Create match index
      match_pos = match(test_temp$ADD1, master_pc$AB_ADDRESS_1)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_1")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "1"
        temp = merge(temp, test_temp[c("ADD1","ROW_NUM")], by.x=3, by.y="ADD1", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD1", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 1A
      #Create match index
      match_pos = match(test_temp$ADD1, master_pc$AB_ADDRESS_1a)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_1a")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "1a"
        temp = merge(temp, test_temp[c("ADD1","ROW_NUM")], by.x=3, by.y="ADD1", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD1", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 2
      #Create match index
      match_pos = match(test_temp$ADD2, master_pc$AB_ADDRESS_2)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_2")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "2"
        temp = merge(temp, test_temp[c("ADD2","ROW_NUM")], by.x=3, by.y="ADD2", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD2", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 2a
      #Create match index
      match_pos = match(test_temp$ADD2, master_pc$AB_ADDRESS_2a)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_2a")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "2a"
        temp = merge(temp, test_temp[c("ADD2","ROW_NUM")], by.x=3, by.y="ADD2", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD2", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 3
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_3)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_3")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "3"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 4
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_4)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_4")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "4"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 5
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_5)
      #Reduce master to relevant fields
      temp = master_pc[match_pos,c("UPRN","UDPRN","AB_ADDRESS_5")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "5"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 6
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_6)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_6")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "6"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 7
      #Create match index
      match_pos = match(test_temp$ADD2, master_pc$AB_ADDRESS_7)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_7")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "7"
        temp = merge(temp, test_temp[c("ADD2","ROW_NUM")], by.x=3, by.y="ADD2", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD2", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 8
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_8)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_8")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "8"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 9
      #Create match index
      match_pos = match(test_temp$ADD4, master_pc$AB_ADDRESS_9)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_9")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "9"
        temp = merge(temp, test_temp[c("ADD4","ROW_NUM")], by.x=3, by.y="ADD4", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD4", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 10
      #Create match index
      match_pos = match(test_temp$ADD2, master_pc$AB_ADDRESS_10)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_10")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "10"
        temp = merge(temp, test_temp[c("ADD2","ROW_NUM")], by.x=3, by.y="ADD2", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD2", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 11
      #Create match index
      match_pos = match(test_temp$ADD5, master_pc$AB_ADDRESS_11)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_11")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "11"
        temp = merge(temp, test_temp[c("ADD5","ROW_NUM")], by.x=3, by.y="ADD5", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD5", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 12
      #Create match index
      match_pos = match(test_temp$ADD4, master_pc$AB_ADDRESS_12)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_12")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "12"
        temp = merge(temp, test_temp[c("ADD4","ROW_NUM")], by.x=3, by.y="ADD4", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD4", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 13
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_13)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_13")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "13"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 14
      #Create match index
      match_pos = match(test_temp$ADD4, master_pc$AB_ADDRESS_14)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_14")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "14"
        temp = merge(temp, test_temp[c("ADD4","ROW_NUM")], by.x=3, by.y="ADD4", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD4", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 15
      #Create match index
      match_pos = match(test_temp$ADD4, master_pc$AB_ADDRESS_15)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_15")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "15"
        temp = merge(temp, test_temp[c("ADD4","ROW_NUM")], by.x=3, by.y="ADD4", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD4", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 16
      #Create match index
      match_pos = match(test_temp$ADD4, master_pc$AB_ADDRESS_16)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_16")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "16"
        temp = merge(temp, test_temp[c("ADD4","ROW_NUM")], by.x=3, by.y="ADD4", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD4", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 17
      #Create match index
      match_pos = match(test_temp$ADD1, master_pc$AB_ADDRESS_17)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_17")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "17"
        temp = merge(temp, test_temp[c("ADD1","ROW_NUM")], by.x=3, by.y="ADD1", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD1", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 18
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_18)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_18")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "18"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 19
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_19)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_19")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "19"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 19a
      #Create match index
      match_pos = match(test_temp$ADD7, master_pc$AB_ADDRESS_19a)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_19a")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "19a"
        temp = merge(temp, test_temp[c("ADD7","ROW_NUM")], by.x=3, by.y="ADD7", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD7", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 20
      #Create match index
      match_pos = match(test_temp$ADD4, master_pc$AB_ADDRESS_20)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_20")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "20"
        temp = merge(temp, test_temp[c("ADD4","ROW_NUM")], by.x=3, by.y="ADD4", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD4", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 21
      #Create match index
      match_pos = match(test_temp$ADD6, master_pc$AB_ADDRESS_1)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_1")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "21"
        temp = merge(temp, test_temp[c("ADD6","ROW_NUM")], by.x=3, by.y="ADD6", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD6", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 22
      #Create match index
      match_pos = match(test_temp$ADD3, master_pc$AB_ADDRESS_21)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_21")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "22"
        temp = merge(temp, test_temp[c("ADD3","ROW_NUM")], by.x=3, by.y="ADD3", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD3", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 22a
      #Create match index
      match_pos = match(test_temp$ADD1, master_pc$AB_ADDRESS_22a)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_22a")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "22a"
        temp = merge(temp, test_temp[c("ADD1","ROW_NUM")], by.x=3, by.y="ADD1", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD1", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 22a
      #Create match index
      match_pos = match(test_temp$ADD2, master_pc$AB_ADDRESS_22b)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_22b")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "22b"
        temp = merge(temp, test_temp[c("ADD2","ROW_NUM")], by.x=3, by.y="ADD2", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD2", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 22c
      #Create match index
      match_pos = match(test_temp$ADD7, master_pc$AB_ADDRESS_22c)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_22c")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "22c"
        temp = merge(temp, test_temp[c("ADD7","ROW_NUM")], by.x=3, by.y="ADD7", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD7", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #Format 23
      #Create match index
      match_pos = match(test_temp$ADD7, master_pc$AB_ADDRESS_23)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_23")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "23"
        temp = merge(temp, test_temp[c("ADD7","ROW_NUM")], by.x=3, by.y="ADD7", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD7", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      # #Format 24
      # #Create match index
      # match_pos = match(test_temp$ADD7, master_pc$AB_ADDRESS_23a)
      # #Reduce master to relevant fields
      # temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_23a")]
      # #Remove non-matches
      # temp = temp[which(is.na(temp$UPRN)==FALSE),]
      # if(length(temp[,1])>0){
      #   #Remove any duplicate UPRNs in matches
      #   temp = temp[!duplicated(temp$UPRN),]
      #   #Add Match code
      #   temp$MATCH = "24"
      #   temp = merge(temp, test_temp[c("ADD7","ROW_NUM")], by.x=3, by.y="ADD7", all.x=FALSE)
      #   temp = temp[c(5,2:3,1,4)]
      #   colnames(temp)[4] = "AB_ADDRESS_FINAL"
      #   output = rbind(output, temp)
      #   #Merge to test_temp object by the address formats that created the match
      #   test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD7", by.y= 3, all.x=TRUE)
      #   #Remove matched records from test_temp object, so they won't be included in the next iteration
      #   test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
      #   test_temp = test_temp[c(1:14)]
      # }

      #Format 25
      #Create match index
      match_pos = match(test_temp$ADD8, master_pc$AB_ADDRESS_23)
      #Reduce master to relevant fields
      temp = master_pc[match_pos, c("UPRN","UDPRN","AB_ADDRESS_23")]
      #Remove non-matches
      temp = temp[which(is.na(temp$UPRN)==FALSE),]
      if(length(temp[,1])>0){
        #Remove any duplicate UPRNs in matches
        temp = temp[!duplicated(temp$UPRN),]
        #Add Match code
        temp$MATCH = "25"
        temp = merge(temp, test_temp[c("ADD8","ROW_NUM")], by.x=3, by.y="ADD8", all.x=FALSE)
        temp = temp[c(5,2:3,1,4)]
        colnames(temp)[4] = "AB_ADDRESS_FINAL"
        output = rbind(output, temp)
        #Merge to test_temp object by the address formats that created the match
        test_temp = merge(test_temp, temp[c("UPRN","UDPRN","AB_ADDRESS_FINAL","MATCH")], by.x="ADD8", by.y= 3, all.x=TRUE)
        #Remove matched records from test_temp object, so they won't be included in the next iteration
        test_temp = test_temp[which(is.na(test_temp$UPRN)==TRUE),]
        test_temp = test_temp[c(1:14)]
      }

      #print(signif(prop.table(table(output$MATCH))),digits=2)
      print(table(output$MATCH))
      final = merge(final, output, by = "ROW_NUM", all.x=TRUE)
      ind_nomatch = which(is.na(final$MATCH)==TRUE)
      final[ind_nomatch, c("MATCH")] = "None"

      #Calculate stats
      batch_rec = length(final[,1])
      current_rec = current_rec + batch_rec
      matches = matches + length(final[-ind_nomatch,1])
      records = records + length(final[,1])
      match_rate = matches/records

      #Print progress
      progress = round((((current_rec/length(test[,1]))*100)),digits=1)
      cat(paste("Batch ",j,": ",current_rec," total records processed (",progress,"%)", sep=""),
          paste("Cumulative match rate: ", round(match_rate*100, digits=1), "%", sep=""), sep="\n")

      #Save output file when batch is complete and then remove and clear memory
      saveRDS(final, paste0(path, "RData/Output", b, "_", j, ".rds"))
      gc()
    }

    #Move to next batch of addresses
    print(paste("Batch ", b," complete. Preparing Batch ",(b+1),"...",sep=""))

    #Print time of batch processing
    print(proc.time() - ptm)

  }

  #Clear workspace
  rm(list=ls())
  gc()

  #Print date
  print(date())

  pc_group = 4

  print("Final formatting of results.")

  # Get a list of files in the folder
  output_files <- list.files(path = paste0(path, "RData/"), pattern = paste0("^Output\\d+_\\d+", ".rds"), full.names = TRUE)

  # Check if there are any files that match the pattern
  if (length(output_files) == 0) {
    cat("No files found with the 'OutputX_X' pattern.")
  } else {
    # Read and combine the files
    output_temp <- bind_rows(lapply(output_files, readRDS))
  }

  records = length(output_temp[,1])

  if (length(output_temp[,1]) == records){

    output = output_temp
    saveRDS(output, file=paste0(path, "/RData/Output_Final.rds"))
    rm(output_temp)

    no_match = output[which(output$MATCH=="None"),]
    saveRDS(no_match, file=paste0(path, "/RData/No_Match1.rds"))

    #Print summary of results
    print("Address matching Complete")

    # Table More streamlined now and less need for manually adding new levels and blocks for matches =

    # Create a simple table with counts
    # Need to sort the levels alphabetically first to make it easier to read
    # Mixedorder() to sort numbers within strings (e.g. 1a comes before 10 but not before 1)
    mixedstring = function(x) order(gtools::mixedorder(x))

    mixed_sorted <- output
    mixed_sorted <- mixed_sorted %>% arrange(mixedstring(MATCH))

    # Order the levels based on the original order in the data
    mixed_sorted$MATCH <- factor(mixed_sorted$MATCH, levels = unique(mixed_sorted$MATCH))

    counts_table <- table(mixed_sorted$MATCH)

    # Convert the table to a data frame and calculate proportions
    result_table <- as.data.frame(counts_table)
    result_table$Proportion <- paste0(round(result_table$Freq / sum(result_table$Freq) * 100, digits = 2), "%")

    # Rename the columns for clarity
    colnames(result_table) <- c("Match level", "Number of matches", "Proportion")

    assign("match_summary", result_table, envir = .GlobalEnv) ; rm(list = c("result_table", "counts_table", "mixed_sorted"))

    #Create and write log file
    #log = as.data.frame(cbind(log_level, log_matches, log_match_rate))
    matched <- output %>% filter(MATCH != "None")
    # use prettyNum to format numbers to have commas etc when bigger than 1,000
    print(paste0(prettyNum(length(matched$ROW_NUM), big.mark = ","), " (", round(length(matched$ROW_NUM)/length(output$ROW_NUM) * 100, 2),  "%) matched addresses"))
    print("Final address match logs:")
    print(match_summary, row.names = FALSE)
    write.table(match_summary, file=(paste0(path, "/log", ".txt")), sep = "\t", quote = FALSE, row.names=FALSE)
  }

  # Add a flag
  output <- output %>%
    mutate(ADDRESS_MATCHED = "Yes")

  print("AM final output ready")
  saveRDS(output, file = (paste0(path, "AM_output_final.rds")))
  assign('AM_output_final', output, envir = .GlobalEnv)
  #rm(list=setdiff(ls(), c("path", "AM_output_final", "match_summary")))
  gc()
}
