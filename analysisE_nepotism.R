#first we'll run script step 00 to connect to db
source("00_connecttodb.R")

library(tidyverse)
library(lubridate)
library(janitor)
library(dbplyr)
library(writexl)

#list the tables in the database
src_dbi(con)

#pull in the schedule B table from postgres db
expends_db <- tbl(con, "cycle_2020_scheduleb")

glimpse(expends_db)

#filter out only active records 
expends_db <- expends_db %>% 
  filter(active==TRUE) 



#process data on House incumbents ####

#import incumbents file with cand ids
house_incumbents_list <- read_csv("raw_data/house_incumbents_list.csv", 
                                  col_types = cols(active_through = col_character()))

house_incumbents_list <- house_incumbents_list %>% 
  clean_names()

#change candidate_id to match subsequent table
house_incumbents_list <- house_incumbents_list %>% 
  rename(cand_id = candidate_id)


#import committee file for all house candidates overall
fec_committee_list_housecands <- read_csv("raw_data/fec_committee_list_housecands.csv", 
                                          col_types = cols(CMTE_ZIP = col_character(), 
                                                           FEC_ELECTION_YR = col_character()))

fec_committee_list_housecands <- fec_committee_list_housecands %>% 
  clean_names()


#join
joined_incumbents <- left_join(house_incumbents_list, fec_committee_list_housecands) #handful of candidates have multiple active cmtes



#now we'll get the data from the big FEC database for those committees ####

#grab committee ids
incumbent_cmte_ids <- joined_incumbents %>% pull(cmte_id)




### RUN ONE OF THE FOLLOWING TWO CHUNKS ##################################################

#1)
#filter the main fec contribs table then collect to local dataframe
hinc_expends <- expends_db %>%
  filter(filer_committee_id_number %in% incumbent_cmte_ids) %>% 
  collect()

saveRDS(hinc_expends, "processed_data/hinc_expends.rds")


#2)
#load from saved version of the collected records
hinc_expends <- readRDS("processed_data/hinc_expends.rds")


###########################################################################################




#date formatting and derived columns
hinc_expends$expenditure_date <- ymd(hinc_expends$expenditure_date)
hinc_expends$expenditure_year <- year(hinc_expends$expenditure_date)
hinc_expends$expenditure_month <- month(hinc_expends$expenditure_date)
hinc_expends$expenditure_day <- day(hinc_expends$expenditure_date)


#join new expends table to incumbent data to add candidate name and details
hinc_expends <- hinc_expends %>% 
  left_join(joined_incumbents, by = c("filer_committee_id_number" = "cmte_id")) %>% 
  select(name, office_full, party, state, district, everything())

#split candidate name to extract last name as own column
hinc_expends$candname_last <- str_split(hinc_expends$name, ",", simplify = TRUE)[, 1]
hinc_expends$candname_last <- str_trim(hinc_expends$candname_last)

hinc_expends <- hinc_expends %>% 
  select(candname_last, everything())

glimpse(hinc_expends)


#uppercase the payee fields to ensure compatible joining
hinc_expends <- hinc_expends %>% 
  mutate(
    payee_organization_name = str_to_upper(str_trim(payee_organization_name)),
    payee_last_name = str_to_upper(str_trim(payee_last_name)),
    conduit_name = str_to_upper(str_trim(conduit_name))
  )


#now the matching
#look for matches of cand's last name with payee fields ####
possible_matches <- hinc_expends %>% 
  filter(
    # str_detect(payee_organization_name, candname_last) |
    str_detect(payee_last_name, candname_last) |
    str_detect(conduit_name, candname_last)
  ) 

possible_matches

#save results
write_xlsx(possible_matches, "output/possible_matches.xlsx")






################################################################################
#### NOW WE'LL DO THE SENATE ###################################################
################################################################################



#process data on SENATE incumbents ####

#import incumbents file with cand ids
senate_incumbents_list <- read_csv("raw_data/senate_cands_list.csv", 
                                  col_types = cols(active_through = col_character()))

senate_incumbents_list <- senate_incumbents_list %>% 
  clean_names()

#change candidate_id to match subsequent table
senate_incumbents_list <- senate_incumbents_list %>% 
  rename(cand_id = candidate_id)


#import committee file for all SENATE candidates overall
fec_committee_list_senatecands <- read_csv("raw_data/fec_commitee_list_senatecands.csv", col_types = cols(.default = "c"))

fec_committee_list_senatecands <- fec_committee_list_senatecands %>% 
  clean_names()

glimpse(fec_committee_list_senatecands)

#clean up candidate id column
fec_committee_list_senatecands$candidate_ids <- str_remove(fec_committee_list_senatecands$candidate_ids, "[{]")
fec_committee_list_senatecands$candidate_ids <- str_remove(fec_committee_list_senatecands$candidate_ids, "[}]")

#separate rows based on candidate ID, then filter for Senate only
temp <- fec_committee_list_senatecands %>% 
  separate_rows(candidate_ids) %>% 
  mutate(
    str_trim(candidate_ids) 
    ) %>% 
  filter((str_sub(candidate_ids, 1, 1) == "S"))

fec_committee_list_senatecands <- temp

fec_committee_list_senatecands <- fec_committee_list_senatecands %>% 
  rename(cand_id = candidate_ids)


#join
joined_senate_incumbents <- inner_join(senate_incumbents_list, fec_committee_list_senatecands, by = "cand_id") #handful of candidates have multiple active cmtes

#now we'll get the data from the big FEC database for those committees ####

#grab committee ids
incumbent_senate_cmte_ids <- joined_senate_incumbents %>% pull(committee_id)



### RUN ONE OF THE FOLLOWING TWO CHUNKS ##################################################

#1)
#filter the main fec contribs table then collect to local dataframe
sinc_expends <- expends_db %>%
  filter(filer_committee_id_number %in% incumbent_senate_cmte_ids) %>% 
  collect()

saveRDS(sinc_expends, "processed_data/sinc_expends.rds")


#2)
#load from saved version of the collected records
sinc_expends <- readRDS("processed_data/sinc_expends.rds")


###########################################################################################




#date formatting and derived columns
sinc_expends$expenditure_date <- ymd(sinc_expends$expenditure_date)
sinc_expends$expenditure_year <- year(sinc_expends$expenditure_date)
sinc_expends$expenditure_month <- month(sinc_expends$expenditure_date)
sinc_expends$expenditure_day <- day(sinc_expends$expenditure_date)


#join new expends table to incumbent data to add candidate name and details
sinc_expends <- sinc_expends %>% 
  left_join(joined_senate_incumbents, by = c("filer_committee_id_number" = "committee_id")) 

#move candidate name and party/state to start on the left
sinc_expends <- sinc_expends %>% 
  select(name = name.x, office_full, party = party.x, state = state.x, everything())

#split candidate name to extract last name as own column
sinc_expends$candname_last <- str_split(sinc_expends$name, ",", simplify = TRUE)[, 1]
sinc_expends$candname_last <- str_trim(sinc_expends$candname_last)

sinc_expends <- sinc_expends %>% 
  select(candname_last, everything())

glimpse(sinc_expends)


#uppercase the payee fields to ensure compatible joining
sinc_expends <- sinc_expends %>% 
  mutate(
    payee_organization_name = str_to_upper(str_trim(payee_organization_name)),
    payee_last_name = str_to_upper(str_trim(payee_last_name)),
    conduit_name = str_to_upper(str_trim(conduit_name))
  )


#now the matching
#look for matches of cand's last name with payee fields ####
possible_matches <- sinc_expends %>% 
  filter(
    # str_detect(payee_organization_name, candname_last) |
    str_detect(payee_last_name, candname_last) |
      str_detect(conduit_name, candname_last)
  ) 

possible_matches

#save results
write_xlsx(possible_matches, "output/possible_matches_SENATE.xlsx")


