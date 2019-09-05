#first we'll run script step 00 to connect to db
source("00_connecttodb.R")

library(tidyverse)
library(lubridate)
library(janitor)
library(dbplyr)

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

#filter the contribs table by them and then collect locally
hinc_expends <- expends_db %>%
  filter(filer_committee_id_number %in% incumbent_cmte_ids) %>% 
  collect()


#join to add candidate name to table
prez_expends <- prez_expends %>% 
  left_join(candnames, by = c("filer_committee_id_number" = "fec_committee_id")) %>% 
  select(name, everything())

#date format and derived columns
prez_expends$expenditure_date <- ymd(prez_expends$expenditure_date)
prez_expends$expenditure_year <- year(prez_expends$expenditure_date)
prez_expends$expenditure_month <- month(prez_expends$expenditure_date)
prez_expends$expenditure_day <- day(prez_expends$expenditure_date)

#filter by date 
prez_expends <- prez_expends %>% 
  filter(expenditure_year == 2019,
         expenditure_month >= 4)




prez_expends %>% 
  filter(expenditure_year == 2019) %>% 
  group_by(status) %>% 
  summarise(n(), sum(expenditure_amount))

prez_expends %>% 
  filter(expenditure_year == 2019) %>% 
  group_by(name, status) %>% 
  summarise(n(), sum(expenditure_amount))


prez_expends %>% 
  filter(expenditure_year == 2019,
         status == "ACTIVE") %>% 
  group_by(name) %>% 
  summarise(num = n(), tot_amt = sum(expenditure_amount)) %>% 
  arrange(desc(tot_amt))


# attempt to isolate travel expenses ####
prez_expends <- prez_expends %>% 
  mutate(
    expenditure_purpose_descrip = str_squish(str_to_upper(expenditure_purpose_descrip)),
    payee_organization_name = str_squish(str_to_upper(payee_organization_name))
  )

prez_travel <- prez_expends %>% 
  filter(expenditure_year == 2019,
         str_detect(expenditure_purpose_descrip, "TRAVEL") |
           str_detect(expenditure_purpose_descrip, "TRANSPORTATION") |
           str_detect(expenditure_purpose_descrip, "AIR") |
           str_detect(expenditure_purpose_descrip, "PLANE")
           ) 

#see the desc variations
prez_travel %>% 
  count(expenditure_purpose_descrip) 

#check amounts for active vs. memo
prez_travel %>% 
  group_by(status) %>% 
  summarise(n(), sum(expenditure_amount))


#look for possible airline mentions as payee
air_search <- prez_travel %>% 
         filter(
           str_detect(payee_organization_name, "AIR"),	
           !str_detect(payee_organization_name, "FAIRFIELD INN"),
           !str_detect(payee_organization_name, "AIRBNB"),
           !str_detect(payee_organization_name, "AIRPORT ")
                )


air_search %>% 
  count(payee_organization_name) %>% View()

#NOTE: remember to examine the active vs. memo in the results!

#appears to be a record for possible charter flight by Warren:
air_search %>% 
  filter(payee_organization_name == "AIR CHARTER TEAM, INC.") %>% 
  write_csv("output/warren_flight.csv")

#appears to be a record for possible charter flight by Warren:
air_search %>% 
  filter(payee_organization_name == "ZEN AIR") %>% 
  write_csv("output/zenair.csv")




#while we're at it, what's up with Airbnb expenses?
airbnb <- prez_travel %>% 
  filter(
    str_detect(payee_organization_name, "AIRBNB")
  )
