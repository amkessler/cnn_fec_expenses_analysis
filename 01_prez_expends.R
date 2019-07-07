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

#pull out only BIDEN
# expends_db %>% 
#   filter(filer_committee_id_number == "C00213652")


#pull candidate table from postgres db
cand_db <- tbl(con, "cycle_2020_candidate")

#filter only for presidential
candnames <- cand_db %>%
  filter(district == "US") %>%
  select(name, fec_committee_id) %>%
  collect()

#grab prez committee ids
prez_cmte_ids <- candnames %>% pull(fec_committee_id)

#filter the contribs table by them and then collect locally
prez_expends <- expends_db %>%
  filter(filer_committee_id_number %in% prez_cmte_ids) %>% 
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
