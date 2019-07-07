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

#pull out only BERNIE
bernie_expends <- expends_db %>%
  filter(filer_committee_id_number == "C00696948") %>% 
  collect()

#purpose descriptions
bernie_expends %>% 
  count(expenditure_purpose_descrip) 


#salary or payroll
bernie_expends_staff <- bernie_expends %>% 
  filter(expenditure_purpose_descrip == "Salary" |
           str_detect(expenditure_purpose_descrip, "Payroll")) 

#looks like Bernie uses memos to list actual staffers
bernie_expends_staff <- bernie_expends_staff %>% 
  filter(status == "MEMO")





# 
# #pull candidate table from postgres db
# cand_db <- tbl(con, "cycle_2020_candidate")
# 
# #filter only for presidential
# candnames <- cand_db %>% 
#   filter(district == "US") %>% 
#   select(name, fec_committee_id) %>% 
#   collect()
# 
# #grab prez committee ids
# prez_cmte_ids <- candnames %>% pull(fec_committee_id)
# 
# #filter the contribs table by them
# prez_contribs <- contribs_db %>% 
#   filter(filer_committee_id_number %in% prez_cmte_ids)


