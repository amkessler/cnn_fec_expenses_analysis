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

#pull out only Warren
warren_expends <- expends_db %>%
  filter(filer_committee_id_number == "C00693234") %>% 
  collect()

#purpose descriptions
warren_expends %>% 
  count(expenditure_purpose_descrip) %>% 
  View()


#salary or payroll
warren_expends_staff <- warren_expends %>% 
  filter(expenditure_purpose_descrip == "Salary" |
           str_detect(expenditure_purpose_descrip, "Payroll")) 

warren_expends_staff %>% 
  count(status)





