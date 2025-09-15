library(readxl)
#library(dplyr)
library(tidyverse)
library (DBI)
library (odbc)
library (dbplyr)



#### set up UDAL connection for extracts from data marts
serv  <-"udalsqlmartprod.database.windows.net"
db  <- "udal-sql-mart-qualitydataanalysis"
user  <- "jin.tong@udal.nhs.uk" #####<---------- PLEASE REPLACE

con_udal <- dbConnect(
  drv = odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = serv,
  database = db,
  UID = user,
  authentication = "ActiveDirectoryInteractive"
)

con = con_udal

#get list of all the files in the Unplanned closures folder and filter for ones starting with "unpl"
personal <- "jin.tong" #####<---------- PLEASE REPLACE


filenames <- as_tibble(list.files(path=paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting")))
unplanned_filenames <- filenames %>%
  dplyr::filter(tolower(substr(value,1,4)) == "unpl")

#initialise blank data frame to be filled with data from each region
all_data <- data.frame()

for(f in unplanned_filenames$value){

  data <- read_excel(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/", f), 
                     sheet = "data entry", 
                     col_types = "text"
                     )
  
  #for debugging to see which file fails if it does
  print(f)
  
  data <- data %>%
    dplyr::filter(!is.na(`ODS code`)) %>%
    rename_with(toupper) %>%
    mutate(input_file_name = f) 
  
  #make column names compatible with R
  names(data) <- names(data) %>% make.names()
  
  #create unique identifies for linked closures
  data <- data %>%
    mutate(unique_CLOSURE.NUMBER = paste0(CLOSURE.NUMBER, input_file_name),
           unique_LINKED.CLOSURE = if_else(!is.na(LINKED.CLOSURE),
                                           paste0(LINKED.CLOSURE, input_file_name),
                                           NA_character_)
           ) 

  all_data <- bind_rows(all_data, data) 

}


#sort out date column types and remove duplicates and invalid dates
#all_data$`HOURS.TO`<-ifelse(as.numeric(all_data$`HOURS.TO`)== 0, "1", all_data$`HOURS.TO`)

all_data_1 <- all_data %>%
  dplyr::filter(!str_detect(CLOSURE.NUMBER, "test")) %>%
  select(-starts_with("BLANK"),
         -starts_with("EMAIL.ADDRESS"),
         -starts_with("...")) %>%
  mutate(DATE.OF.CLOSURE = as.numeric(DATE.OF.CLOSURE),
         HOURS.FROM = as.numeric(HOURS.FROM),
         HOURS.TO = as.numeric(HOURS.TO)) %>%
  mutate(DATE.OF.CLOSURE = as.Date(DATE.OF.CLOSURE, origin = "1899-12-30"),
         HOURS.FROM = chron::times(HOURS.FROM),
         HOURS.TO = chron::times(HOURS.TO),
         DURATION.OF.CLOSURE = HOURS.TO - HOURS.FROM,
         unique_string = paste0(ODS.CODE, DATE.OF.CLOSURE, HOURS.FROM)) %>%
  mutate(IsDuplicate = duplicated(unique_string)) %>%
  dplyr::filter(!IsDuplicate) %>%
  select(-IsDuplicate) %>%
  dplyr::filter(DATE.OF.CLOSURE >= as.Date("2021-10-01")) %>%
  select(CLOSURE.NUMBER, 
         LINKED.CLOSURE, 
         REASON.FOR.LINKED.CLOSURE, 
         ODS.CODE, 
         DATE.OF.CLOSURE, 
         REASON.FOR.CLOSURE, 
         NOTES.ON.REASON.FOR.CLOSURE, 
         HOURS.FROM, 
         HOURS.TO, 
         DURATION.OF.CLOSURE, 
         input_file_name, 
         unique_CLOSURE.NUMBER, 
         unique_LINKED.CLOSURE,
         unique_string) %>%
  mutate(ODS.CODE = toupper(ODS.CODE))


pull_ref_contractor<-function(){
  sql <- " select *   FROM [CommunityPharmacy_Restricted].[Ref_Contractor]"
  result <- dbSendQuery(con,sql)
  Ref_Contractor <- dbFetch(result)
  
  Ref_Contractor <- Ref_Contractor %>%
    select(ODS.CODE = ContractorCode,
           Health_Wellbeing_Board,
           ContractorName,
           ParentOrgName,
           Address1,
           Address2,
           Address3,
           Address4,
           PostCode,
           STP,
           STP.Name = STP_Name,
           Region_Name,
           ParentOrgSize,
           LA_Name,
           SettlementCategory,
           RuralUrbanClass,
           Is100hourPharmacy = `100hourPharmacy`,
           ContractType,
           Supermarket,
           IMD_Decile,
           CoLocated_withGP,
           ParentOrgName_Clean
    )
}

Ref_Contractor <-pull_ref_contractor()

#saveRDS(Ref_Contractor, "~/Rprojects/Unplanned-pharmacy-closures/Data files for monthly pack/Ref_Contractor.rds")

all_data_1 <- all_data_1 %>%
  left_join(Ref_Contractor, by = "ODS.CODE") %>%
  mutate(`Organisation.Name.Cat` = case_when( ParentOrgName =="LLOYDS PHARMACY LTD" ~"Lloyds",
                                              ParentOrgName =="BOOTS UK LIMITED" ~"Boots",
                                              ParentOrgName =="BESTWAY NATIONAL CHEMISTS LIMITED" ~"Bestway",
                                              ParentOrgName =="L ROWLAND & CO (RETAIL) LTD" ~"Rowland",
                                              ParentOrgName =="ASDA STORES LTD" ~"ASDA",
                                              ParentOrgName =="TESCO STORES LIMITED" ~"Tesco",
                                              ParentOrgName=="TESCO PLC" ~ "TESCO",
                                              TRUE ~ "All Other Pharmacies")) %>%
  mutate(month_of_closure = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month"))

all_data_1 <- all_data_1 %>%
  dplyr::filter(month_of_closure < (Sys.Date() - lubridate::weeks(4)))

all_data_1$`REASON.FOR.CLOSURE`<-ifelse(is.na(all_data_1$`REASON.FOR.CLOSURE`),"Other, please fill in column U", all_data_1$`REASON.FOR.CLOSURE` )

#save data
writexl::write_xlsx(all_data_1, paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data/unplannedClosuresData.xlsx"))
saveRDS(all_data_1, "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/Data files for monthly pack/unplannedClosuresData.rds")

data_date <- format(max(all_data_1$month_of_closure), "%b%y")

# #internal stakeholder data
internal_data <- all_data_1 %>%
  select(`Pharmacy ODS Code` = ODS.CODE,
         `Pharmacy Trading Name` =  ContractorName, #Pharmacy.Trading.Name, #
         `Parent Company` = ParentOrgName, #Organisation.Name, #
         `Address Field 1` = Address1, #Address.Field.1, #
         `Address Field 2` = Address2, #Address.Field.2,
         `Address Field 3` = Address3, #Address.Field.3,
         `Address Field 4` = Address4, #Address.Field.4,
         `Postcode`= PostCode, #postcode, #
         `Health and Wellbeing Board` = Health_Wellbeing_Board,
         `ICB Code` = STP, #STP.Code, #
         `ICB Name` = STP.Name, #ICB.Name,
         `Region Name` = Region_Name,
         `Contract Type` = ContractType,
         `100 hours pharmacy?` = Is100hourPharmacy,
         `Date of Closure` = DATE.OF.CLOSURE,
         `Reason for Closure` = REASON.FOR.CLOSURE,
         `Notes on Reason for Closure` = NOTES.ON.REASON.FOR.CLOSURE,
         `Hours from` =`HOURS.FROM`,
         `Hour to` = `HOURS.TO`,
         `Duration of closure (hours)` = `DURATION.OF.CLOSURE`)

#correct negative time difference due to overnight closure
internal_data$`Duration of closure (hours)`<-ifelse(internal_data$`Duration of closure (hours)`<0, internal_data$`Hour to`+1 -internal_data$`Hours from`, internal_data$`Duration of closure (hours)` )

#internal_data$`Hours from`<-as.character(internal_data$`Hours from`)
#internal_data$`Hour to`<-as.character(internal_data$`Hour to`)
#internal_data$`Duration of closure (hours)`<-as.character(internal_data$`Duration of closure (hours)`)

external_data <- internal_data %>%
  select(-`Notes on Reason for Closure`)

writexl::write_xlsx(internal_data, paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data/Internal stakeholders/collated_unplanned_closures_data_Oct21_", data_date, "_INTERNAL.xlsx"))
writexl::write_xlsx(external_data, paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data/External stakeholders/collated_unplanned_closures_data_Oct21_", data_date, "_EXTERNAL.xlsx"))

list<-c("East Of England","London", "Midlands", "North East And Yorkshire", "North West", "South East", "South West")

for (x in list) {
  data<- internal_data%>% dplyr::filter(`Region Name`== x)
  writexl::write_xlsx(data, paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data/Internal stakeholders/Regional data/collated_unplanned_closures_data_Oct21_", data_date, "_", x, ".xlsx"))
  
}


################################################################################
pull_raw_items_dispensed <- function(){
  # obtain number of items dispensed per year data from NCDR
  # should be updated with new years
  
  sql="

select * FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
where Measure = 'items dispensed'

"
  
  result <- dbSendQuery(con,sql)
  itemsDispensed <- dbFetch(result)
  dbClearResult(result)
  itemsDispensed
}

################################################################################
pull_pharm_list <- function(){
  
  sql = "SELECT *
  FROM [CommunityPharmacy_Public].[Ref_PharmaceuticalList]"
  result <- dbSendQuery(con,sql)
  pharm_list <- dbFetch(result)
  dbClearResult(result)
  
  names(pharm_list) <- names(pharm_list) %>% make.names()
  
  #fix miscoded snapshot month
  pharm_list <- pharm_list %>%
    mutate(SnapshotMonth = as.Date(SnapshotMonth)) %>%
    mutate(SnapshotMonth = if_else(SnapshotMonth == as.Date("2022-10-01"), 
                                   as.Date("2022-09-01"), 
                                   SnapshotMonth)) %>%
    rename(ODS.CODE = Pharmacy.ODS.Code..F.Code., 
           postcode = Post.Code, 
           ICB.Name = STP.Name, 
           EPS.Indicator = EPS.Enabled)
}

co_ords_data <- read_csv(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ONSPD_NOV_2022_UK.csv"))



################################################################################
source("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/02_monthly_pack_graphs.R")
full_pharmlist <- pull_pharm_list()

get_scorecard_data(data = all_data_1)