######################################################
## This script is to download and prep datasets that do not change anymore; 
## They are saved as RDS files and ready to be used in later scripts;
######################################################

library(DBI)
library(magrittr)
library(tidyverse)
library(stringr)
library(dplyr)
library(scales)
library(grid)
library(gridExtra)
library(textclean)
library(lubridate)


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

#get list of all the files 
personal <- "jin.tong" #####<---------- PLEASE REPLACE

################################################################################
pull_GPcpcs <- function(){
  
  ########### GP CPCS data  ----
  sql= "select [RecordNo],[GPCode]=[Ref_ReferrerProviderNHSCode], [ContractorCode]=[Pharm_FollowupProviderNHSCode], 
[Measure] = 'GP CPCS',
-- case when [ReportingMonth] < 10 then CONCAT([ReportingYear], '-0', [ReportingMonth], '-01') else CONCAT([ReportingYear], '-', [ReportingMonth], '-01') end as [Month]
[Month]=format(DATEADD(MONTH, DATEDIFF(MONTH, 0, [ReportingWeekCommencing]), 0), 'yyyy-MM-dd') 
from [CommunityPharmacy_Archive].[CPCS_GP_start_to_21_22] where [IsValidRecord] = 1 and [Ref_ReferralStatus] = 'Completed'"
  
  result<-dbSendQuery(con,sql)
  GPcpcs<-dbFetch(result)
  dbClearResult(result)
  
  GPcpcs
}


################################################################################
pull_cpcs111 <- function(){
  ######## 111 CPCS data  ----

  sql="SELECT  [Month],
		[ContractorCode], 
		[Measure],
		[Figure] = COUNT(DISTINCT [PhIF_RecordID])
FROM    (select [ContractorCode] = [FollowupProviderNHSCode], [Month]=format(DATEADD(MONTH, DATEDIFF(MONTH, 0, [Provision Date]), 0), 'yyyy-MM-dd'), [PhIF_RecordID], 
			 case when [CPCS_Workstream] = 'Minor Illness_SONAR' OR [CPCS_Workstream] = 'DMIRS_PharmOutcomes' then '111 Minor Illness (MI) CPCS activity' 
			      when  [CPCS_Workstream] = 'Emergency supply_SONAR' OR [CPCS_Workstream] = 'NUMSAS_PharmOutcomes' then '111 Urgent Medicine Supply (US) CPCS activity'
			 else '' end as [Measure]
			 from [CommunityPharmacy_Archive].[CPCS_MI_US_start_to_21_22]  where [Referral Status] = 'completed'
			) as a
group by [ContractorCode], [Month], [Measure]"
  
  result<-dbSendQuery(con,sql)
  cpcs111<-dbFetch(result)
  dbClearResult(result)
  
  cpcs111
}

################################################################################
pull_gp_stage <- function(){
  ######## GP CPCS provider stage position----

  sql="SELECT [Practice ODS Code]
      ,[Practice System]
      ,[Practice Referral Method]
      ,[2nd_stage]=format(DATEADD(MONTH, DATEDIFF(MONTH, 0, [Engaged Date]), 0), 'yyyy-MM-dd')
      ,[3rd_stage]=format(DATEADD(MONTH, DATEDIFF(MONTH, 0, [Agreed Go-live Date]), 0), 'yyyy-MM-dd')
      ,[Stage]
      ,[ODS Region]
      ,[ODS CCG]
      ,[ODS STP]
	    ,[PCN Name]
      ,[Practice_List_Size]
  FROM [CommunityPharmacy_PhifArchive].[GPCPCS_Providers_Final]"
  
  result<-dbSendQuery(con,sql)
  gp_stage<-dbFetch(result)
  dbClearResult(result)
  
  gp_stage
}

pull_cpcs_new <- function(){

  sql="SELECT *
  FROM [CommunityPharmacy_PhifArchive].[CPCS Data 22_23_onwards]"
  
  result<-dbSendQuery(con,sql)
  cpcs_new<-dbFetch(result)
  dbClearResult(result)
  
  cpcs_new
}


GPcpcs <- pull_GPcpcs()
cpcs111 <- pull_cpcs111()
gp_stage <-pull_gp_stage()
names(gp_stage) <- names(gp_stage) %>% make.names()
cpcs_new<- pull_cpcs_new()




############## Prepare GP CPCS data----
#aggregate old data at pharmacy level
GPcpcs1<- GPcpcs %>%
  select(`Month`,`ContractorCode`,`Measure`)%>%
  group_by(`Month`,`ContractorCode`, `Measure`)%>%
  mutate(`Figure`=max(row_number()))%>%
  distinct()%>%
  collect()

#Prepare new cpcs data -- after 01/04/22, cpcs data is from new source


######################
####we decide to mainly use [presc_code] instead [referral_org_ods] as we are told practices (e.g. B83037) that confirmed not making referrals have appeared in [referral_org_ods] column ####
#### but both columns are free text field without validation rules, so both have data quality issues. We have done some manual cleanning using ODS portal to spot check and replacing missing values. 
#### Whenever an error (e.g. misreporting about a practice) is flagged, we then add it into our automated cleaning process. 
#######################


############# Patient list sizes - latest download from NHS D (uploaded to NCDR)------
pull_gp<- function(){

  sql="SELECT b.*
FROM
(SELECT MAX([EXTRACT_DATE]) as [EXTRACT_DATE], [PRACTICE_CODE]
  FROM [CommunityPharmacy_Public].[z_gp-reg-pat-prac-map]
  GROUP BY [PRACTICE_CODE]) a
  LEFT JOIN
(SELECT MAX([EXTRACT_DATE]) as [EXTRACT_DATE], [PRACTICE_CODE], [COMM_REGION_CODE],[COMM_REGION_NAME]
  FROM [CommunityPharmacy_Public].[z_gp-reg-pat-prac-map]
  GROUP BY [PRACTICE_CODE], [COMM_REGION_CODE],[COMM_REGION_NAME]) b
  ON a.[PRACTICE_CODE] = b.[PRACTICE_CODE] AND a.[EXTRACT_DATE]= b.[EXTRACT_DATE]
"
  result<-dbSendQuery(con,sql)
  gp<-dbFetch(result)
  dbClearResult(result)
  
  gp <- gp %>% 
    select(GPCode = PRACTICE_CODE, 
           Region_code=COMM_REGION_CODE
           ,Region_Name=COMM_REGION_NAME)%>% 
    collect()
  
  gp
}

gp_reg<- pull_gp()

validGP<-unique(gp_reg$`GPCode`)
keep_referer_ods = c("J84007","F83004","M83723","B83055","E84017","E85026") # among mismatched rows, spot sense check on ODS portal for the practices with multiple referral records

cpcs_new$`presc_code`<- substr(cpcs_new$`presc_code`, 1,6)
cpcs_new$`referral_org_ods`<- substr(cpcs_new$`referral_org_ods`, 1,6)

cpcs_new$`presc_code`<-ifelse(cpcs_new$`presc_code` == "V81999"|
                                cpcs_new$`presc_code` == "GMP123"|
                                cpcs_new$`presc_code` == "NULL"|
                                is.null(cpcs_new$presc_code)|
                                is.na(cpcs_new$presc_code),
                              cpcs_new$`referral_org_ods`, cpcs_new$`presc_code` ) # invalid ODS code, or the ones means unknown according to ODS portal
cpcs_new$`presc_code`<-ifelse(!(cpcs_new$`presc_code` %in% validGP) | cpcs_new$`presc_code` %in% keep_referer_ods, cpcs_new$`referral_org_ods`, cpcs_new$`presc_code` )

#cpcs_new$`presc_code`<-ifelse(substr(cpcs_new$`referral_org_ods`, 1,6)=="J84007",substr(cpcs_new$`referral_org_ods`, 1,6), cpcs_new$`presc_code` ) ## seems patients from other regions visiting isle of wight, so stick to the practice within the same region



cpcs_new_1<-cpcs_new%>%
  mutate(`Month`= paste0(substr(`part_month`,1,4), "-", substr(`part_month`, 5,6), "-01"),
         `ContractorCode` = `disp_code`, `GPCode`= `presc_code`,
         `Measure` = case_when(`px_cpcs_token_type`== "0" & `referrer_org_type`!="UTC"~ "111 Urgent Medicine Supply (US) CPCS activity",
                               `px_cpcs_token_type`== "1" & `referrer_org_type`!="UTC" ~ "111 Minor Illness (MI) CPCS activity",
                               `px_cpcs_token_type`== "3" & `referrer_org_type`!="UTC" ~ "GP CPCS",
                               `referrer_org_type`=="UTC" |`px_cpcs_token_type`== "5" | `px_cpcs_token_type`== "6" ~ "UEC CPCS"
                               #`referrer_org_type`== "NHS_111_ONLINE" ~ "111 (online & telephony) CPCS activity (MI & US)",
                               # `referrer_org_type`=="NHS_111_SERVICE" ~ "111 (online & telephony) CPCS activity (MI & US)",
                               #`referrer_org_type`== "GP_PRACTICE" ~ "GP CPCS",
                               #`referrer_org_type`== "GP_ONLINE" ~ "GP CPCS"
         ))%>%
  collect()

cpcs_new_2<-cpcs_new_1%>%
  group_by(`Month`,`ContractorCode`, `Measure`)%>%
  summarise(`Figure`=n_distinct(`id_3`))%>%
  mutate(Figure = as.numeric(Figure))%>%
  filter(!is.na(`Measure`), 
         `Month`>= "2022-04-01", #exclude any records before 1st April; 
         `Month`< "2024-02-01"  #also exclude any records since 31st Jan 24;
  )%>% 
  collect()

GPcpcs1$`Figure`<- as.numeric(GPcpcs1$`Figure`)
cpcs111$`Figure`<- as.numeric(cpcs111$`Figure`)

CPCS_all <- rbind(GPcpcs1, cpcs111, cpcs_new_2)

########## aggregate old and new gp cpcs data at practice level
cpcs_new_3<- cpcs_new_1%>% # aggregate new bsa data
  filter(`Measure`== "GP CPCS", `Month`>= "2022-04-01"
         , `Month`< "2024-02-01"
  )%>%
  group_by(`Month`,`GPCode`, `Measure`)%>%
  summarise(`Figure`=n_distinct(`id_3`))%>%
  mutate( Figure = as.numeric(Figure))%>%
  collect()

GPcpcs2<- GPcpcs %>% # aggregate old data from pharmacy system
  select(`Month`,`GPCode`,`Measure`)%>%
  group_by(`Month`,`GPCode`, `Measure`)%>%
  mutate(`Figure`=max(row_number()))%>%
  distinct()%>%
  collect()

GPcpcs2<- rbind(GPcpcs2, cpcs_new_3)

pull_dos<- function(){

  sql= "SELECT Figure=count( [Case ID] ), [Location Region]
      ,[Service Status]
      ,[Service Type]
      ,[Year Month]
  FROM [CommunityPharmacy_Restricted].[111 DOS data for CPCS]
  group by [Location Region]
      ,[Service Status]
      ,[Service Type]
      ,[Year Month]"
  
  result<-dbSendQuery(con,sql)
  DOS<-dbFetch(result)
  dbClearResult(result)
  
  DOS<-DOS %>%
    mutate(`Month`= paste0(substr(`Year Month`,1,4), "-", substr(`Year Month`, 6,7), "-01"))%>%
    select(`Month`,`Region_Name`=`Location Region`,`Status`= `Service Status`,`Type`= `Service Type`,`Figure`)%>%
    collect()
  
  DOS["Region_Name"][DOS["Region_Name"]== "East of England"] <- "East Of England"
  DOS["Region_Name"][DOS["Region_Name"]== "North East and Yorkshire"] <- "North East And Yorkshire"
  
  DOS
}


####### Save all downloads as RDS files
saveRDS(pull_dos(),"~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/DOS.rds" )
saveRDS(CPCS_all,"~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/CPCS_all.rds" )
saveRDS(GPcpcs2,"~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/GPcpcs2.rds" )
saveRDS(gp_stage,"~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/gp_stage.rds" )