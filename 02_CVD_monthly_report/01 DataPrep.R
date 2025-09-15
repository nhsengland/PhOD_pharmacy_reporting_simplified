library(tidyverse)
library(ggplot2)
library(ggpubr)
library(stringr)
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)
library(scales)
library(readxl)
library(textclean)
library(DBI)
library (odbc)

################################################################################
get_download_timestamp <- function(){
   ############# Date and Time stamp for the latest download from NCDR ----
   Date_stamp <- format(Sys.time(), "%a %b %d %X %Y")
}


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

######## Pharmacy contractor tables ----
pull_contractor<-function(){

sql=" 
SELECT [Fcode]=[ContractorCode]
      ,[ContractorName]
      ,[ContractorType]
      ,[PharmacyCategory]
      ,[STP]
      ,[STP_Name]
      ,[Region_Name]
      ,[SettlementCategory]
      ,[RuralUrbanClass]
      ,[Pharmacy_100hour] = [100hourPharmacy]
      ,[ContractType]
      ,[Supermarket]
      ,[IMD_Decile]
      ,[CoLocated_withGP]
      ,[DispensingDocListSize]
      ,[ParentOrgSize]
      ,[ICB]
      ,[ICB_Name],
      [EndDate] 
  FROM [CommunityPharmacy_Restricted].[Ref_Contractor]
   --where [EndDate] = '1900-01-01 00:00:00.000' or [EndDate] is NULL
"
result<-dbSendQuery(con,sql)
Contractor<-dbFetch(result)

#clean co-located and ContractType columns in Contractor table
Contractor$`CoLocated_withGP`<- ifelse(is.na(Contractor$`CoLocated_withGP`), FALSE, Contractor$`CoLocated_withGP`)
Contractor["ContractType"][Contractor["ContractType"]== "Community Pharmacy" | Contractor["ContractType"]== "Pharmacy"] <- "Community"
Contractor$`Permanently_closed`= "Yes"
Contractor["Permanently_closed"][Contractor["EndDate"]== "1900-01-01 00:00:00.000" | is.na(Contractor$EndDate)] <- "No"
Contractor_simple <- Contractor[,c("Fcode", "ContractorName", "Region_Name","STP", "STP_Name", "IMD_Decile", "Permanently_closed")]

Contractor_simple
}


record_master <- function(){
   
   sql= "SELECT [submission_id]
      ,[disp_code]
      ,[part_month]
      ,[declaration_date]
      ,[declaration_agree]
      ,[submission_created]
      ,[submission_last_modified]
      ,[patient_age]
      ,[bp_check]
      ,[abpm]
      ,[bp_check_referral]
      ,[bp_check_date]
      ,[abpm_referral]
      ,[abpm_date_started]
      ,[patient_details_created]
      ,[patient_details_last_modified]
      ,[systolic_bp_result]
      ,[diastolic_bp_result]
      ,[systolic_abpm_result]
      ,[diastolic_abpm_result]
  FROM [CommunityPharmacy_Restricted].[BloodPressureService_MYS_activity]"
   result<-dbSendQuery(con,sql)
   cvd<-dbFetch(result)
   
cvd<-cvd%>%select(`submission_id`
                      ,`disp_code`
                      ,`part_month`
                      ,`declaration_agree`
                      ,`patient_age`
                      ,`bp_check`
                      ,`abpm`
                      ,`bp_check_referral`
                      ,`abpm_referral`
                      ,`systolic_bp_result`
                      ,`diastolic_bp_result`
                      ,`systolic_abpm_result`
                      ,`diastolic_abpm_result`)%>%collect()


cvd$`declaration_agree`<- ifelse(cvd$`declaration_agree`== "true"|cvd$`declaration_agree`== "TRUE"|cvd$`declaration_agree`== "True", TRUE, FALSE)
cvd$`bp_check`<- ifelse(cvd$`bp_check`== "true"|cvd$`bp_check`== "TRUE"|cvd$`bp_check`== "True", TRUE, FALSE)
cvd$`abpm`<- ifelse(cvd$`abpm`== "true"|cvd$`abpm`== "TRUE"|cvd$`abpm`== "True", TRUE, FALSE)
cvd$`bp_check_referral`<- ifelse(cvd$`bp_check_referral`== "true"|cvd$`bp_check_referral`== "TRUE"|cvd$`bp_check_referral`== "True", TRUE, FALSE)
cvd$`abpm_referral`<- ifelse(cvd$`abpm_referral`== "true"|cvd$`abpm_referral`== "TRUE"|cvd$`abpm_referral`== "True", TRUE, FALSE)

current <- as.Date(paste(format(Sys.Date(), "%Y-%m"), "-01", sep = ""))


### extract data from new API -- temp solution simply convert new API data into old format of dataframe, so metric calculations in current report does not need to change
### This is only a temp solution to append unverified data from new API as part of pharmacy first expansion. Data quality has worsened so scripts below need to be review and revised

sql= "select 
case when a.[submission_id] is NULL then b.[submission_id] else a.[submission_id] end as [submission_id],
case when a.[patient_age] is NULL then b.[patient_age] else a.[patient_age] end as [patient_age],
case when a.[disp_code] is NULL then b.[disp_code] else a.[disp_code] end as [disp_code],
case when b.[abpm_Date] is NULL then CONCAT(datepart(year, a.[bp_Date]),CASE WHEN LEN(datepart(month, a.[bp_Date])) = 1 THEN CONCAT('0', CAST(datepart(month, a.[bp_Date]) AS VARCHAR(2)))
                                                                        ELSE CAST(datepart(month, a.[bp_Date]) AS VARCHAR(2)) END)
                                else CONCAT(datepart(year,[abpm_Date]), CASE WHEN LEN(datepart(month, b.[abpm_Date])) = 1 THEN CONCAT('0', CAST(datepart(month, b.[abpm_Date]) AS VARCHAR(2)))
                                                                        ELSE CAST(datepart(month, b.[abpm_Date]) AS VARCHAR(2)) END) 
                               end as [part_month],
a.[bp_check],a.[bp_check_referral],a.[diastolic_bp_result],a.[systolic_bp_result],
b.[abpm],b.[abpm_referral], b.[systolic_abpm_result],b.[diastolic_abpm_result]
from
(SELECT [Reference] as [submission_id],
         [Patient Age] as [patient_age]   
      ,[Claimant ODS Code] as [disp_code],  
	   [Assessment Date] as [bp_Date]
      ,[Service Type] as [bp_check]
      ,[Referral Source] as [bp_check_referral]
      ,[Systolic BP] as [systolic_bp_result]
      ,[Diastolic BP] as [diastolic_bp_result]     
  FROM [CommunityPharmacy_Restricted].[BloodPressureService_MYS_activity_NEW_API]
  where [Service Type] = 'CLINIC' and [Assessment Date]<='2024-03-31'
  union
  SELECT [Reference] as [submission_id],
         [Patient Age] as [patient_age]   
      ,[Claimant ODS Code] as [disp_code],  
	   [Assessment Date] as [bp_Date]
      ,[Service Type] as [bp_check]
      ,[Referral Source] as [bp_check_referral]
      ,[Systolic BP] as [systolic_bp_result]
      ,[Diastolic BP] as [diastolic_bp_result]     
  FROM [CommunityPharmacy_Restricted].[BloodPressureService_MYS_activity_NEW_API_Monthly]
  where [Service Type] = 'CLINIC' and [Assessment Date]>'2024-03-31') a
full join
 ( SELECT [Reference] as [submission_id],
         [Patient Age] as [patient_age],
  cast([Assessment Date] AS DATE) as [abpm_Date] 
      ,[Claimant ODS Code] as [disp_code]     
      ,[Service Type] as [abpm]
      ,[Referral Source] as [abpm_referral]
      ,[Systolic BP] as [systolic_abpm_result]
      ,[Diastolic BP] as [diastolic_abpm_result]     
  FROM [CommunityPharmacy_Restricted].[BloodPressureService_MYS_activity_NEW_API]
  where [Service Type] = 'ABPM' and [Assessment Date]<='2024-03-31'
   union
  SELECT [Reference] as [submission_id],
         [Patient Age] as [patient_age],
  cast([Assessment Date] AS DATE) as [abpm_Date] 
      ,[Claimant ODS Code] as [disp_code]     
      ,[Service Type] as [abpm]
      ,[Referral Source] as [abpm_referral]
      ,[Systolic BP] as [systolic_abpm_result]
      ,[Diastolic BP] as [diastolic_abpm_result]     
  FROM [CommunityPharmacy_Restricted].[BloodPressureService_MYS_activity_NEW_API_Monthly]
  where [Service Type] = 'ABPM' and [Assessment Date]>'2024-03-31') b
  
  on a.[submission_id]=b.[submission_id] and a.[disp_code] =b.[disp_code] "
result<-dbSendQuery(con,sql)
cvd_new<-dbFetch(result)

cvd_new$`declaration_agree`<- TRUE
cvd_new$`bp_check`<- ifelse(cvd_new$`bp_check`== "CLINIC", TRUE, FALSE)
cvd_new$`bp_check`<- ifelse(cvd_new$`bp_check`== ""|is.na(cvd_new$`bp_check`), FALSE,TRUE)
cvd_new$`abpm`<- ifelse(cvd_new$`abpm`== "ABPM", TRUE, FALSE)
cvd_new$`abpm`<- ifelse(cvd_new$`abpm`== ""|is.na(cvd_new$`abpm`),  FALSE,TRUE)
cvd_new$`bp_check_referral`<- ifelse(cvd_new$`bp_check_referral`== "PHARMACY"|cvd_new$`bp_check_referral`== "SELF_REFERRAL"|cvd_new$`bp_check_referral`== ""|is.na(cvd_new$`bp_check_referral`), FALSE,TRUE)
cvd_new$`abpm_referral`<- ifelse(cvd_new$`abpm_referral`== "PHARMACY"|cvd_new$`abpm_referral`=="SELF_REFERRAL"|cvd_new$`abpm_referral`== ""|is.na(cvd_new$`abpm_referral`),FALSE, TRUE)

cvd_all<-rbind(cvd, cvd_new)


cvd<- cvd_all %>%
  mutate(`Month`= paste0(substr(`part_month`,1,4), "-", substr(`part_month`, 5,6), "-01"))%>%
  mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"), `Fcode`= `disp_code`)%>%
   filter(`Month`< current)%>%
 # filter(`Month`<'2024-03-01')%>%
  collect()
  
## new variable showing BP check result based on systolic and diastolic readings
  cvd$bp_result = NA
  other= cvd$bp_check == TRUE
  cvd$bp_result[other] = "Other"
  low = cvd$bp_check == TRUE & cvd$systolic_bp_result <=90 & cvd$diastolic_bp_result <=60
  cvd$bp_result[low] = "Low"
  normal = cvd$bp_check == TRUE & cvd$systolic_bp_result>90 & cvd$systolic_bp_result<140 & cvd$diastolic_bp_result >=60 & cvd$diastolic_bp_result <90
  cvd$bp_result[normal] = "Normal"
  high = cvd$bp_check == TRUE & cvd$systolic_bp_result>=140 & cvd$systolic_bp_result<180 & cvd$diastolic_bp_result >=90 & cvd$diastolic_bp_result <120
  cvd$bp_result[high] = "High"
  veryhigh = cvd$bp_check == TRUE & cvd$systolic_bp_result>=180 & cvd$diastolic_bp_result >=120
  cvd$bp_result[veryhigh] ="Very high"
  veryhigh_a = cvd$bp_check == TRUE & cvd$systolic_bp_result>=180 & cvd$bp_result != "Very high"
  veryhigh_b = cvd$bp_check == TRUE &  cvd$diastolic_bp_result >=120 & cvd$bp_result != "Very high"
  cvd$bp_result[veryhigh] ="Very high"
  refer1 = cvd$bp_check == TRUE & cvd$systolic_bp_result>=140 & cvd$bp_result != "High" & cvd$bp_result != "Very high" # raised systolic pressure only
  cvd$bp_result[refer1] ="refer1"
  refer2  = cvd$bp_check == TRUE & cvd$diastolic_bp_result >=90 & cvd$bp_result != "High" & cvd$bp_result != "Very high" # raised diastolic pressure only
  cvd$bp_result[refer2] ="refer2"

  ## new variable assigning age group
  cvd$age_grp = NA
  under20 = cvd$`patient_age`<20
  to39 = cvd$`patient_age`>=20 & cvd$`patient_age`<=39
  to59 = cvd$`patient_age`>=40 & cvd$`patient_age`<=59
  to79 = cvd$`patient_age`>=60 & cvd$`patient_age`<=79
  over80 = cvd$`patient_age`>=80
  cvd$age_grp[under20] = "Under 20"
  cvd$age_grp[to39] = "20-39"
  cvd$age_grp[to59] = "40-59"
  cvd$age_grp[to79] = "60-79"
  cvd$age_grp[over80] = "Over 80"
  
  ## new variable showing ABPM check result based on systolic and diastolic readings
  cvd$abpm_result = NA
  other= cvd$abpm == TRUE
  low = cvd$abpm == TRUE & cvd$systolic_abpm_result<=90 & cvd$diastolic_abpm_result <=60
  normal = cvd$abpm == TRUE & cvd$systolic_abpm_result>90 & cvd$systolic_abpm_result<135 & cvd$diastolic_abpm_result >60 & cvd$diastolic_abpm_result <85
  stage1 = cvd$abpm == TRUE & cvd$systolic_abpm_result>=135 & cvd$systolic_abpm_result<150 & cvd$diastolic_abpm_result >=85 & cvd$diastolic_abpm_result <95
  stage2 = cvd$abpm == TRUE & cvd$systolic_abpm_result>=150 & cvd$systolic_abpm_result<170 & cvd$diastolic_abpm_result >=95 & cvd$diastolic_abpm_result <115
  severe = cvd$abpm == TRUE & cvd$systolic_abpm_result>=170 & cvd$diastolic_abpm_result >=115 
  cvd$abpm_result[other] = "Other"
  cvd$abpm_result[low] = "Low"
  cvd$abpm_result[normal] = "Normal"
  cvd$abpm_result[stage1] = "Stage 1"
  cvd$abpm_result[stage2] = "Stage 2"
  cvd$abpm_result[severe] = "Potential severe hypertension"
  
  stage1_a = cvd$abpm == TRUE & cvd$systolic_abpm_result>=135 & cvd$systolic_abpm_result<150 & cvd$abpm_result != "Stage 1" # raised systolic pressure only
  stage1_b = cvd$abpm == TRUE & cvd$diastolic_abpm_result >=85 & cvd$diastolic_abpm_result <95 & cvd$abpm_result != "Stage 1" # raised diastolic pressure only
  cvd$abpm_result[stage1_a] = "Stage 1"
  cvd$abpm_result[stage1_b] = "Stage 1"
  
  stage2_a = cvd$abpm == TRUE & cvd$systolic_abpm_result>=150 & cvd$systolic_abpm_result<170 & cvd$abpm_result != "Stage 2" # raised systolic pressure only
  stage2_b = cvd$abpm == TRUE & cvd$diastolic_abpm_result >=95 & cvd$diastolic_abpm_result <115 & cvd$abpm_result != "Stage 2" # raised diastolic pressure only
  cvd$abpm_result[stage2_a] = "Stage 2"
  cvd$abpm_result[stage2_b] = "Stage 2"
  
  severe_a = cvd$abpm == TRUE & cvd$systolic_abpm_result>=170 & cvd$abpm_result != "Potential severe hypertension"
  severe_b = cvd$abpm == TRUE & cvd$diastolic_abpm_result >=115 & cvd$abpm_result != "Potential severe hypertension"
  cvd$abpm_result[severe_a] = "Potential severe hypertension"
  cvd$abpm_result[severe_b] = "Potential severe hypertension"
  
  ##check if an ABPM is follow up of a opportunistic BP check
  cvd$abpm_fu = FALSE
  FU= cvd$`bp_check`== TRUE & cvd$`bp_check_referral`!=TRUE & cvd$abpm == TRUE
  cvd$abpm_fu[FU] = TRUE
  
  ##check if an ABPM is follow up of a referred BP check
  cvd$abpm_fu_ref = FALSE
  FU_ref= cvd$`bp_check`== TRUE & cvd$`bp_check_referral`==TRUE & cvd$abpm == TRUE
  cvd$abpm_fu_ref[FU_ref] = TRUE
 
  Contractor_simple<-subset(pull_contractor(), select = -c(`Permanently_closed`))
  
  cvd<- cvd%>%
    left_join(Contractor_simple,by="Fcode")%>%
    collect()
  
  cvd}

pull_bsa <- function(){
  
  #bsa <- read.csv("../data/cvd_claims.csv")
  
  sql= "SELECT *
  FROM [CommunityPharmacy_Public].[BloodPressureService_BSA_claims]"
  result<-dbSendQuery(con,sql)
  bsa<-dbFetch(result)
  
  bsa
}

pull_CVDclaims_1<-function(){
  cvdclaims<-pull_bsa()%>%
    mutate(`Month`=as.Date(`Month(claim)`, "%Y-%m-%d"),
           Fcode=`Pharmacy Code`,
           SetupFee = `Set up fee`+`Set up fee adjustment`,
           Incentive = `Incentive fee` + `Incentive fee adjustment`)%>%
    select(Month, Fcode, SetupFee, Incentive)%>%
    collect()
  
    cvdclaims
}

pull_CVDclaims_2<-function(){

   cvdclaims<-claims_prep%>%
       group_by(Fcode)%>%
       summarise(`Month`=min(Month), SetupFee = sum(SetupFee, na.rm=T))%>%
     collect()
     
   Contractor_simple<-pull_contractor()
   
   checks<-cvd%>%
      group_by(`Fcode`)%>%
      summarise(`max_submissionID`=max(submission_id))%>%
      collect()
   
   total<-get_pharmacy_master()%>%
      group_by(`Fcode`)%>%
      summarise(`Total_patients`=sum(`Total_patients`), `Total_checks`=sum(`Total_checks`))%>%
      collect()
   
   master<- cvdclaims%>%
     mutate(`Signup_Month`=Month)%>%
      filter(`SetupFee`>0)%>%
      left_join(Contractor_simple,by="Fcode")%>%
      left_join(checks,by="Fcode")%>%
      left_join(total, by = "Fcode")%>%
      collect()
   
   master$`ProvidingService?`<- ifelse(is.na(master$`max_submissionID`), "No", "Yes")
   master <- subset(master, select = -c(`max_submissionID`))
   
   master      
}

pull_CVDclaims_3<-function(){
  data<-claims_prep%>%
    group_by(Fcode)%>%
    summarise(total_incentive=sum(Incentive, na.rm=T))%>%
                right_join(claims_prep, "Fcode")%>%
    mutate(type = case_when(`Incentive` ==1000 ~ "Initial incentive",
                            `Incentive` ==400 & total_incentive == 1400 ~ "First followup incentive",
                            `Incentive` == 400 & total_incentive == 1800 & Month >'2023-03-01' ~ "Second followup incentive"))%>%
    filter(type != "NA")%>%
    group_by(`Month`,`type`)%>%
    summarise(`no_pharm`=n_distinct(`Fcode`))%>%
    collect()
  
  data
}

 #Aggregate at pharmacy level for master data table
 get_pharmacy_master <- function(){
   Allcheck <- cvd %>%
     group_by(`Month`,`Fcode`)%>%
     summarise(`Total_patients`=n(), `Avg_patient_age`=mean(`patient_age`), `min_age`=min(`patient_age`),  `max_age`=max(`patient_age`))%>%
     select(`Month`, `Fcode`, `Total_patients`, `min_age`, `Avg_patient_age`,`max_age`)%>%
     collect()
   Allcheck<- distinct(Allcheck)
   
   BP_opp <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE)%>%
     group_by(`Month`,`Fcode`)%>%
     summarise(`BP_Opp`=n(), `Avg_BPopp_age`=mean(`patient_age`))%>%
     select(`Month`, `Fcode`, `BP_Opp`, `Avg_BPopp_age`)%>%
     collect()
   BP_opp<-distinct(BP_opp)
   
   BP_opp_over40 <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `patient_age`>= 40)%>%
     group_by(`Month`,`Fcode`)%>%
     summarise(`BP_Opp_over40`=n())%>%
     select(`Month`, `Fcode`, `BP_Opp_over40`)%>%
     collect()
   BP_opp_over40<-distinct(BP_opp_over40)
   
   ABPM_opp <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE)%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`abpm_Opp`=n(), `Avg_ABPMopp_age`=mean(`patient_age`))%>%
     select(`Month`, `Fcode`, `abpm_Opp`, `Avg_ABPMopp_age`)%>%
     collect()
   ABPM_opp<-distinct(ABPM_opp)
   
   BP_ref <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`== TRUE)%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BP_ref`=n(), `Avg_BPref_age`=mean(`patient_age`))%>%
     select(`Month`, `Fcode`, `BP_ref`, `Avg_BPref_age`)%>%
     collect()
   BP_ref<-distinct(BP_ref)
   
   BP_ref_over40 <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`== TRUE, `patient_age`>= 40)%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BP_ref_over40`=n())%>%
     select(`Month`, `Fcode`, `BP_ref_over40`)%>%
     collect()
   BP_ref_over40<-distinct(BP_ref_over40)
   
   ABPM_ref <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE)%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`abpm_ref`=n(), `Avg_ABPMref_age`=mean(`patient_age`))%>%
     select(`Month`, `Fcode`, `abpm_ref`, `Avg_ABPMref_age`)%>%
     collect()
   ABPM_ref<-distinct(ABPM_ref)
   
   BPopp_veryhigh <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Very high")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPopp_veryhigh`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPopp_veryhigh`)%>%
     collect()
   BPopp_veryhigh<-distinct(BPopp_veryhigh)
   
   BPopp_low <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Low")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPopp_low`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPopp_low`)%>%
     collect()
   BPopp_low<-distinct(BPopp_low)
   
   BPopp_high <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`=="High")%>%
      group_by(`Month`,`Fcode`)%>%
      mutate(`BPopp_high`=max(row_number()))%>%
      select(`Month`, `Fcode`, `BPopp_high`)%>%
      collect()
   BPopp_high<-distinct(BPopp_high)
   
   BPopp_normal <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`=="Normal")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPopp_normal`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPopp_normal`)%>%
     collect()
   BPopp_normal<-distinct(BPopp_normal)
   
   BPopp_refer <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPopp_refer`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPopp_refer`)%>%
     collect()
   BPopp_refer<-distinct(BPopp_refer)
   
   
   BPopp_other <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `bp_result`== "Other")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPopp_other`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPopp_other`)%>%
     collect()
   BPopp_other<-distinct(BPopp_other)
   
   BPref_veryhigh <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Very high")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPref_veryhigh`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPref_veryhigh`)%>%
     collect()
   BPref_veryhigh<-distinct(BPref_veryhigh)
   
   BPref_low <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Low")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPref_low`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPref_low`)%>%
     collect()
   BPref_low<-distinct(BPref_low)
   
   BPref_high <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`=="High")%>%
      group_by(`Month`,`Fcode`)%>%
      mutate(`BPref_high`=max(row_number()))%>%
      select(`Month`, `Fcode`, `BPref_high`)%>%
      collect()
   BPref_high<-distinct(BPref_high)
   
   BPref_normal <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Normal")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPref_normal`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPref_normal`)%>%
     collect()
   BPref_normal<-distinct(BPref_normal)
   
   BPref_refer <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPref_refer`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPref_refer`)%>%
     collect()
   BPref_refer<-distinct(BPref_refer)
   
   BPref_other <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE, `bp_result`== "Other")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`BPref_other`=max(row_number()))%>%
     select(`Month`, `Fcode`, `BPref_other`)%>%
     collect()
   BPref_other<-distinct(BPref_other)
   
   ABPMopp_low <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Low")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMopp_low`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMopp_low`)%>%
     collect()
   ABPMopp_low<-distinct(ABPMopp_low)
   
   ABPMopp_normal <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Normal")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMopp_normal`=n())%>%
     select(`Month`, `Fcode`, `ABPMopp_normal`)%>%
     collect()
   ABPMopp_normal<-distinct(ABPMopp_normal)
   
   ABPMopp_stage1 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 1")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMopp_Stage1`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMopp_Stage1`)%>%
     collect()
   ABPMopp_stage1<-distinct(ABPMopp_stage1)
   
   ABPMopp_stage2 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 2")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMopp_Stage2`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMopp_Stage2`)%>%
     collect()
   ABPMopp_stage2<-distinct(ABPMopp_stage2)
   
   ABPMopp_severe <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Potential severe hypertension")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMopp_severe`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMopp_severe`)%>%
     collect()
   ABPMopp_severe<-distinct(ABPMopp_severe)
   
   ABPMopp_other <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Other")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMopp_other`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMopp_other`)%>%
     collect()
   ABPMopp_other<-distinct(ABPMopp_other)
   
   ABPMref_low <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Low")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMref_low`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMref_low`)%>%
     collect()
   ABPMref_low<-distinct(ABPMref_low)
   
   ABPMref_normal <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Normal")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMref_normal`=n())%>%
     select(`Month`, `Fcode`, `ABPMref_normal`)%>%
     collect()
   ABPMref_normal<-distinct(ABPMref_normal)
   
   ABPMref_stage1 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 1")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMref_Stage1`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMref_Stage1`)%>%
     collect()
   ABPMref_stage1<-distinct(ABPMref_stage1)
   
   ABPMref_stage2 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 2")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMref_Stage2`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMref_Stage2`)%>%
     collect()
   ABPMref_stage2<-distinct(ABPMref_stage2)
   
   ABPMref_severe <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Potential severe hypertension")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMref_severe`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMref_severe`)%>%
     collect()
   ABPMref_severe<-distinct(ABPMref_severe)
   
   ABPMref_other <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Other")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`ABPMref_other`=max(row_number()))%>%
     select(`Month`, `Fcode`, `ABPMref_other`)%>%
     collect()
   ABPMref_other<-distinct(ABPMref_other)
   
   oppBP_FU <-cvd%>%
     filter(`abpm_fu` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High")%>%
     group_by(`Month`,`Fcode`)%>%
     mutate(`oppBP_FU`=max(row_number()))%>%
     select(`Month`, `Fcode`, `oppBP_FU`)%>%
     collect()
    oppBP_FU<-distinct(oppBP_FU)
    
    refBP_FU <-cvd%>%
      filter(`abpm_fu_ref` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High")%>%
      group_by(`Month`,`Fcode`)%>%
      mutate(`refBP_FU`=max(row_number()))%>%
      select(`Month`, `Fcode`, `refBP_FU`)%>%
      collect()
    refBP_FU<-distinct(refBP_FU)
   
   
   #Add contractor details (e.g. region) only to pharmacy level data, so can be filtered at regional level for data provision purpose
    Contractor<-subset(pull_contractor(), select = -c(`Permanently_closed`))
   
   master <- Allcheck%>%
     left_join(Contractor, by= "Fcode")%>% 
     mutate(`Total_checks`= 0,`5BP?` = FALSE, `5BP+1ABPM`= FALSE)%>%
     left_join(BP_opp, by= c("Fcode", "Month"))%>% 
     left_join(BP_opp_over40, by= c("Fcode", "Month"))%>%  
     mutate(`%BPopp_over40`=`BP_Opp_over40`/`BP_Opp`)%>%
     left_join(BPopp_low, by= c("Fcode", "Month"))%>%
     left_join(BPopp_veryhigh, by= c("Fcode", "Month"))%>%
     left_join(BPopp_high, by= c("Fcode", "Month"))%>%
     left_join(BPopp_refer, by= c("Fcode", "Month"))%>%
     left_join(BPopp_normal, by= c("Fcode", "Month"))%>%
     left_join(BPopp_other, by= c("Fcode", "Month"))%>%
     left_join(BP_ref, by= c("Fcode", "Month"))%>%
     left_join(BP_ref_over40, by= c("Fcode", "Month"))%>%  
     mutate(`%BPref_over40`=`BP_ref_over40`/`BP_ref`)%>%
     left_join(BPref_low, by= c("Fcode", "Month"))%>%
     left_join(BPref_veryhigh, by= c("Fcode", "Month"))%>%
     left_join(BPref_high, by= c("Fcode", "Month"))%>%
     left_join(BPref_refer, by= c("Fcode", "Month"))%>%
     left_join(BPref_normal, by= c("Fcode", "Month"))%>%
     left_join(BPref_other, by= c("Fcode", "Month"))%>%
     left_join(ABPM_opp, by= c("Fcode", "Month"))%>%
     left_join(ABPMopp_low, by= c("Fcode", "Month"))%>%
     left_join(ABPMopp_normal, by= c("Fcode", "Month"))%>%
     left_join(ABPMopp_stage1, by= c("Fcode", "Month"))%>%
     left_join(ABPMopp_stage2, by= c("Fcode", "Month"))%>%
     left_join(ABPMopp_severe, by= c("Fcode", "Month"))%>%
     left_join(ABPMopp_other, by= c("Fcode", "Month"))%>%
     left_join(ABPM_ref, by= c("Fcode", "Month"))%>%
     left_join(ABPMref_low, by= c("Fcode", "Month"))%>%
     left_join(ABPMref_normal, by= c("Fcode", "Month"))%>%
     left_join(ABPMref_stage1, by= c("Fcode", "Month"))%>%
     left_join(ABPMref_stage2, by= c("Fcode", "Month"))%>%
     left_join(ABPMref_severe, by= c("Fcode", "Month"))%>%
     left_join(ABPMref_other, by= c("Fcode", "Month"))%>%
     left_join(oppBP_FU, by= c("Fcode", "Month"))%>%
     left_join(refBP_FU, by= c("Fcode", "Month"))%>%
     collect()
   
   #check if TP service payment lower and higher requirements are met
   master$`5BP?`<- ifelse(rowSums(master[, c("BP_Opp", "BP_ref")], na.rm=TRUE)>=5, TRUE, FALSE)
   master$`5BP+1ABPM`<- ifelse(rowSums(master[, c("BP_Opp", "BP_ref")], na.rm=TRUE)>=5 & rowSums(master[,c("abpm_Opp", "abpm_ref")], na.rm=TRUE)>=1, TRUE,FALSE)
   master$`Total_checks`<- rowSums(master[, c("BP_Opp", "BP_ref", "abpm_Opp", "abpm_ref")],na.rm=T)
   
   master
 }
 
 
 #Aggregate at stp level for master data table
 get_stp_master <- function(){
   Allcheck <- cvd %>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     summarise(`Total_patients`=n(), `Avg_patient_age`=mean(`patient_age`), `min_age`=min(`patient_age`),  `max_age`=max(`patient_age`), `no_pharm`= n_distinct(`disp_code`) )%>%
     select(`Month`, `STP`, `STP_Name`, `no_pharm`, `Total_patients`, `min_age`, `Avg_patient_age`,`max_age`)%>%
     collect()
   Allcheck<- distinct(Allcheck)
   
   BP_opp <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE)%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     summarise(`BP_Opp`=n(), `Avg_BPopp_age`=mean(`patient_age`))%>%
     select(`Month`, `STP`, `STP_Name`, `BP_Opp`, `Avg_BPopp_age`)%>%
     collect()
   BP_opp<-distinct(BP_opp)
   
   BP_opp_over40 <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `patient_age`>= 40)%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     summarise(`BP_Opp_over40`=n())%>%
     select(`Month`, `STP`, `STP_Name`, `BP_Opp_over40`)%>%
     collect()
   BP_opp_over40<-distinct(BP_opp_over40)
   
   ABPM_opp <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE)%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`abpm_Opp`=n(), `Avg_ABPMopp_age`=mean(`patient_age`))%>%
     select(`Month`, `STP`, `STP_Name`, `abpm_Opp`, `Avg_ABPMopp_age`)%>%
     collect()
   ABPM_opp<-distinct(ABPM_opp)
   
   BP_ref <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`== TRUE)%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BP_ref`=n(), `Avg_BPref_age`=mean(`patient_age`))%>%
     select(`Month`, `STP`, `STP_Name`, `BP_ref`, `Avg_BPref_age`)%>%
     collect()
   BP_ref<-distinct(BP_ref)
   
   BP_ref_over40 <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`== TRUE, `patient_age`>= 40)%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BP_ref_over40`=n())%>%
     select(`Month`, `STP`, `STP_Name`, `BP_ref_over40`)%>%
     collect()
   BP_ref_over40<-distinct(BP_ref_over40)
   
   ABPM_ref <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE)%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`abpm_ref`=n(), `Avg_ABPMref_age`=mean(`patient_age`))%>%
     select(`Month`, `STP`, `STP_Name`, `abpm_ref`, `Avg_ABPMref_age`)%>%
     collect()
   ABPM_ref<-distinct(ABPM_ref)
   
   BPopp_veryhigh <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Very high")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPopp_veryhigh`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPopp_veryhigh`)%>%
     collect()
   BPopp_veryhigh<-distinct(BPopp_veryhigh)
   
   BPopp_low <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Low")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPopp_low`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPopp_low`)%>%
     collect()
   BPopp_low<-distinct(BPopp_low)
   
   BPopp_high <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`=="High")%>%
      group_by(`Month`,`STP`, `STP_Name`)%>%
      mutate(`BPopp_high`=max(row_number()))%>%
      select(`Month`, `STP`, `STP_Name`, `BPopp_high`)%>%
      collect()
   BPopp_high<-distinct(BPopp_high)
   
   BPopp_refer <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPopp_refer`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPopp_refer`)%>%
     collect()
   BPopp_refer<-distinct(BPopp_refer)
   
   BPopp_normal <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Normal")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPopp_normal`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPopp_normal`)%>%
     collect()
   BPopp_normal<-distinct(BPopp_normal)
   
   BPopp_other <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Other")%>%
      group_by(`Month`,`STP`, `STP_Name`)%>%
      mutate(`BPopp_other`=max(row_number()))%>%
      select(`Month`, `STP`, `STP_Name`, `BPopp_other`)%>%
      collect()
   BPopp_other<-distinct(BPopp_other)
   
   
   BPref_veryhigh <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Very high")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPref_veryhigh`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPref_veryhigh`)%>%
     collect()
   BPref_veryhigh<-distinct(BPref_veryhigh)
   
   BPref_low <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Low")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPref_low`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPref_low`)%>%
     collect()
   BPref_low<-distinct(BPref_low)
   
   BPref_high <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE, `bp_result`=="High")%>%
      group_by(`Month`,`STP`, `STP_Name`)%>%
      mutate(`BPref_high`=max(row_number()))%>%
      select(`Month`, `STP`, `STP_Name`, `BPref_high`)%>%
      collect()
   BPref_high<-distinct(BPref_high)
   
   BPref_refer <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPref_refer`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPref_refer`)%>%
     collect()
   BPref_refer<-distinct(BPref_refer)
   
   BPref_normal <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Normal")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`BPref_normal`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `BPref_normal`)%>%
     collect()
   BPref_normal<-distinct(BPref_normal)
   
   BPref_other <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE, `bp_result`== "Other")%>%
      group_by(`Month`,`STP`, `STP_Name`)%>%
      mutate(`BPref_other`=max(row_number()))%>%
      select(`Month`, `STP`, `STP_Name`, `BPref_other`)%>%
      collect()
   BPref_other<-distinct(BPref_other)
   
   ABPMopp_low <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Low")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMopp_low`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMopp_low`)%>%
     collect()
   ABPMopp_low<-distinct(ABPMopp_low)
   
   ABPMopp_normal <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Normal")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMopp_normal`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMopp_normal`)%>%
     collect()
   ABPMopp_normal<-distinct(ABPMopp_normal)
   
   ABPMopp_stage1 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 1")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMopp_Stage1`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMopp_Stage1`)%>%
     collect()
   ABPMopp_stage1<-distinct(ABPMopp_stage1)
   
   ABPMopp_stage2 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 2")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMopp_Stage2`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMopp_Stage2`)%>%
     collect()
   ABPMopp_stage2<-distinct(ABPMopp_stage2)
   
   ABPMopp_severe <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Potential severe hypertension")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMopp_severe`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMopp_severe`)%>%
     collect()
   ABPMopp_severe<-distinct(ABPMopp_severe)

   ABPMopp_other <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Other")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMopp_other`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMopp_other`)%>%
     collect()
   ABPMopp_other<-distinct(ABPMopp_other)
   
   ABPMref_low <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Low")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMref_low`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMref_low`)%>%
     collect()
   ABPMref_low<-distinct(ABPMref_low)
   
   ABPMref_normal <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Normal")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMref_normal`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMref_normal`)%>%
     collect()
   ABPMref_normal<-distinct(ABPMref_normal)
   
   ABPMref_stage1 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 1")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMref_Stage1`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMref_Stage1`)%>%
     collect()
   ABPMref_stage1<-distinct(ABPMref_stage1)
   
   ABPMref_stage2 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 2")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMref_Stage2`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMref_Stage2`)%>%
     collect()
   ABPMref_stage2<-distinct(ABPMref_stage2)
   
   ABPMref_severe <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Potential severe hypertension")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMref_severe`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMref_severe`)%>%
     collect()
   ABPMref_severe<-distinct(ABPMref_severe)
   
   ABPMref_other <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Other")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`ABPMref_other`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `ABPMref_other`)%>%
     collect()
   ABPMref_other<-distinct(ABPMref_other)
   
   oppBP_FU <-cvd%>%
     filter(`abpm_fu` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High"| `bp_result`== "Very high")%>%
   group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`oppBP_FU`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `oppBP_FU`)%>%
     collect()
   oppBP_FU<-distinct(oppBP_FU)
   
   refBP_FU <-cvd%>%
     filter(`abpm_fu_ref` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High"| `bp_result`== "Very high")%>%
     group_by(`Month`,`STP`, `STP_Name`)%>%
     mutate(`refBP_FU`=max(row_number()))%>%
     select(`Month`, `STP`, `STP_Name`, `refBP_FU`)%>%
     collect()
   refBP_FU<-distinct(refBP_FU)
   
   master <- Allcheck%>%
     mutate(`Total_checks`= 0)%>%
     left_join(BP_opp, by= c("STP", "STP_Name", "Month"))%>% 
     left_join(BP_opp_over40, by= c("STP", "STP_Name", "Month"))%>%  
     mutate(`%BPopp_over40`=`BP_Opp_over40`/`BP_Opp`)%>%
     left_join(BPopp_low, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPopp_veryhigh, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPopp_high, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPopp_refer, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPopp_normal, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPopp_other, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BP_ref, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BP_ref_over40, by= c("STP", "STP_Name", "Month"))%>%  
     mutate(`%BPref_over40`=`BP_ref_over40`/`BP_ref`)%>%
     left_join(BPref_low, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPref_veryhigh, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPref_high, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPref_refer, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPref_normal, by= c("STP", "STP_Name", "Month"))%>%
     left_join(BPref_other, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPM_opp, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMopp_low, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMopp_normal, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMopp_stage1, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMopp_stage2, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMopp_severe, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMopp_other, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPM_ref, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMref_low, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMref_normal, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMref_stage1, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMref_stage2, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMref_severe, by= c("STP", "STP_Name", "Month"))%>%
     left_join(ABPMref_other, by= c("STP", "STP_Name", "Month"))%>%
     left_join(oppBP_FU, by= c("STP", "STP_Name", "Month"))%>%
     left_join(refBP_FU, by= c("STP", "STP_Name", "Month"))%>%
     collect()
   
   #check if TP service payment lower and higher requirements are met
   master$`Total_checks`<- rowSums(master[, c("BP_Opp", "BP_ref", "abpm_Opp", "abpm_ref")],na.rm=T)
  
   master
 }
 
 
 #Aggregate at regional level for master data table
 get_regional_master <- function(){
   Allcheck <- cvd %>%
     group_by(`Month`,`Region_Name`)%>%
     summarise(`Total_patients`=n(), `Avg_patient_age`=mean(`patient_age`), `min_age`=min(`patient_age`),  `max_age`=max(`patient_age`), `no_pharm`= n_distinct(`disp_code`))%>%
     select(`Month`, `Region_Name`,`no_pharm`, `Total_patients`, `min_age`, `Avg_patient_age`,`max_age`)%>%
     collect()
   Allcheck<- distinct(Allcheck)
   
   BP_opp <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE)%>%
     group_by(`Month`,`Region_Name`)%>%
     summarise(`BP_Opp`=n(), `Avg_BPopp_age`=mean(`patient_age`))%>%
     select(`Month`, `Region_Name`, `BP_Opp`, `Avg_BPopp_age`)%>%
     collect()
   BP_opp<-distinct(BP_opp)
   
   BP_opp_over40 <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `patient_age`>= 40)%>%
     group_by(`Month`,`Region_Name`)%>%
     summarise(`BP_Opp_over40`=n())%>%
     select(`Month`, `Region_Name`, `BP_Opp_over40`)%>%
     collect()
   BP_opp_over40<-distinct(BP_opp_over40)

   ABPM_opp <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE)%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`abpm_Opp`=n(), `Avg_ABPMopp_age`=mean(`patient_age`))%>%
     select(`Month`, `Region_Name`, `abpm_Opp`, `Avg_ABPMopp_age`)%>%
     collect()
   ABPM_opp<-distinct(ABPM_opp)
   
   BP_ref <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`== TRUE)%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BP_ref`=n(), `Avg_BPref_age`=mean(`patient_age`))%>%
     select(`Month`, `Region_Name`, `BP_ref`, `Avg_BPref_age`)%>%
     collect()
   BP_ref<-distinct(BP_ref)
   
   BP_ref_over40 <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`== TRUE, `patient_age`>= 40)%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BP_ref_over40`=n())%>%
     select(`Month`, `Region_Name`, `BP_ref_over40`)%>%
     collect()
   BP_ref_over40<-distinct(BP_ref_over40)
   
   ABPM_ref <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE)%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`abpm_ref`=n(), `Avg_ABPMref_age`=mean(`patient_age`))%>%
     select(`Month`, `Region_Name`, `abpm_ref`, `Avg_ABPMref_age`)%>%
     collect()
   ABPM_ref<-distinct(ABPM_ref)
   
   BPopp_veryhigh <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Very high")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPopp_veryhigh`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPopp_veryhigh`)%>%
     collect()
   BPopp_veryhigh<-distinct(BPopp_veryhigh)
   
   BPopp_low <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Low")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPopp_low`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPopp_low`)%>%
     collect()
   BPopp_low<-distinct(BPopp_low)
   
   BPopp_high <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`=="High")%>%
      group_by(`Month`,`Region_Name`)%>%
      mutate(`BPopp_high`=max(row_number()))%>%
      select(`Month`, `Region_Name`, `BPopp_high`)%>%
      collect()
   BPopp_high<-distinct(BPopp_high)
   
   BPopp_refer <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPopp_refer`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPopp_refer`)%>%
     collect()
   BPopp_refer<-distinct(BPopp_refer)
   
   BPopp_normal <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Normal")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPopp_normal`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPopp_normal`)%>%
     collect()
   BPopp_normal<-distinct(BPopp_normal)
   
   BPopp_other <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `bp_result`== "Other")%>%
      group_by(`Month`,`Region_Name`)%>%
      mutate(`BPopp_other`=max(row_number()))%>%
      select(`Month`, `Region_Name`, `BPopp_other`)%>%
      collect()
   BPopp_other<-distinct(BPopp_other)
   

   BPref_veryhigh <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Very high")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPref_veryhigh`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPref_veryhigh`)%>%
     collect()
   BPref_veryhigh<-distinct(BPref_veryhigh)
   
   BPref_low <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Low")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPref_low`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPref_low`)%>%
     collect()
   BPref_low<-distinct(BPref_low)
   
   BPref_high <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`=="High")%>%
      group_by(`Month`,`Region_Name`)%>%
      mutate(`BPref_high`=max(row_number()))%>%
      select(`Month`, `Region_Name`, `BPref_high`)%>%
      collect()
   BPref_high<-distinct(BPref_high)
   
   BPref_refer <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPref_refer`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPref_refer`)%>%
     collect()
   BPref_refer<-distinct(BPref_refer)
   
   BPref_normal <-cvd%>%
     filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Normal")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`BPref_normal`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `BPref_normal`)%>%
     collect()
   BPref_normal<-distinct(BPref_normal)
   
   BPref_other <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE, `bp_result`== "Other")%>%
      group_by(`Month`,`Region_Name`)%>%
      mutate(`BPref_other`=max(row_number()))%>%
      select(`Month`, `Region_Name`, `BPref_other`)%>%
      collect()
   BPref_other<-distinct(BPref_other)
   
   ABPMopp_low <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Low")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMopp_low`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMopp_low`)%>%
     collect()
   ABPMopp_low<-distinct(ABPMopp_low)
   
   ABPMopp_normal <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Normal")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMopp_normal`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMopp_normal`)%>%
     collect()
   ABPMopp_normal<-distinct(ABPMopp_normal)
   
   ABPMopp_stage1 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 1")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMopp_Stage1`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMopp_Stage1`)%>%
     collect()
   ABPMopp_stage1<-distinct(ABPMopp_stage1)
   
   ABPMopp_stage2 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 2")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMopp_Stage2`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMopp_Stage2`)%>%
     collect()
   ABPMopp_stage2<-distinct(ABPMopp_stage2)
   
   ABPMopp_severe <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Potential severe hypertension")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMopp_severe`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMopp_severe`)%>%
     collect()
   ABPMopp_severe<-distinct(ABPMopp_severe)
   
   ABPMopp_other <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Other")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMopp_other`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMopp_other`)%>%
     collect()
   ABPMopp_other<-distinct(ABPMopp_other)
   
   ABPMref_low <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Low")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMref_low`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMref_low`)%>%
     collect()
   ABPMref_low<-distinct(ABPMref_low)
   
   ABPMref_normal <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Normal")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMref_normal`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMref_normal`)%>%
     collect()
   ABPMref_normal<-distinct(ABPMref_normal)
   
   ABPMref_stage1 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 1")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMref_Stage1`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMref_Stage1`)%>%
     collect()
   ABPMref_stage1<-distinct(ABPMref_stage1)
   
   ABPMref_stage2 <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 2")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMref_Stage2`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMref_Stage2`)%>%
     collect()
   ABPMref_stage2<-distinct(ABPMref_stage2)
   
   
   ABPMref_severe <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Potential severe hypertension")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMref_severe`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMref_severe`)%>%
     collect()
   ABPMref_severe<-distinct(ABPMref_severe)
   
   ABPMref_other <-cvd%>%
     filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Other")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`ABPMref_other`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `ABPMref_other`)%>%
     collect()
   ABPMref_other<-distinct(ABPMref_other)
   
   oppBP_FU <-cvd%>%
     filter(`abpm_fu` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High")%>%
   group_by(`Month`,`Region_Name`)%>%
     mutate(`oppBP_FU`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `oppBP_FU`)%>%
     collect()
   oppBP_FU<-distinct(oppBP_FU)
   
   refBP_FU <-cvd%>%
     filter(`abpm_fu_ref` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High")%>%
     group_by(`Month`,`Region_Name`)%>%
     mutate(`refBP_FU`=max(row_number()))%>%
     select(`Month`, `Region_Name`, `refBP_FU`)%>%
     collect()
   refBP_FU<-distinct(refBP_FU)
   
   master <- Allcheck%>%
     mutate(`Total_checks`= 0)%>%
     left_join(BP_opp, by= c("Region_Name", "Month"))%>% 
     left_join(BP_opp_over40, by= c("Region_Name", "Month"))%>%  
     mutate(`%BPopp_over40`=`BP_Opp_over40`/`BP_Opp`)%>%
     left_join(BPopp_low, by= c("Region_Name", "Month"))%>%
     left_join(BPopp_veryhigh, by= c("Region_Name", "Month"))%>%
     left_join(BPopp_high, by= c("Region_Name", "Month"))%>%
     left_join(BPopp_refer, by= c("Region_Name", "Month"))%>%
     left_join(BPopp_normal, by= c("Region_Name", "Month"))%>%
     left_join(BPopp_other, by= c("Region_Name", "Month"))%>%
     left_join(BP_ref, by= c("Region_Name", "Month"))%>%
     left_join(BP_ref_over40, by= c("Region_Name", "Month"))%>%  
     mutate(`%BPref_over40`=`BP_ref_over40`/`BP_ref`)%>%
     left_join(BPref_low, by= c("Region_Name", "Month"))%>%
     left_join(BPref_veryhigh, by= c("Region_Name", "Month"))%>%
     left_join(BPref_high, by= c("Region_Name", "Month"))%>%
     left_join(BPref_refer, by= c("Region_Name", "Month"))%>%
     left_join(BPref_normal, by= c("Region_Name", "Month"))%>%
     left_join(BPref_other, by= c("Region_Name", "Month"))%>%
     left_join(ABPM_opp, by= c("Region_Name", "Month"))%>%
     left_join(ABPMopp_low, by= c("Region_Name", "Month"))%>%
     left_join(ABPMopp_normal, by= c("Region_Name", "Month"))%>%
     left_join(ABPMopp_stage1, by= c("Region_Name", "Month"))%>%
     left_join(ABPMopp_stage2, by= c("Region_Name", "Month"))%>%
     left_join(ABPMopp_severe, by= c("Region_Name", "Month"))%>%
     left_join(ABPMopp_other, by= c("Region_Name", "Month"))%>%
     left_join(ABPM_ref, by= c("Region_Name", "Month"))%>%
     left_join(ABPMref_low, by= c("Region_Name", "Month"))%>%
     left_join(ABPMref_normal, by= c("Region_Name", "Month"))%>%
     left_join(ABPMref_stage1, by= c("Region_Name", "Month"))%>%
     left_join(ABPMref_stage2, by= c("Region_Name", "Month"))%>%
     left_join(ABPMref_severe, by= c("Region_Name", "Month"))%>%
     left_join(ABPMref_other, by= c("Region_Name", "Month"))%>%
     left_join(oppBP_FU, by= c("Region_Name", "Month"))%>%
     left_join(refBP_FU, by= c("Region_Name", "Month"))%>%
     collect()
   
   #check if TP service payment lower and higher requirements are met
   master$`Total_checks`<- rowSums(master[, c("BP_Opp", "BP_ref", "abpm_Opp", "abpm_ref")],na.rm=T)
  # master[is.na(master)] <- 0   
   master
 }
 
 #Aggregate at national level for master data table
 get_national_master <- function(){
    Allcheck <- cvd %>%
       group_by(`Month`)%>%
       summarise(`Total_patients`=n(), `Avg_patient_age`=mean(`patient_age`), `min_age`=min(`patient_age`),  `max_age`=max(`patient_age`), `no_pharm`= n_distinct(`disp_code`))%>%
       select(`Month`, `no_pharm`, `Total_patients`, `min_age`, `Avg_patient_age`,`max_age`)%>%
       collect()
    Allcheck<- distinct(Allcheck)
    
    BP_opp <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE)%>%
       group_by(`Month`)%>%
       summarise(`BP_Opp`=n(), `Avg_BPopp_age`=mean(`patient_age`))%>%
       select(`Month`,  `BP_Opp`, `Avg_BPopp_age`)%>%
       collect()
    BP_opp<-distinct(BP_opp)
    
    BP_opp_over40 <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `patient_age`>= 40)%>%
       group_by(`Month`)%>%
       summarise(`BP_Opp_over40`=n())%>%
       select(`Month`,  `BP_Opp_over40`)%>%
       collect()
    BP_opp_over40<-distinct(BP_opp_over40)
    
    ABPM_opp <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`!=TRUE)%>%
       group_by(`Month`)%>%
       mutate(`abpm_Opp`=n(), `Avg_ABPMopp_age`=mean(`patient_age`))%>%
       select(`Month`,  `abpm_Opp`, `Avg_ABPMopp_age`)%>%
       collect()
    ABPM_opp<-distinct(ABPM_opp)
    
    BP_ref <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`== TRUE)%>%
       group_by(`Month`)%>%
       mutate(`BP_ref`=n(), `Avg_BPref_age`=mean(`patient_age`))%>%
       select(`Month`,  `BP_ref`, `Avg_BPref_age`)%>%
       collect()
    BP_ref<-distinct(BP_ref)
    
    BP_ref_over40 <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`== TRUE, `patient_age`>= 40)%>%
       group_by(`Month`)%>%
       mutate(`BP_ref_over40`=n())%>%
       select(`Month`,  `BP_ref_over40`)%>%
       collect()
    BP_ref_over40<-distinct(BP_ref_over40)
    
    ABPM_ref <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`==TRUE)%>%
       group_by(`Month`)%>%
       mutate(`abpm_ref`=n(), `Avg_ABPMref_age`=mean(`patient_age`))%>%
       select(`Month`,  `abpm_ref`, `Avg_ABPMref_age`)%>%
       collect()
    ABPM_ref<-distinct(ABPM_ref)
    
    BPopp_veryhigh <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Very high")%>%
       group_by(`Month`)%>%
       mutate(`BPopp_veryhigh`=max(row_number()))%>%
       select(`Month`,  `BPopp_veryhigh`)%>%
       collect()
    BPopp_veryhigh<-distinct(BPopp_veryhigh)
    
    BPopp_low <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Low")%>%
       group_by(`Month`)%>%
       mutate(`BPopp_low`=max(row_number()))%>%
       select(`Month`,  `BPopp_low`)%>%
       collect()
    BPopp_low<-distinct(BPopp_low)
    
    BPopp_high <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`=="High")%>%
       group_by(`Month`)%>%
       mutate(`BPopp_high`=max(row_number()))%>%
       select(`Month`,  `BPopp_high`)%>%
       collect()
    BPopp_high<-distinct(BPopp_high)
    
    BPopp_refer <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
      group_by(`Month`)%>%
      mutate(`BPopp_refer`=max(row_number()))%>%
      select(`Month`,  `BPopp_refer`)%>%
      collect()
    BPopp_refer<-distinct(BPopp_refer)
    
    BPopp_normal <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE,`bp_result`== "Normal")%>%
      group_by(`Month`)%>%
      mutate(`BPopp_normal`=max(row_number()))%>%
      select(`Month`,  `BPopp_normal`)%>%
      collect()
    BPopp_normal<-distinct(BPopp_normal)
    
    
    BPopp_other <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE, `bp_result`== "Other")%>%
       group_by(`Month`)%>%
       mutate(`BPopp_other`=max(row_number()))%>%
       select(`Month`,  `BPopp_other`)%>%
       collect()
    BPopp_other<-distinct(BPopp_other)
    

    BPref_veryhigh <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Very high")%>%
       group_by(`Month`)%>%
       mutate(`BPref_veryhigh`=max(row_number()))%>%
       select(`Month`,  `BPref_veryhigh`)%>%
       collect()
    BPref_veryhigh<-distinct(BPref_veryhigh)
    
    BPref_low <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Low")%>%
       group_by(`Month`)%>%
       mutate(`BPref_low`=max(row_number()))%>%
       select(`Month`,  `BPref_low`)%>%
       collect()
    BPref_low<-distinct(BPref_low)
    
    BPref_high <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`=="High")%>%
       group_by(`Month`)%>%
       mutate(`BPref_high`=max(row_number()))%>%
       select(`Month`,  `BPref_high`)%>%
       collect()
    BPref_high<-distinct(BPref_high)
    
    BPref_refer <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "refer1"| `bp_result`== "refer2")%>%
      group_by(`Month`)%>%
      mutate(`BPref_refer`=max(row_number()))%>%
      select(`Month`,  `BPref_refer`)%>%
      collect()
    BPref_refer<-distinct(BPref_refer)
    
    BPref_normal <-cvd%>%
      filter(`bp_check`== TRUE, `bp_check_referral`==TRUE,`bp_result`== "Normal")%>%
      group_by(`Month`)%>%
      mutate(`BPref_normal`=max(row_number()))%>%
      select(`Month`,  `BPref_normal`)%>%
      collect()
    BPref_normal<-distinct(BPref_normal)
    
    BPref_other <-cvd%>%
       filter(`bp_check`== TRUE, `bp_check_referral`==TRUE, `bp_result`== "Other")%>%
       group_by(`Month`)%>%
       mutate(`BPref_other`=max(row_number()))%>%
       select(`Month`,  `BPref_other`)%>%
       collect()
    BPref_other<-distinct(BPref_other)
    
    ABPMopp_low <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Low")%>%
       group_by(`Month`)%>%
       mutate(`ABPMopp_low`=max(row_number()))%>%
       select(`Month`,  `ABPMopp_low`)%>%
       collect()
    ABPMopp_low<-distinct(ABPMopp_low)
    
    ABPMopp_normal <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Normal")%>%
       group_by(`Month`)%>%
       mutate(`ABPMopp_normal`=max(row_number()))%>%
       select(`Month`,  `ABPMopp_normal`)%>%
       collect()
    ABPMopp_normal<-distinct(ABPMopp_normal)
    
    ABPMopp_stage1 <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 1")%>%
       group_by(`Month`)%>%
       mutate(`ABPMopp_Stage1`=max(row_number()))%>%
       select(`Month`,  `ABPMopp_Stage1`)%>%
       collect()
    ABPMopp_stage1<-distinct(ABPMopp_stage1)
    
    ABPMopp_stage2 <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Stage 2")%>%
       group_by(`Month`)%>%
       mutate(`ABPMopp_Stage2`=max(row_number()))%>%
       select(`Month`,  `ABPMopp_Stage2`)%>%
       collect()
    ABPMopp_stage2<-distinct(ABPMopp_stage2)
    
    ABPMopp_severe <-cvd%>%
      filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Potential severe hypertension")%>%
      group_by(`Month`)%>%
      mutate(`ABPMopp_severe`=max(row_number()))%>%
      select(`Month`,`ABPMopp_severe`)%>%
      collect()
    ABPMopp_severe<-distinct(ABPMopp_severe)
    
    ABPMopp_other <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`!=TRUE,`abpm_result`== "Other")%>%
       group_by(`Month`)%>%
       mutate(`ABPMopp_other`=max(row_number()))%>%
       select(`Month`,  `ABPMopp_other`)%>%
       collect()
    ABPMopp_other<-distinct(ABPMopp_other)
    
    ABPMref_low <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Low")%>%
       group_by(`Month`)%>%
       mutate(`ABPMref_low`=max(row_number()))%>%
       select(`Month`,  `ABPMref_low`)%>%
       collect()
    ABPMref_low<-distinct(ABPMref_low)
    
    ABPMref_normal <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Normal")%>%
       group_by(`Month`)%>%
       mutate(`ABPMref_normal`=max(row_number()))%>%
       select(`Month`,  `ABPMref_normal`)%>%
       collect()
    ABPMref_normal<-distinct(ABPMref_normal)
    
    ABPMref_stage1 <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 1")%>%
       group_by(`Month`)%>%
       mutate(`ABPMref_Stage1`=max(row_number()))%>%
       select(`Month`,  `ABPMref_Stage1`)%>%
       collect()
    ABPMref_stage1<-distinct(ABPMref_stage1)
    
    ABPMref_stage2 <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Stage 2")%>%
       group_by(`Month`)%>%
       mutate(`ABPMref_Stage2`=max(row_number()))%>%
       select(`Month`,  `ABPMref_Stage2`)%>%
       collect()
    ABPMref_stage2<-distinct(ABPMref_stage2)
    
    ABPMref_severe <-cvd%>%
      filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Potential severe hypertension")%>%
      group_by(`Month`)%>%
      mutate(`ABPMref_severe`=max(row_number()))%>%
      select(`Month`,`ABPMref_severe`)%>%
      collect()
    ABPMref_severe<-distinct(ABPMref_severe)
    
    ABPMref_other <-cvd%>%
       filter(`abpm`== TRUE, `abpm_referral`==TRUE,`abpm_result`== "Other")%>%
       group_by(`Month`)%>%
       mutate(`ABPMref_other`=max(row_number()))%>%
       select(`Month`,  `ABPMref_other`)%>%
       collect()
    ABPMref_other<-distinct(ABPMref_other)
    
    oppBP_FU <-cvd%>%
      filter(`abpm_fu` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High")%>%
    group_by(`Month`)%>%
      mutate(`oppBP_FU`=max(row_number()))%>%
      select(`Month`, `oppBP_FU`)%>%
      collect()
    oppBP_FU<-distinct(oppBP_FU)
    
    refBP_FU <-cvd%>%
      filter(`abpm_fu_ref` == TRUE, `bp_result`== "refer1"| `bp_result`== "refer2"|`bp_result`== "High")%>%
      group_by(`Month`)%>%
      mutate(`refBP_FU`=max(row_number()))%>%
      select(`Month`, `refBP_FU`)%>%
      collect()
    refBP_FU<-distinct(refBP_FU)
    
    master <- Allcheck%>%
       mutate(`Total_checks`= 0)%>%
       left_join(BP_opp, by= c( "Month"))%>% 
       left_join(BP_opp_over40, by= c( "Month"))%>%  
       mutate(`%BPopp_over40`=`BP_Opp_over40`/`BP_Opp`)%>%
       left_join(BPopp_low, by= c( "Month"))%>%
       left_join(BPopp_veryhigh, by= c( "Month"))%>%
       left_join(BPopp_high, by= c( "Month"))%>%
       left_join(BPopp_refer, by= c( "Month"))%>%
       left_join(BPopp_normal, by= c( "Month"))%>%
       left_join(BPopp_other, by= c( "Month"))%>%
       left_join(BP_ref, by= c( "Month"))%>%
       left_join(BP_ref_over40, by= c( "Month"))%>%  
       mutate(`%BPref_over40`=`BP_ref_over40`/`BP_ref`)%>%
       left_join(BPref_low, by= c( "Month"))%>%
       left_join(BPref_veryhigh, by= c( "Month"))%>%
       left_join(BPref_high, by= c( "Month"))%>%
       left_join(BPref_refer, by= c( "Month"))%>%
       left_join(BPref_normal, by= c( "Month"))%>% 
       left_join(BPref_other, by= c( "Month"))%>%
       left_join(ABPM_opp, by= c( "Month"))%>%
       left_join(ABPMopp_low, by= c( "Month"))%>%
       left_join(ABPMopp_normal, by= c( "Month"))%>%
       left_join(ABPMopp_stage1, by= c( "Month"))%>%
       left_join(ABPMopp_stage2, by= c( "Month"))%>%
       left_join(ABPMopp_severe, by= c( "Month"))%>%
       left_join(ABPMopp_other, by= c( "Month"))%>%
       left_join(ABPM_ref, by= c( "Month"))%>%
       left_join(ABPMref_low, by= c( "Month"))%>%
       left_join(ABPMref_normal, by= c( "Month"))%>%
       left_join(ABPMref_stage1, by= c( "Month"))%>%
       left_join(ABPMref_stage2, by= c( "Month"))%>%
      left_join(ABPMref_severe, by= c( "Month"))%>% 
       left_join(ABPMref_other, by= c( "Month"))%>%
      left_join(oppBP_FU, by= c( "Month"))%>%
      left_join(refBP_FU, by= c( "Month"))%>%
       collect()
    
    #check if TP service payment lower and higher requirements are met
 master$`Total_checks`<- rowSums(master[, c("BP_Opp", "BP_ref", "abpm_Opp", "abpm_ref")],na.rm=T)
 master[is.na(master)] <- 0   
    master
 }


 bsa_claims <- function(){

    claims<-pull_bsa()%>% 
       mutate(Month=as.Date(`Month(claim)`, "%Y-%m-%d"),
              Fcode=`Pharmacy Code`, 
              no_patient=as.numeric(`Number of patients`, na.rm= T), 
              no_bp=ifelse(is.na(`Number of Clinic Blood Pressure checks (including LPS pharmacies)`), `Number of Clinic Blood Pressure checks (exc. LPS)`, `Number of Clinic Blood Pressure checks (including LPS pharmacies)`),
              no_abpm=ifelse(is.na(`Number of Ambulatory Blood Pressure Monitoring (including LPS pharmacies)`), `Number of Ambulatory Blood Pressure Monitoring (ABPM) (exc. LPS)`, `Number of Ambulatory Blood Pressure Monitoring (including LPS pharmacies)`))%>%
       select(Month, Fcode, no_patient,  no_bp, no_abpm)%>%
       collect()
    
    claims
    }
 
pop<- function(){
   
   #pop<-read.csv("../data/gp-reg-pat-prac-sing-age-regions.csv")
  
  sql=" select *
  FROM [CommunityPharmacy_Public].[z_gp-reg-pat-prac-single-age-regions]
  where [ORG_TYPE]='Comm Region'"
  result<-dbSendQuery(con,sql)
  pop<-dbFetch(result)
  
  
  pop$age_grp = NA
  under20 = pop$`AGE`<20
  to39 = pop$`AGE`>=20 & pop$`AGE`<=39
  to59 = pop$`AGE`>=40 & pop$`AGE`<=59
  to79 = pop$`AGE`>=60 & pop$`AGE`<=79
  over80 = pop$`AGE`>=80
  pop$age_grp[under20] = "Under 20"
  pop$age_grp[to39] = "20-39"
  pop$age_grp[to59] = "40-59"
  pop$age_grp[to79] = "60-79"
  pop$age_grp[over80] = "Over 80"
  
  pop1<- pop%>%
    mutate(Month=as.Date(`EXTRACT_DATE`, "%Y-%m-%d"), Region_Code=ORG_CODE)%>%
    group_by(Month, Region_Code,age_grp )%>%
    summarise(total=sum(`NUMBER_OF_PATIENTS`, na.rm=T))%>% 
    filter(!is.na(age_grp))%>%collect()
   
      
   
   pop2<-pop1%>%
      group_by(age_grp, Month)%>%
      summarise(total=sum(total, na.rm=T))%>%
      mutate(Region_Code="National")%>%
    collect()
   
   pop<-rbind(pop1,pop2)  
   
   pop<- pop%>%
      mutate(`Region_Name` = case_when(Region_Code == 'Y62' ~"North West",
                            Region_Code =='Y63' ~"North East And Yorkshire",
                            Region_Code =='Y60' ~"Midlands",
                            Region_Code =='Y61' ~"East Of England",
                            Region_Code =='Y56' ~"London",
                            Region_Code =='Y58' ~"South West",
                            Region_Code =='Y59' ~"South East",
                            Region_Code == 'National' ~ "National"))
      
   pop
}

dict<-function(){
  data<- read.csv("~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/CVD pack data dictionary.csv")
  data
}


 #################return master data for each level-----------------
  cvd <- record_master()
  pharmacy_master <-get_pharmacy_master()
  claims_prep<-pull_CVDclaims_1()
  CVDclaims <- pull_CVDclaims_2()
  incent<-pull_CVDclaims_3()
  stp_master <-get_stp_master()
  regional_master <- get_regional_master()
  national_master <-get_national_master()
  
  Date_stamp <- get_download_timestamp()
  pop<-pop()
  bsa<- bsa_claims()

  
  dictionary<- dict()
  
