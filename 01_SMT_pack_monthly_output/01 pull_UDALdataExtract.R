library(DBI)
library (odbc)
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
pull_Master1 <- function(){
 
  ######## Pharmacy Dashboard master table ----
  sql=" 
SELECT distinct 
[Month]
  ,[ContractorCode] = [Contractor Code]
  ,[Measure]
  ,[Figure]
  FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
  where [Measure] in ('--Medicine Use Review and Prescription Intervention Service (MUR) Activity',
                      'Appliance Use Reviews (AUR) Activity',
                      'Items Dispensed',
                      'Covid Vaccination Service Activity',
                      'Hepatitis C Antibody Testing Service Activity',
                      'New Medicine Service (NMS) Activity',
                      -- 'Community Pharmacist Consultation Service (CPCS) Activity',
                      'Stoma Customisation (STOMA) Activity') 
                      and [Figure]>0
   "
  
  result<-dbSendQuery(con,sql)
  Master1<-dbFetch(result)
  dbClearResult(result)
  
  Master1
  
}



################################################################################
pull_Contractor <- function(){
  ######## Pharmacy contractor table ----
  sql=" 
SELECT [ContractorCode]
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
      ,[ICB_Name]
      ,[EndDate]
  FROM [CommunityPharmacy_Restricted].[Ref_Contractor]
    --where [EndDate] = '1900-01-01 00:00:00.000' or [EndDate] is NULL
"
  result<-dbSendQuery(con,sql)
  Contractor<-dbFetch(result)
  dbClearResult(result)
  
  #clean co-lcoated and ContractType columns in Contractor table
  Contractor$`CoLocated_withGP`<- ifelse(is.na(Contractor$`CoLocated_withGP`), FALSE, Contractor$`CoLocated_withGP`)
  Contractor["ContractType"][Contractor["ContractType"]== "Community Pharmacy" | Contractor["ContractType"]== "Pharmacy"] <- "Community"
  Contractor["Pharmacy_100hour"][Contractor["Pharmacy_100hour"]== "NO"] <- "No"
  Contractor$`Permanently_closed`= "Yes"
  Contractor["Permanently_closed"][Contractor["EndDate"]== "1900-01-01 00:00:00.000" | is.na(Contractor$EndDate)] <- "No"
  
  Contractor
}

################################################################################
pull_flu_national <- function(){
  #national total for each flu season
  sql= "SELECT  [Month]
      ,[Contractor Code] as [Contractor]
 
     ,[Figure] as [Flu_activity]
	 ,case 
when (Month <='2015-03-01' ) then 'before 2015'
when (Month> = '2015-04-01' and Month <='2016-03-01' ) then '2015/16'
when (Month> = '2016-04-01' and Month <='2017-03-01' ) then '2016/17'
when (Month> = '2017-04-01' and Month <='2018-03-01' ) then '2017/18'
when (Month> = '2018-04-01' and Month <='2019-03-01' ) then '2018/19'
when (Month> = '2019-04-01' and Month <='2020-03-01' ) then '2019/20'
when (Month> = '2020-04-01' and Month <='2021-03-01' ) then '2020/21'
when (Month> = '2021-04-01' and Month <='2022-03-01' ) then '2021/22'
when (Month> = '2022-04-01' and Month <='2023-03-01' ) then '2022/23'
when (Month> = '2023-04-01' and Month <='2024-03-01' ) then '2023/24'
when (Month> = '2024-04-01' and Month <='2025-03-01' ) then '2024/25'
when (Month> = '2025-04-01' and Month <='2026-03-01' ) then '2025/26'
else 'After 2025/26'
end as [Flu_season]
  FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
  where [Measure]= 'Seasonal Influenza Vaccination Advances Service (FLU) Activity'
 "
  result<-dbSendQuery(con,sql)
  flu_national<-dbFetch(result)
  dbClearResult(result)
  
  flu_national
}

################################################################################
pull_dose_band <- function(){
  
  #Contractor total for each flu season and assign contractors into dose bands
  #Calculate total number of contractors and total doses in each dose band for each flu season
  sql=" select c.[Flu_season], 
              c.[dose_band], 
              [noPharm]=count(distinct c.[Contractor]),
              [totalDose]= sum(c.[Flu_activity])
from
(select a.[Contractor],a.[Flu_season],a.[Flu_activity],
case
when [Flu_activity] ='0' then '0'
when [Flu_activity] <='50' and [Flu_activity]>'0' then '1-50'
when [Flu_activity] >'50' and [Flu_activity]<='100' then '51-100'
when [Flu_activity] >'100' and [Flu_activity]<='200' then '101-200'
when [Flu_activity] >'200' and [Flu_activity]<='500' then '201-500'
when [Flu_activity] >'500'  then '500+'
end as [dose_band]
from
(select b.[Contractor],b.[Flu_season], [Flu_activity]=sum(b.[Flu_activity])
from 
(SELECT  [Month]
      ,[Contractor Code] as [Contractor]
 
     ,[Figure] as [Flu_activity]
	 ,case 
when (Month <='2015-03-01' ) then 'before 2015'
when (Month> = '2015-04-01' and Month <='2016-03-01' ) then '2015/16'
when (Month> = '2016-04-01' and Month <='2017-03-01' ) then '2016/17'
when (Month> = '2017-04-01' and Month <='2018-03-01' ) then '2017/18'
when (Month> = '2018-04-01' and Month <='2019-03-01' ) then '2018/19'
when (Month> = '2019-04-01' and Month <='2020-03-01' ) then '2019/20'
when (Month> = '2020-04-01' and Month <='2021-03-01' ) then '2020/21'
when (Month> = '2021-04-01' and Month <='2022-03-01' ) then '2021/22'
when (Month> = '2022-04-01' and Month <='2023-03-01' ) then '2022/23'
when (Month> = '2023-04-01' and Month <='2024-03-01' ) then '2023/24'
when (Month> = '2024-04-01' and Month <='2025-03-01' ) then '2024/25'
when (Month> = '2025-04-01' and Month <='2026-03-01' ) then '2025/26'
else 'After 2025/26'
end as [Flu_season]
  FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
  where [Measure]= 'Seasonal Influenza Vaccination Advances Service (FLU) Activity') as b
group by b.[Flu_season],b.[Contractor]) as a
where a.[Flu_activity] > '0') as c

group by c.[Flu_season], c.[dose_band]

"
  
  result<-dbSendQuery(con,sql)
  dose_band<-dbFetch(result)
  dbClearResult(result)
  
  dose_band
}


######### SSP payment ####
pull_ssp <- function(){
  
  sql= "SELECT [Month]
      ,[Contractor],
     [SSP]= ([SSP REMUN]+[SSP REIMB]+[SSP REMUN ADJ]+[SSP REIMB ADJ])
      
  FROM [CommunityPharmacy_Public].[MIS_Pharmacy]"
  
  
  result<-dbSendQuery(con,sql)
  SSP<-dbFetch(result)
  dbClearResult(result)
  
  
  SSP
  
}


################################################################################
pull_FYfund <- function(){
  
  ######## running total funding for CPCF and non-CPCF ----
  sql=" 
select a.FY,a.Measure, sum(Figure) as [Figure]
from
(select [Month],[Measure],
case when [Month] between '2015-04-01' and '2016-03-01' then '2015/16'
     when [Month] between '2016-04-01' and '2017-03-01' then '2016/17'
	 when [Month] between '2017-04-01' and '2018-03-01' then '2017/18'
	 when [Month] between '2018-04-01' and '2019-03-01' then '2018/19'
	 when [Month] between '2019-04-01' and '2020-03-01' then '2019/20'
	 when [Month] between '2020-04-01' and '2021-03-01' then '2020/21'
	 when [Month] between '2021-04-01' and '2022-03-01' then '2021/22'
	 when [Month] between '2022-04-01' and '2023-03-01' then '2022/23'
	when [Month] between '2023-04-01' and '2024-03-01' then '2023/24'
	when [Month] between '2024-04-01' and '2025-03-01' then '2024/25'
		when [Month] between '2025-04-01' and '2026-03-01' then '2025/26'
	 else '' end as [FY],
	 sum(figure) as Figure
FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
where measure in  ('Covid Test Distribution Service Income',
'Covid Vaccination Service Income',
'Seasonal Influenza Vaccination Advances Service (FLU) Income',
'Methadone Payment',
'Stoma Customisation (STOMA) Income',
'New Medicine Service (NMS) Income',
'Appliance Use Reviews (AUR) Income',
'Hepatitis C Antibody Testing Service Income',
'Medicine Use Review and Prescription Intervention Service (MUR) Income',
'CPCS GP Referral Pathway Engagement Income',
'Discharge Medicine Service Income',
'Community Pharmacist Consultation Service (CPCS) Income',
'Hypertension Case-finding Service Setup Fee',
'Hypertension Case-finding Service Income',
'Smoking Cessation Service Set Up fee',
'General Practice Digital Minor Illness Service Pilot Income',
'UEC to CPCS Pilot Income',
 'Tier 1 Ongoing supply of Oral Contraception Pilot Income',
 'Hypertension Case Finding Pilot Income',
 'NHS Smoking Cessation Service Pilot Income',
 'Tier 2 Initiation of Oral Contraception Pilot Income',
 'Smoking Cessation Service Consultations Income',
 'Contraception Service Consultation Income',
 'Contraception Service Set Up fee',
 'NHS Community Pharmacy Independent Prescribing Service Pathfinder Programme',
 'Pharmacy First Initial Fixed Payment',--added20240530
 'Pharmacy First Clinical Pathways Consultation Fee',--added20240530
 'Pharmacy First Clinical Pathways Monthly Fixed Payment',--added20240530
 'Pharmacy First MI & UMS Consultation Fee' -- added20240628
 )  
and [Contractor Type] = 'Pharmacy' 
--and [Figure]>0
group by [Month],[Measure]) a
group by [FY],[Measure]
   "
  
  result<-dbSendQuery(con,sql)
  FYfund<-dbFetch(result)
  dbClearResult(result)
  
  FYfund<-FYfund%>%
    filter(Figure !=0)%>%
    collect()
  
  FYfund
  
}

################################################################################
pull_funding <- function(){
  ######## Monthly total funding since March 2020----
  
  sql="select CPCFservices.Month as Month
, sum(CPCFservices.payment) as CPCFservices
, sum(totalpayment.totalpayment) as totalpayment
  from
  (select [Month], sum(figure) as payment
FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
where measure in ('Stoma Customisation (STOMA) Income',
'New Medicine Service (NMS) Income',
'Appliance Use Reviews (AUR) Income',
'Hepatitis C Antibody Testing Service Income',
'Medicine Use Review and Prescription Intervention Service (MUR) Income',
'CPCS GP Referral Pathway Engagement Income',
'CPCS Setup Income',
'Discharge Medicine Service Income',
'Community Pharmacist Consultation Service (CPCS) Income',
'Hypertension Case-finding Service Setup Fee',
'Hypertension Case-finding Service Income',
'Smoking Cessation Service Set Up fee',
'Smoking Cessation Service Consultations Income',
 'Contraception Service Consultation Income',
 'Contraception Service Set Up fee',
 'Pharmacy First Initial Fixed Payment',--added20240530
 'Pharmacy First Clinical Pathways Consultation Fee',--added20240530
 'Pharmacy First Clinical Pathways Monthly Fixed Payment',--added20240530
 'Pharmacy First MI & UMS Consultation Fee' -- added20240628
 ) 
and [Contractor Type] = 'Pharmacy'
and [Month] > '2020-03-01'
group by [Month]) CPCFservices
  inner join 
  (select  [Month],sum(figure) as totalpayment
FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
where measure in ('Other Dispensing Activity Related Payments',
'Stoma Customisation (STOMA) Income',
'Local enhanced services + local approved payments',
'New Medicine Service (NMS) Income',
'Appliance Use Reviews (AUR) Income',
'Pharmacy Access Scheme',
'Hepatitis C Antibody Testing Service Income',
'Medicine Use Review and Prescription Intervention Service (MUR) Income',
'Dispensing Activity Fee',
'CPCS GP Referral Pathway Engagement Income',
'Covid Test Distribution Service Income',
'Discharge Medicine Service Income',
'Covid Vaccination Service Income',
'Urgent Medicine Supply Advanced Service (NUMSAS) Income',
'Infrastructure Payments',
'Seasonal Influenza Vaccination Advances Service (FLU) Income',
'Methadone Payment',
'Community Pharmacist Consultation Service (CPCS) Income',
'Pharmacy Quality Scheme',
'Hypertension Case-finding Service Setup Fee',
'Hypertension Case-finding Service Income',
'Smoking Cessation Service Set Up fee',
'CPCS Setup Income',
'Smoking Cessation Service Consultations Income',
'General Practice Digital Minor Illness Service Pilot Income',
'UEC to CPCS Pilot Income',
'Covid Test Distribution Service Income',
'Hypertension Case Finding Pilot Income',
'NHS Smoking Cessation Service Pilot Income',
'Tier 1 Ongoing supply of Oral Contraception Pilot Income',
'Other Dispensing Activity Related Payments', 
'Tier 2 Initiation of Oral Contraception Pilot Income',
 'Contraception Service Consultation Income',
 'Contraception Service Set Up fee',
 'NHS Community Pharmacy Independent Prescribing Service Pathfinder Programme',
 'Pharmacy First Initial Fixed Payment',--added20240530
 'Pharmacy First Clinical Pathways Consultation Fee',--added20240530
 'Pharmacy First Clinical Pathways Monthly Fixed Payment',--added20240530
 'Pharmacy First MI & UMS Consultation Fee' -- added20240628
				) 
and [Contractor Type] = 'Pharmacy'
and [Month] > '2020-02-01'
group by [Month]) totalpayment
on CPCFservices.Month = totalpayment.Month
group by CPCFservices.Month    
order by CPCFservices.Month"
  
  result<-dbSendQuery(con,sql)
  funding<-dbFetch(result)
  dbClearResult(result)
  
  funding
}


################################################################################
pull_servicefund <- function(){
  ######## % of total for each CPCF service since March 2020----
  sql="select [Month], [Measure] as [CPCF_service],sum(figure) as [Figure], [Contractor Type]
FROM (select * from [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master] ) a

group by [Measure], [Month], [Contractor Type]
having measure in ('Stoma Customisation (STOMA) Income',
'New Medicine Service (NMS) Income',
'Appliance Use Reviews (AUR) Income',
'Hepatitis C Antibody Testing Service Income',
'Medicine Use Review and Prescription Intervention Service (MUR) Income',
'CPCS GP Referral Pathway Engagement Income',
'Discharge Medicine Service Income',
'Community Pharmacist Consultation Service (CPCS) Income',
'Hypertension Case-finding Service Setup Fee',
'Hypertension Case-finding Service Income',
'Smoking Cessation Service Set Up fee',
'CPCS Setup Income',
'Smoking Cessation Service Consultations Income',
 'Contraception Service Consultation Income',
 'Contraception Service Set Up fee',
 'Pharmacy First Initial Fixed Payment',--added20240530
 'Pharmacy First Clinical Pathways Consultation Fee',--added20240530
 'Pharmacy First Clinical Pathways Monthly Fixed Payment',--added20240530
 'Pharmacy First MI & UMS Consultation Fee' -- added20240628
 ) 
and [Contractor Type] = 'Pharmacy'
and [Month] > '2020-02-01'"
  
  result<-dbSendQuery(con,sql)
  servicefund<-dbFetch(result)
  dbClearResult(result)
  
  servicefund<-servicefund%>%
    filter(Figure!=0)%>%
    collect()
  
  servicefund
}

################################################################################
pull_dac_income <-function(){
  sql="SELECT  [Month]
      ,[ContractorCode]=[Contractor Code]
      ,[Figure],[Measure]
  FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master] 
  where [Measure] in ('Stoma Customisation (STOMA) Income','Appliance Use Reviews (AUR) Income')
  "
  
  result<-dbSendQuery(con,sql)
  dac_income<-dbFetch(result)
  dbClearResult(result)
  
  dac_income<-dac_income%>%
    filter(Figure !=0)%>%
    collect()
  
  dac_income
}

################################################################################
pull_servicevol <- function(){
  ######## % of total for each CPCF service since March 2020----
  sql="select a.FY,a.Measure, sum(Figure) as [Figure], count(distinct [Contractor Code]) as [noPharm]
from
(select [Month],[Measure], [Contractor Code], Figure,
case when [Month] between '2015-04-01' and '2016-03-01' then '2015/16'
     when [Month] between '2016-04-01' and '2017-03-01' then '2016/17'
	 when [Month] between '2017-04-01' and '2018-03-01' then '2017/18'
	 when [Month] between '2018-04-01' and '2019-03-01' then '2018/19'
	 when [Month] between '2019-04-01' and '2020-03-01' then '2019/20'
	 when [Month] between '2020-04-01' and '2021-03-01' then '2020/21'
	 when [Month] between '2021-04-01' and '2022-03-01' then '2021/22'
	 when [Month] between '2022-04-01' and '2023-03-01' then '2022/23'
	 	 when [Month] between '2023-04-01' and '2024-03-01' then '2023/24'
	 	 	when [Month] between '2024-04-01' and '2025-03-01' then '2024/25'
	 	 		 	 	when [Month] between '2025-04-01' and '2026-03-01' then '2025/26'
	 else '' end as [FY]
FROM [CommunityPharmacy_Public].[Pharmacy_Dashboard_Master]
where measure in  (--'No. of Fees for Prescriptions',
'Medicine Use Review and Prescription Intervention Service (MUR) Activity',
'Appliance Use Reviews (AUR) Activity',
'Covid Vaccination Service Activity',
'Seasonal Influenza Vaccination Advances Service (FLU) Activity',
'Hepatitis C Antibody Testing Service Activity',
'New Medicine Service (NMS) Activity',
--'Community Pharmacist Consultation Service (CPCS) Activity',
'Stoma Customisation (STOMA) Activity')  
and [Contractor Type] = 'Pharmacy' and [Figure]>0) a
group by [FY],[Measure]"
  
  result<-dbSendQuery(con,sql)
  servicevol<-dbFetch(result)
  dbClearResult(result)
  
  servicevol
}


################################################################################
pull_cpcs_signup <- function(){
  ######## CPCS sign up fee (needs double check why it's not included in pharmacy master table)----
  sql="SELECT [Month]
,[CPCF_service] = 'CPCS SIGN-UP'
  ,[Figure] = sum([CPCS SIGN-UP])
  FROM (select * from [CommunityPharmacy_Public].[MIS_Pharmacy] 
  ) a
  group by [Month]"
  
  result<-dbSendQuery(con,sql)
  cpcs_signup<-dbFetch(result)
  dbClearResult(result)
  
  cpcs_signup<- cpcs_signup%>%
    group_by(Month, CPCF_service)%>%
    summarise(Figure=sum(Figure, na.rm=T))%>%
    filter(Figure !=0)%>%
    collect()
  
  cpcs_signup
}

pull_cpcs_signup_2<-function(){
  sql="  select distinct  * from
 (select a.FCode from
  (SELECT FCode   
  FROM [CommunityPharmacy_Restricted].[Service_Registrations]
  where Service = 'Community Pharmacy Consultation Service' and 
  DateReported = (select max([CommunityPharmacy_Restricted].[Service_Registrations] 
  where Service = 'Community Pharmacy Consultation Service') ) a
  left join 
  (SELECT [FCode], [deReg]=1
  FROM [CommunityPharmacy_Restricted].[Service_Deregistrations]
  where [Service]= 'Community Pharmacy Consultation Service' and [DateReported]= (select max([DateReported]) FROM [NHSE_Sandbox_DispensingReporting].[dbo].[Service_Deregistrations] where [Service] = 'Community Pharmacy Consultation Service')
  and [ReRegistrationDate] is NULL) b
   on a.[FCode]=b.[FCode] where b.[deReg] is NULL) c
   left join (SELECT distinct [ContractorCode], [inactive] = 1
  FROM [CommunityPharmacy_Public].[Ref_Contractor]
  where [EndDate] is not NULL) p
  on c.FCode=p.[ContractorCode] 
  where p.[inactive] is NULL"
  
  result<-dbSendQuery(con,sql)
  cpcs_signup<-dbFetch(result)
  dbClearResult(result)
  
  cpcs_signup
  
}

# ################################################################################
# pull_provider <- function(){
#   ######## NHS provider fuller list for lookup regions later----
#   sql="select a.*, [Region_Name]=b.NHS_England_Region_Name from
# (SELECT distinct [OrgCode]= [Organisation_Code], Region_Code= National_Grouping_Code FROM NHSE_UKHF.[ODS].[vw_GP_Practices_And_Prescribing_CCs_SCD] 
# 			WHERE [Is_Latest]=1) a
# left join [NHSE_UKHF].[ODS].[vw_NHS_England_Region_Names_And_Codes_SCD] b
# on a.[Region_Code] = b.[NHS_England_Region_Code]"
#   
#   result<-dbSendQuery(con,sql)
#   provider<-dbFetch(result)
#   dbClearResult(result)
#   
#   provider
# }
# # 


################################################################################
pull_trust_total <- function(){
  
  sql= " SELECT
                      [Provider_Code] = [Der_Provider_Code]
                      ,[No_discharge] = count(distinct [Hospital_Spell_No])
                      ,[Discharge_Month]=format(t1.Discharge_Month, 'dd/MM/yyyy') 
                   FROM [Reporting_MESH_APC].[APCE_Core_Union]
                   CROSS APPLY (VALUES(DATEADD(DAY, 1, EOMONTH(Discharge_Date, -1))))t1(Discharge_Month)
                   WHERE Der_Financial_Year IN ('2020/21','2021/22', '2022/23', '2023/24', '2024/25', '2025/26')
                   AND [Discharge_Date] > '2021-01-31' 
                   AND [Discharge_Destination] NOT IN ('79','99','98')  
		               AND Der_Pseudo_NHS_Number IS NOT NULL
		               AND [Patient_Classification] = 1
		               AND [Episode_Number] = '1'
             	GROUP BY
	               	[Der_Provider_Code]
	              	,t1.Discharge_Month "
  
  
  result<-dbSendQuery(con,sql)
  trusttotal<-dbFetch(result)
  dbClearResult(result)
  
  trusttotal   
}

#trust_total<-pull_trust_total()
#trust_total <- mutate(trust_total, `Provider_Code` = case_when(`Provider_Code`=="RDZ" ~ "R0D",
                                                              # TRUE ~ as.character(`Provider_Code`)),
                     # `Provider_Code`=case_when(`Provider_Code`=="RD3"~"R0D",
                                               # TRUE ~ as.character(`Provider_Code`)) )

# pull_trust_lookup <- function(){
#   sql= "SELECT  distinct [Provider_Code] =upper([Organisation_Code])
#        ,[Region_Code] ,[Region]=[Region_Name], [Effective_To], [NHSE_Organisation_Type], [ODS_Organisation_Type]
#        FROM [Reporting_UKHD_ODS].[Provider_Hierarchies]"
# 
#   
#   result<-dbSendQuery(con,sql)
#   trust_lookup<-dbFetch(result)
#   dbClearResult(result)
#   
#   trust_lookup
# }
# 
# 
# pull_discharge <- function(type="Acute"){
#   
#   if(type=="Acute"){
#     discharge<-trust_total%>%
#       left_join(trust_lookup, "Provider_Code")%>%
#       filter(`NHSE_Organisation_Type`== "ACUTE TRUST")%>%
#       group_by(`Discharge_Month`, `Region`)%>%
#       summarise(`total`=sum(No_discharge, na.rm=T))%>%
#       collect()
#   }
#   else if(type=="All"){
#     discharge<-trust_total%>%
#       left_join(trust_lookup, "Provider_Code")%>%
#       group_by(`Discharge_Month`, `Region`)%>%
#       summarise(`total`=sum(No_discharge, na.rm=T))%>%
#       collect()
#   }else{}
#   
#   discharge
# }


################################################################################
pull_CVD <- function(){

  sql= "SELECT [submission_id]
      ,[disp_code]
      ,[part_month]
      ,[declaration_agree]
      ,[patient_age]
      ,[bp_check]
      ,[abpm]
      ,[bp_check_referral]
      ,[abpm_referral]
      ,[systolic_bp_result]
      ,[diastolic_bp_result]
      ,[systolic_abpm_result]
      ,[diastolic_abpm_result]
  FROM [CommunityPharmacy_Restricted].[BloodPressureService_MYS_activity]"
  result<-dbSendQuery(con,sql)
  cvd<-dbFetch(result)
  
  
  cvd$`declaration_agree`<- ifelse(cvd$`declaration_agree`== "true"|cvd$`declaration_agree`== "TRUE"|cvd$`declaration_agree`== "True", TRUE, FALSE)
  cvd$`bp_check`<- ifelse(cvd$`bp_check`== "true"|cvd$`bp_check`== "TRUE"|cvd$`bp_check`== "True", TRUE, FALSE)
  cvd$`abpm`<- ifelse(cvd$`abpm`== "true"|cvd$`abpm`== "TRUE"|cvd$`abpm`== "True", TRUE, FALSE)
  cvd$`bp_check_referral`<- ifelse(cvd$`bp_check_referral`== "true"|cvd$`bp_check_referral`== "TRUE"|cvd$`bp_check_referral`== "True", TRUE, FALSE)
  cvd$`abpm_referral`<- ifelse(cvd$`abpm_referral`== "true"|cvd$`abpm_referral`== "TRUE"|cvd$`abpm_referral`== "True", TRUE, FALSE)
  current <- as.Date(paste(format(Sys.Date(), "%Y-%m"), "-01", sep = ""))
  
  ### extract data from new API -- temp solution simply convert new API data into old format of data frame, so metric calculations in current report do not need to change

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
  
  cvd_all<- cvd_all %>%
    mutate(`Month`= paste0(substr(`part_month`,1,4), "-", substr(`part_month`, 5,6), "-01"))%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"), 
           `Fcode`= `disp_code`, 
           `patient_age` = as.numeric(`patient_age`), 
           `systolic_abpm_result`=as.numeric(`systolic_abpm_result`),
           `diastolic_abpm_result`=as.numeric(`diastolic_abpm_result`))%>%
    filter(`Month`< current)%>%
    collect()
  
  Contractor<- Contractor%>%select(`Fcode`=`ContractorCode`, `Region_Name`,`STP`, `STP_Name`, `IMD_Decile`)%>%collect()
  
  
  master<- cvd_all%>%
    left_join(Contractor,by="Fcode")%>%
    collect()
  
  master
}


################################################################################
pull_CVDclaims_1<-function(){
  sql="select * from
  (SELECT [Month]=[Month(claim)]
      ,[Fcode]=[Pharmacy Code]    
      ,[SetupFee]=[Set up fee]+[Set up fee adjustment]
      ,[HYP_ABPM]=45*[Number of Ambulatory Blood Pressure Monitoring (ABPM) (exc. LPS)]
      ,[BP_FEE]= 15*[Number of Clinic Blood Pressure checks (exc. LPS)]
      ,[HYP_INCTV]=[Incentive fee]+[Incentive fee adjustment]
  FROM [CommunityPharmacy_Public].[BloodPressureService_BSA_claims]
  where [Month(claim)] <='2025-03-01'
  union
  SELECT [Month]=[Month(claim)]
      ,[Fcode]=[Pharmacy Code]    
      ,[SetupFee]=[Set up fee]+[Set up fee adjustment]
      ,[HYP_ABPM]=isnull([ABPM consultation fee],0)+isnull([ABPM consultation fee adjustment],0)
      ,[BP_FEE]= isnull([BP consultation fee],0)+isnull([BP consultation fee adjustment],0)
      ,[HYP_INCTV]=[Incentive fee]+[Incentive fee adjustment]
  FROM [CommunityPharmacy_Public].[BloodPressureService_BSA_claims]
  where [Month(claim)] >'2025-03-01') a 
   "
  result<-dbSendQuery(con,sql)
  CVDclaims<-dbFetch(result)
  
  CVDclaims
}

pull_CVDclaims_2<-function(){
  
  
  CVDclaims <- subset(cvdclaims_prep, select = -c(HYP_ABPM, BP_FEE, `HYP_INCTV`))
  
  Contractor<- Contractor%>%select(`Fcode`=`ContractorCode`, `Region_Name`,`STP`, `STP_Name`, `IMD_Decile`, `Permanently_closed`)%>%collect()
  checks<-cvd%>%
    group_by(`Fcode`)%>%
    summarise(`max_submissionID`=max(submission_id))%>%
    collect()
  
  master<- CVDclaims%>%
    mutate(`Month`=as.Date(`Month`, "%Y-%m-%d"))%>%
    group_by(Fcode)%>%
    summarise(SetupFee=sum(SetupFee, na.rm = T ), Month= min(Month))%>%
    filter(`SetupFee`>0)%>%
    left_join(Contractor,by="Fcode")%>%
    left_join(checks,by="Fcode")%>%
    collect()
  
  master$`ProvidingService?`<- ifelse(is.na(master$`max_submissionID`), "No", "Yes")
  master <- subset(master, select = -c(`max_submissionID`))
  
  master      
}

pull_CVDclaims_3<-function(){
  data<-cvdclaims_prep%>%
    group_by(Fcode)%>%
    summarise(total_incentive=sum(HYP_INCTV, na.rm=T))%>%
    right_join(cvdclaims_prep, "Fcode")%>%
    mutate(type = case_when(`HYP_INCTV` ==1000 ~ "Initial incentive",
                            `HYP_INCTV` ==400 & total_incentive == 1400 ~ "First followup incentive",
                            `HYP_INCTV` == 400 & total_incentive == 1800 & Month >'2023-03-01' ~ "Second followup incentive"))%>%
    filter(type != "NA")%>%
    group_by(`Month`,`type`)%>%
    summarise(`no_pharm`=n_distinct(`Fcode`))%>%
    collect()
  
  data
}


################################################################################
pull_dms <- function(){

  sql=" SELECT [Region Code]
      ,[Region]
      ,[ICB Code]
      ,[ICB]
      ,[Pharmacy Code]
      ,[Pharmacy  ]
      ,[Part Month]
      ,[Referral Received Date]
      ,[Referring Trust] =upper([Referring Trust] )
      ,[Referring Trust Name]
      ,[Stage 1]
      ,[Stage 2]
      ,[Stage 3]
      ,[Fee total]
      ,[Status]
  FROM [CommunityPharmacy_Restricted].[DMScombined]
"
  result<-dbSendQuery(con,sql)
  dms<-dbFetch(result)
  dbClearResult(result)
  
  dms
}

pull_dms_acute<-function(){
  discharge<- discharge_acute%>%
    mutate(`Month` = as.Date(`Discharge_Month`, "%d/%m/%Y"))%>%
    filter(`Month`>="01/11/2021")%>%
    collect()
  
  dms_trust<- DMS%>%
    mutate(`Month`= paste0(substr(`Part Month`,1,4), "-", substr(`Part Month`, 5,6), "-01"), 
           `Measure`= "Discharge Medicine Service activity", `Provider_Code`=`Referring Trust`)%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"), )%>%
    left_join(trust_lookup, "Provider_Code")%>%
    filter(`NHSE_Organisation_Type`== "ACUTE TRUST",
           `Month`>= "2021-11-01")%>%
    select(`Month`,`Trust_Code`=`Provider_Code`, `Measure`)%>%
    group_by(`Month`,`Trust_Code`, `Measure`)%>%
    mutate(`Figure`=max(row_number()))%>% collect()
  
  dms_trust
}


####################### Stop Smoking service - registration data ------
smoking_reg<- function(){
  sql="select  distinct [Service], [FCode], [RegistrationDate], [DateReported] FROM [CommunityPharmacy_Restricted].[Service_Registrations]
where [Service]='Smoking Cessation Advanced Service'"
  result<-dbSendQuery(con,sql)
  smoking_reg<-dbFetch(result)
  dbClearResult(result)
  
  smoking_reg<- smoking_reg%>%
    filter(`DateReported`==max(`DateReported`))%>%
    mutate(`RegistrationDate`=as.character(`RegistrationDate`))%>%
    mutate(`Month`= paste0(substr(`RegistrationDate`,1,8), "01"))%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    select(`Fcode`=`FCode`,`Month`)%>% collect()
  
  #smoking_reg<- read.csv("../data/Pharm_data/smoking_reg.csv")
  #smoking_reg<- smoking_reg%>%
  #mutate(`Month`= paste0(substr(`Date`,7,10),"-", substr(`Date`,4,5),"-01"))%>%
  #mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"), Fcode=`F.Code`)%>%
  #select(`Fcode`,`Month`)%>% collect()
  
  Contractor_simple<-Contractor%>%select(`Fcode`=`ContractorCode`, `Region_Name`,`STP`, `STP_Name`, `IMD_Decile`)%>%collect()
  
  master<- smoking_reg%>%
    left_join(Contractor_simple,by="Fcode")%>%
    collect()
  
  master
}

pull_SCS_all<- function(){

  sql="SELECT *
  FROM 
  [CommunityPharmacy_Public].[SCS_activity]"
  
  result<-dbSendQuery(con,sql)
  scs<-dbFetch(result)
  dbClearResult(result)
  
  colnames(scs) <- c("ICB Code"
                     ,"ICB"
                     ,"Pharmacy Code"
                     ,"Pharmacy"
                     ,"Set up fee"
                     ,"Set up fee adjustment"
                     ,"Consultation fee_30_10_40"
                     ,"Consultation fee adjustment"
                     ,"Nicotine Replacement Therapy product cost"
                     ,"Nicotine Replacement Therapy product cost adjustment"
                     ,"Nicotine Replacement Therapy product charges"
                     ,"Nicotine Replacement Therapy product charges adjustment"
                     ,"Month"
                     ,"Number of consultations"
                     ,"Consultation fee_30"
                     ,"Consultation fee_10"
                     ,"Consultation fee_40" )
  scs
}

pull_SCS<- function(){
  scs<- scs_all%>%
    mutate(`Measure`= "Stop Smoking Service activity")%>%
    select(`Month`,`ContractorCode`=`Pharmacy Code`, `Measure`, `Figure`=`Number of consultations`)%>%
    filter(`Figure`!=0)%>%
    collect()
  
  scs<- distinct(scs)
  
  scs
  
}

##### Contraception T1 service ----
pull_OCT1_reg<-function(){

  sql="select  distinct [Service], [FCode], [RegistrationDate], [DateReported] FROM [CommunityPharmacy_Restricted].[Service_Registrations]
where [Service]='Oral Contraception Tier 1 Service'"
  result<-dbSendQuery(con,sql)
  OCT1_reg0<-dbFetch(result)
  dbClearResult(result)
  
  OCT1_reg1 <- OCT1_reg0%>%
    filter(RegistrationDate<"2023-12-01")%>%
    filter(`DateReported`==max(`DateReported`))%>%
    mutate(`RegistrationDate`=as.character(`RegistrationDate`))%>%
    mutate(`Month`= paste0(substr(`RegistrationDate`,1,8), "01"))%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    select(`Fcode`=`FCode`,`Month`)%>% collect()
  
  OCT1_reg2 <- OCT1_reg0%>%
    filter(RegistrationDate>="2023-12-01")%>%
    filter(`DateReported`==max(`DateReported`))%>%
    mutate(`RegistrationDate`=as.character(`RegistrationDate`))%>%
    mutate(`Month`= paste0(substr(`RegistrationDate`,1,8), "01"))%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    select(`Fcode`=`FCode`,`Month`)%>% collect()
  
  OCT1_reg<-rbind(OCT1_reg1, OCT1_reg2)
  
  # OCT1_reg<- read.csv("../data/Pharm_data/OC_T1_reg.csv")
  # OCT1_reg<- OCT1_reg%>%
  # mutate(`Month`= paste0(substr(`Date`,7,10),"-", substr(`Date`,4,5),"-01"))%>%
  # mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"), Fcode=`F.Code`)%>%
  # select(`Fcode`,`Month`)%>% collect()
  
  Contractor_simple<-Contractor%>%select(`Fcode`=`ContractorCode`, `Region_Name`,`STP`, `STP_Name`, `IMD_Decile`)%>%collect()
  
  master<- OCT1_reg%>%
    left_join(Contractor_simple,by="Fcode")%>%
    collect()
  
  master
}


### changed data source from BSA's verified activity data to unverified data extract as part of new pharmacy first weekly data flow 
## Please note it is no longer called Tier 1 service, although "T1" is still used in function names. 
pull_OCT1_act<-function(){

  # sql="select a.[year_month], a.[Pharmacy Code] , count(distinct a.[Reference]) [Number of consultations]
  #	from (SELECT  [Reference]    
  #  ,[Claimant ODS Code] as [Pharmacy Code]     
  #  ,DATEADD(MONTH, DATEDIFF(MONTH, 0, [Assessment Date]), 0) AS [year_month]   
  # FROM [NHSE_Sandbox_DispensingReporting].[dbo].[OralContraception_MYS_activity]) a
  # group by a.[year_month],a.[Pharmacy Code] 
  #  order by a.[year_month],a.[Pharmacy Code] 
  
  sql="SELECT  [Date] as [year_month],[Contractor Code] as [Pharmacy Code],[NumberofCommunityPharmacyContraceptiveConsultations] as [Number of consultations]
  FROM [CommunityPharmacy_Public].[Pharm_and_DAC]
  where [Date]='2023-12-01' and [NumberofCommunityPharmacyContraceptiveConsultations]> 0
union
  SELECT  [Date] as [year_month],[Contractor Code] as [Pharmacy Code],([NumberofCommunityPharmacyContraceptiveOngoingConsultations]+[NumberofCommunityPharmacyContraceptiveInitiationConsultations]) as [Number of consultations]
  FROM [CommunityPharmacy_Public].[Pharm_and_DAC]
  where [Date] > '2023-12-01' and ([NumberofCommunityPharmacyContraceptiveOngoingConsultations]+[NumberofCommunityPharmacyContraceptiveInitiationConsultations]) >0
union
   SELECT  [Month(claim)] as [year_month] , [Pharmacy Code],[Number of consultations]
  FROM [CommunityPharmacy_Public].[OC_Tier1_activity_BSA_verified]
  where [Number of consultations]>0"
  
  result<-dbSendQuery(con,sql)
  OCT1_act<-dbFetch(result)
  dbClearResult(result)
  
  OCT1_act<-OCT1_act%>%
    mutate(Month = as.character(`year_month`),
           ContractorCode = `Pharmacy Code`,
           Measure = "Oral Contraception Service Activity",
           Figure = `Number of consultations`)%>%
    filter(Figure>0)%>%
    select(Month, ContractorCode, Measure, Figure)
  
  OCT1_act
}



####################### Pharm List ------
pharm_list<- function(){

  sql="SELECT *
  FROM [CommunityPharmacy_Public].[Ref_PharmaceuticalList]
  where SnapshotMonth = (SELECT max(SnapshotMonth)
  FROM [CommunityPharmacy_Public].[Ref_PharmaceuticalList])"
  result<-dbSendQuery(con,sql)
  pharm_list<-dbFetch(result)
  dbClearResult(result)
  
  pharm_list
}


pull_reg_pop<- function(){

  sql="SELECT *
  FROM [CommunityPharmacy_Public].[z_gp-reg-pat-prac-all-age-regions]
  where [EXTRACT_DATE] = 
  (select max([EXTRACT_DATE]) from [CommunityPharmacy_Public].[z_gp-reg-pat-prac-all-age-regions])"
  result<-dbSendQuery(con,sql)
  reg_pop<-dbFetch(result)
  dbClearResult(result)
  
  reg_pop <- reg_pop %>% 
    filter(ORG_TYPE== "Comm Region", SEX == "ALL")%>%
    select(EXTRACT_DATE,Region_code = ORG_CODE, pop=`NUMBER_OF_PATIENTS`)%>%
    collect()
  
  reg_pop
}


pull_PF<- function(){

  sql= "SELECT [Date]  as [Month]
      ,[Contractor Code] as [ContractorCode]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-AcuteOtitisMedia] as [PF_CP_AcuteOtitisMedia]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations -AcuteSoreThroat] as[PF_CP_AcuteSoreThroat]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-Impetigo] as [PF_CP_Impetigo]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-InfectedInsectBites] as[PF_CP_InfectedInsectBites]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-Shingles] as [PF_CP_Shingles] 
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-Sinusitis] as [PF_CP_Sinusitis]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-UncomplicatedUTI] as [PF_CP_UncomplicatedUTI]
      ,[NumberofPharmacyFirstUrgentMedicineSupplyConsultations] as[PF_CPCS_UrgentMedSupply]
      ,[NumberofPharmacyFirstMinorIllnessReferralConsultations] as[PF_CPCS_MinorIllness]
  FROM [CommunityPharmacy_Public].[Pharm_and_DAC]
  where [Date]>'2024-01-01'"
  
  result<-dbSendQuery(con,sql)
  PF<-dbFetch(result)
  dbClearResult(result)
  
  
  PF<-reshape2::melt(PF, 
                     id.vars= c("Month","ContractorCode"), 
                     measure.vars= c("PF_CP_AcuteOtitisMedia",
                                     "PF_CP_AcuteSoreThroat",
                                     "PF_CP_Impetigo",
                                     "PF_CP_InfectedInsectBites",
                                     "PF_CP_Shingles",
                                     "PF_CP_Sinusitis",
                                     "PF_CP_UncomplicatedUTI",
                                     "PF_CPCS_UrgentMedSupply",
                                     "PF_CPCS_MinorIllness"), variable.name= "Measure", value.name ="Figure")
  PF
}


pull_PF_income<-function(){

  sql= "select a.*, b.[InitialPayment_Month], b.[PF_Initial_Payment]
     from
(SELECT [Month]
      ,[Contractor] 
      ,([PF_CP_CONSULTATION_FEE]+[PF_CP_CONSULTATION_FEE_ADJ]) as [PF_CP_Consultation_Fee]
      ,([PF_CP_MONTHLY_FIXED_PAY]+[PF_CP_MONTHLY_FIXED_PAY_ADJ]) as [PF_CP_Monthly_Payment]
      ,([PF_CP_VAT_VALUE]+[PF_CP_VAT_VALUE_ADJ]) as [PF_CP_VAT]
  FROM [CommunityPharmacy_Public].[MIS_Pharmacy]
  where [Month]>'2024-01-01') a
  full join
  (SELECT [Month] as [InitialPayment_Month]
      ,[Contractor] 
      ,([PF_INITIAL_FIXED_PAYMENT]+[PF_INITIAL_FIXED_PAYMENT_ADJ]) as [PF_Initial_Payment]    
  FROM [CommunityPharmacy_Public].[MIS_Pharmacy]
  where [PF_INITIAL_FIXED_PAYMENT] <> 0 or [PF_INITIAL_FIXED_PAYMENT_ADJ] <>0) b
  on a.[Contractor]=b.[Contractor]"
  
  result<-dbSendQuery(con,sql)
  PF_income<-dbFetch(result)
  dbClearResult(result)
  
  PF_income
}

Master1 <- pull_Master1()
Contractor <- pull_Contractor()
flu_national <- pull_flu_national()
#dose_band <- pull_dose_band()
#SSP <- pull_ssp()
#funding <- pull_funding()
#servicefund <- pull_servicefund()
#FYfund<-pull_FYfund()
#servicevol<-pull_servicevol()
#cpcs_signup <- pull_cpcs_signup()
#trust_lookup <-pull_trust_lookup()
#discharge <- pull_discharge("All")
#discharge_acute<-pull_discharge("Acute")
#Date_stamp <- get_download_timestamp()
#names(flu_national) <- names(flu_national) %>% make.names()
cvd <- pull_CVD()
cvdclaims_prep<- pull_CVDclaims_1()
CVDclaims <- pull_CVDclaims_2()
#incent <- pull_CVDclaims_3()
#provider<-pull_provider()
#dac_income<- pull_dac_income()
DMS<-pull_dms()
#dms_trust<-pull_dms_acute()
smoke_reg<-smoking_reg()
OCT1_reg<-pull_OCT1_reg()
OCT1_act<-pull_OCT1_act()
#OCT1_act<-OCT1_act%>%filter(`Month`<'2024-03-01')
scs_all<-pull_SCS_all()
scs<-pull_SCS()
pharm_list <- pharm_list()
#region_pop <- pull_reg_pop()
PF<-pull_PF()
PF_income<-pull_PF_income()

HADS<-readRDS("~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/HADS.rds")
DOS <- readRDS("~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/DOS.rds")
gp_stage<-readRDS("~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Static data files/gp_stage.rds")
