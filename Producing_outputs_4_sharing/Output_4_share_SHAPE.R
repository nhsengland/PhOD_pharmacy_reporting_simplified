library(DBI)
library(magrittr) 
library(openxlsx)
library(tidyverse)
library (odbc)


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


sql=" 
SELECT [Fcode]=[ContractorCode]
      ,[IMD_Decile]

  FROM [CommunityPharmacy_Restricted].[Ref_Contractor]
   --where [EndDate] = '1900-01-01 00:00:00.000' or [EndDate] is NULL
"
result<-dbSendQuery(con,sql)
imd<-dbFetch(result)
dbClearResult(result)

pull_cvd<-function(){
sql= "SELECT *
  FROM [CommunityPharmacy_Public].[BloodPressureService_BSA_claims]"
result<-dbSendQuery(con,sql)
bsa<-dbFetch(result)
dbClearResult(result)

data1<-bsa%>%
  mutate(`Month`=as.Date(`Month(claim)`, "%Y-%m-%d"),
         Fcode=`Pharmacy Code`,
         SetupFee = `Set up fee`+`Set up fee adjustment`)%>%
  group_by(Fcode)%>%
  summarise(`Signup_Month`=min(Month), SetupFee = sum(SetupFee, na.rm=T), checks=sum(`Number of patients`, na.rm=T))%>%
  #filter(`SetupFee`>0)%>%
  mutate(`ProvidingService?`= ifelse(checks>0, "Yes", "No"))%>%
  select(`Fcode`,`Signup_Month`,`ProvidingService?`)%>%
  collect()

data2<-bsa%>%
  rename(Fcode=`Pharmacy Code`, 
         `Total Patients`=`Number of patients`)%>%
  mutate(`Number of Clinic BP checks`=ifelse(is.na(`Number of Clinic Blood Pressure checks (including LPS pharmacies)`), `Number of Clinic Blood Pressure checks (exc. LPS)`, `Number of Clinic Blood Pressure checks (including LPS pharmacies)`),
         `Number of ABPM`=ifelse(is.na(`Number of Ambulatory Blood Pressure Monitoring (including LPS pharmacies)`), `Number of Ambulatory Blood Pressure Monitoring (ABPM) (exc. LPS)`, `Number of Ambulatory Blood Pressure Monitoring (including LPS pharmacies)`),
         `Total Checks`=`Number of Clinic BP checks`+`Number of ABPM`)%>%
  left_join(data1, "Fcode")%>%
  left_join(imd, "Fcode")%>%
  select(`Month(claim)`, Fcode, `Total Patients`,`IMD_Decile`, `Total Checks`,`Number of Clinic BP checks`,`Number of ABPM`,`Signup_Month`,`ProvidingService?`)%>%
  mutate(Signup_Month=format(Signup_Month, "%Y-%m-%d"), `Month(claim)`=format(`Month(claim)`, "%Y-%m-%d"))%>%
  collect()

data2

}

bp_act=subset(pull_cvd(), select = -c(`IMD_Decile`,	`Signup_Month`,	`ProvidingService?`))
bp_act<-bp_act%>%filter(!is.na(`Total Checks`))

update = format(Sys.Date(), '%B%Y')
tab_name<- list('BP activity'= bp_act,
                'BP signup'=distinct(subset(pull_cvd(), select = c(`Fcode`,	`IMD_Decile`,	`Signup_Month`,	`ProvidingService?`))))
openxlsx::write.xlsx(tab_name, file = paste0('~/Rprojects/CVD-reporting/reports\\Pharmacy_BPCS_', update, '.xlsx')) 

##### SCS, OC and PF since launch ----
sql=" 
/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [Date] as [Month(Claim)]
      ,[Area Code] as [ICB Code]
      ,[Area] as [ICB]
      ,[Contractor Code]
      ,[Contractor Name]
   
      ,[NumberofCommunityPharmacySmokingCessationConsultations]

      ,[NumberofCommunityPharmacyContraceptiveConsultations]
      ,[NumberofCommunityPharmacyContraceptiveOngoingConsultations]
      ,[NumberofCommunityPharmacyContraceptiveInitiationConsultations]

      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-AcuteOtitisMedia]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations -AcuteSoreThroat]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-Impetigo]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-InfectedInsectBites]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-Shingles]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-Sinusitis]
      ,[NumberofPharmacyFirstClinicalPathwaysConsultations-UncomplicatedUTI]
      ,[NumberofPharmacyFirstUrgentMedicineSupplyConsultations]
      ,[NumberofPharmacyFirstMinorIllnessReferralConsultations]
  FROM [CommunityPharmacy_Public].[Pharm_and_DAC]
  where [Contractor Type] ='Pharmacy' and [Date] >='2022-03-01'
  order by [Date] 
"
result<-dbSendQuery(con,sql)
otherServ<-dbFetch(result)
dbClearResult(result)

otherServ<-otherServ%>%
  mutate(`Month(Claim)`=as.character(format(`Month(Claim)`, "%Y-%m-%d")))


write.csv(otherServ,paste0('~/Rprojects/CVD-reporting/reports\\OC SCS and PF activity data_', update, '.csv'), row.names = FALSE)


