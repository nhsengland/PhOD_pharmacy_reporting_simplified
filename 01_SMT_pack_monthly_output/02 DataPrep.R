#library(DBI)
library(tidyverse)
library(ggplot2)
library(ggpubr)
library(stringr)
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)
library(scales)
library(readxl)
#library(plyr)
library(textclean)


DMS_simple<- DMS%>%
  mutate(`Month`= paste0(substr(`Part Month`,1,4), "-", substr(`Part Month`, 5,6), "-01"), 
         `Measure`= "Discharge Medicine Service activity")%>%
  select(`Month`,`ContractorCode`=`Pharmacy Code`, `Measure`)%>%
  group_by(`Month`,`ContractorCode`, `Measure`)%>%
  mutate(`Figure`=max(row_number()))%>%
  collect()

DMS_simple<- distinct(DMS_simple)


get_national_master<-function(){
  Allcheck <- cvd %>%
    group_by(`Month`)%>%
    summarise(`Total_patients`=n(), `no_pharm`= n_distinct(`disp_code`))%>%
    select(`Month`, `no_pharm`, `Total_patients`)%>%
    collect()
  Allcheck<- distinct(Allcheck)
  
  BP_opp <-cvd%>%
    filter(`bp_check`== TRUE, `bp_check_referral`!=TRUE)%>%
    group_by(`Month`)%>%
    summarise(`BP_Opp`=n())%>%
    select(`Month`,  `BP_Opp`)%>%
    collect()
  BP_opp<-distinct(BP_opp)
  
  
  ABPM_opp <-cvd%>%
    filter(`abpm`== TRUE, `abpm_referral`!=TRUE)%>%
    group_by(`Month`)%>%
    mutate(`abpm_Opp`=n())%>%
    select(`Month`,  `abpm_Opp`)%>%
    collect()
  ABPM_opp<-distinct(ABPM_opp)
  
  BP_ref <-cvd%>%
    filter(`bp_check`== TRUE, `bp_check_referral`== TRUE)%>%
    group_by(`Month`)%>%
    mutate(`BP_ref`=n())%>%
    select(`Month`,  `BP_ref`)%>%
    collect()
  BP_ref<-distinct(BP_ref)
  
  
  ABPM_ref <-cvd%>%
    filter(`abpm`== TRUE, `abpm_referral`==TRUE)%>%
    group_by(`Month`)%>%
    mutate(`abpm_ref`=n())%>%
    select(`Month`,  `abpm_ref`)%>%
    collect()
  ABPM_ref<-distinct(ABPM_ref)
  
  
  master <- Allcheck%>%
    mutate(`Total_checks`= 0)%>%
    left_join(BP_opp, by= c( "Month"))%>%
    left_join(BP_ref, by= c( "Month"))%>%
    left_join(ABPM_opp, by= c( "Month"))%>%
    left_join(ABPM_ref, by= c( "Month"))%>%
    collect()
  
  #check if TP service payment lower and higher requirements are met
  master$`Total_checks`<- rowSums(master[, c("BP_Opp", "BP_ref", "abpm_Opp", "abpm_ref")],na.rm=T)
  master[is.na(master)] <- 0
  master
}

national_master<- get_national_master()

DMS_simple$`Figure`<- as.numeric(DMS_simple$`Figure`)

scs$Month <-as.character(scs$Month)

#append DMS, CPCS & SCS table into Master
CPCS_all <- readRDS("~/Rprojects/SMT-pharmacy-report/data/Pharm_data/CPCS_all.rds")
Master <- rbind(Master1, DMS_simple, CPCS_all, scs, OCT1_act)


########## aggregate old and new gp cpcs data at practice level
GPcpcs2<- readRDS("~/Rprojects/SMT-pharmacy-report/data/Pharm_data/GPcpcs2.rds")

