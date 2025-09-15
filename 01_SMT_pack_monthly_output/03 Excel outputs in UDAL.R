library(tidyverse)
library(ggplot2)
library(ggpubr)
library(stringr)
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)
library(scales)
library(grid)
library(gridExtra)
library(textclean)
library(lubridate)


############################## Produce excel outputs for data sharing along SMT pack -----------------------
SMT_cpcs<-function(type= "GP CPCS"){
  data<- CPCS_all%>%
    filter(Measure == type) %>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    group_by(`Month`,`Measure` )%>%
    summarise(`Completed_CPCS` = sum(as.numeric(`Figure`)), `Number of Pharmacies delivering in reporting month`= as.numeric(n_distinct(`ContractorCode`, na.rm=T))) %>% 
    collect()
}

SMT_gp<-function(){
  t<- GPcpcs2%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    group_by(Month)%>%
    summarise(`Number of GPs actively making referral in reporting month`= as.numeric(n_distinct(`GPCode`, na.rm=T)) )%>% collect()
  
  t1 <- GPcpcs2%>%
    group_by(GPCode)%>%
    summarise(Month=min(Month))%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    collect()
  
  t2<-t1%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    group_by(Month)%>%
    summarise(`Number of new GPs starting referral`= as.numeric(n_distinct(`GPCode`, na.rm=T)) )%>% collect()
  
  t3 <- t1 %>%
    group_by(`Month`)%>%
    summarise(`newGP`= n_distinct(`GPCode`, na.rm=T))%>%
    select(`newGP`,`Month`)%>%
    mutate(`cum_GP`=0)%>%
    collect()
  
  for (i in (1: nrow(t3))){
    if (i<2){t3$`cum_GP`[1]= t3$`newGP`[1]}
    else{t3$`cum_GP`[i]<- t3$`cum_GP`[i-1]+t3$`newGP`[i]}
  }
  
  t3<- t3%>%
    select(`Month`, `Cumulative GP practices actively referring`=`cum_GP`)%>%
    collect()
  
  data<- SMT_cpcs("GP CPCS")%>%
    left_join(t, "Month")%>%
    left_join(t2, "Month")%>%
    left_join(t3, "Month")%>%
    collect()
  
  data$Month=as.character(data$Month)
  data
}

SMT_dms<- function(){
  data <- Master %>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    filter(Measure == "Discharge Medicine Service activity") %>%
    group_by(`Month`,`Measure`) %>%
    summarise(`Activity` = sum(as.numeric(`Figure`)), `Number of pharmacies delivering`= as.numeric(n_distinct(`ContractorCode`, na.rm=T))) %>% 
    collect()
  data$Month=as.character(data$Month)
  data
}

SMT_nms<- function(){
  data <- Master %>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    filter(Measure == "New Medicine Service (NMS) Activity") %>%
    group_by(`Month`,`Measure`) %>%
    summarise(`Activity` = sum(as.numeric(`Figure`)), `Number of pharmacies delivering`= as.numeric(n_distinct(`ContractorCode`, na.rm=T))) %>% 
    collect()
  data$Month=as.character(data$Month)
  data
}

SMT_bp <- function(){
  
  data<-CVDclaims%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    group_by(`Month`)%>%
    summarise(`no_signup`=n())%>%
    right_join(national_master, "Month")%>%
    #select(Month, `no_signup`,`no_pharm`,`Total_checks`,`Total_patients`)%>%
    collect()
  colnames(data) <- c("Month","Number of pharmacy receiving signup payment","Number of pharmacy delivering","Total patients","Total checks","Opportunistic BP check","BP check by referral","Opportunistic ABPM","ABPM by referral")
  data$Month=as.character(data$Month)
  data
}

SMT_smoke<-function(){
  data<-smoke_reg%>%
    group_by(`Month`)%>%
    summarise(`Number of signups`=n())%>%
    collect()
  
  data1<-scs%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    group_by(`Month`)%>%
    summarise(`Number of pharmacies delivering`= as.numeric(n_distinct(`ContractorCode`, na.rm=T)))%>%
    collect()
  
  data2<-scs_all%>%
    group_by(`Month`)%>%
    summarise(`Number of consulations`= sum(`Number of consultations`, na.rm=T), 
              `Total NRT product costs (£)`=sum(`Nicotine Replacement Therapy product cost adjustment`+`Nicotine Replacement Therapy product cost`, na.rm=T))%>%
    right_join(data1, "Month")%>%
    right_join(data, "Month")
  
  data2$Month=as.character(data2$Month)
  data2
}

SMT_oct1<-function(){
  data<-OCT1_reg%>%
    group_by(`Month`)%>%
    summarise(`Number of pharmacies receiving signup payment`=n())%>%
    collect()
  
  data2<-OCT1_act%>%
    filter(`Figure`>0)%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    group_by(`Month`)%>%
    summarise(`Number of consulations`= sum(`Figure`, na.rm=T), `Number of pharmacies delivering`= as.numeric(n_distinct(`ContractorCode`, na.rm=T)))%>%
    right_join(data, "Month")
  
  data2$Month=as.character(data2$Month)
  data2
}

SMT_PF<-function(){
  data0<-PF_income%>%
    filter(`PF_Initial_Payment`!=0)%>%
    group_by(`InitialPayment_Month`)%>%
    summarise(`Number of pharmacies receiving initial fixed payment`= as.numeric(n_distinct(`Contractor`, na.rm=T)))%>%
    rename(`Month`=`InitialPayment_Month` )
  
  data1<-PF_income%>%
    filter(`PF_CP_Monthly_Payment`>0)%>%
    group_by(Month)%>%
    summarise(`Number of pharmacies qualifying fixed monthly payment`= as.numeric(n_distinct(`Contractor`, na.rm=T)))
  
  data2<-PF%>%
    filter(`Figure`>0)%>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d"))%>%
    group_by(`Month`)%>%
    summarise(`Number of consulations claimed in reporting month`= sum(`Figure`, na.rm=T), `Number of pharmacies claiming in reporting month`= as.numeric(n_distinct(`ContractorCode`, na.rm=T)))
  
  data<-data0%>%
    full_join(data2, "Month")%>%
    full_join(data1, "Month")
  
  data$Month=as.character(data$Month)
  data
}

SMT_PF_pathway<-function(){
  data <- PF%>%
    filter(Measure != "") %>%
    mutate(`Month`= as.Date(`Month`, "%Y-%m-%d") )%>%
    group_by(`Month`,`Measure` )%>%
    summarise(`Number of consulations claimed in reporting month` = sum(as.numeric(`Figure`)), `Number of contractors claiming`= as.numeric(n_distinct(`ContractorCode`, na.rm=T))) %>% 
    collect()
  
  
  data$Month=as.character(data$Month)
  data
}


dataset_names<- list('PharmacyFirst'=SMT_PF()
                     ,'PF_by_pathways'=SMT_PF_pathway()
                     ,'GP CPCS' = SMT_gp()
                     , '111 MI CPCS' = SMT_cpcs("111 Minor Illness (MI) CPCS activity")
                     ,'111 US CPCS' = SMT_cpcs("111 Urgent Medicine Supply (US) CPCS activity")
                     ,'UEC CPCS' = SMT_cpcs("UEC CPCS")
                     ,'DMS'= SMT_dms()
                     ,'NMS'= SMT_nms()
                     ,'BPcheck'=SMT_bp()
                     ,'Smoking'=SMT_smoke()
                     ,"Contraception"=SMT_oct1()
)


openxlsx::write.xlsx(dataset_names, file = paste0('~/Rprojects/PhOD_pharmacy_reporting_simplified/01_SMT_pack_monthly_output/Outputs/SMT_data_', Sys.Date(), '.xlsx')) 
