library(tidyverse)
#library(ggplot2)
#library(ggpubr)
library(stringr)
library(magrittr) # needs to be run every time you start R and want to use %>%
library(dplyr)
#library(scales)
library(readxl)
library(textclean)
library(lubridate)

############ tidy up /rename cols and produce Excel outputs for data sharing and for QA purpose (e.g. against PCDID) ------------
tidy_pharm<-function(){
  colnames(pharmacy_master) <- c("Month",
                                 "Fcode",
                                 "Total_patients",
                                 "min_patient_age",
                                 "Avg_patient_age",
                                 "max_patient_age",
                                 "ContractorName",
                                 "Region_Name",
                                 "ICB",
                                 "ICB_Name",
                                 "IMD_Decile",
                                 "Total_checks",
                                 "5BP?",
                                 "5BP+1ABPM",
                                 "Opportunistic_BP_checks",
                                 "Avg_oppBP_patient_age",
                                 "BP_Opp_over40",
                                 "%BPopp_over40",
                                 "BPopp_result_low",
                                 "BPopp_result_veryhigh",
                                 "BPopp_result_high",
                                 "BPopp_result_refer",
                                 "BPopp_result_normal",
                                 "BPopp_result_other",
                                 "Refer_BP_checks",
                                 "Avg_BPref_age",
                                 "BP_ref_over40",
                                 "%BPref_over40",
                                 "BPref_result_low",
                                 "BPref_result_veryhigh",
                                 "BPref_result_high",
                                 "BPref_result_refer",
                                 "BPref_result_normal",
                                 "BPref_result_other",
                                 "Opportunistic_ABPM",
                                 "Avg_ABPMopp_age",
                                 "ABPMopp_result_low",
                                 "ABPMopp_result_normal",
                                 "ABPMopp_result_Stage1",
                                 "ABPMopp_result_Stage2",
                                 "ABPMopp_result_Severe",
                                 "ABPMopp_result_other",
                                 "Refer_ABPM",
                                 "Avg_ABPMref_age",
                                 "ABPMref_result_low",
                                 "ABPMref_result_normal",
                                 "ABPMref_result_Stage1",
                                 "ABPMref_result_Stage2",
                                 "ABPMref_result_Severe",
                                 "ABPMref_result_other",
                                 "ABPM_asFollowUpOfBPopp",
                                 "ABPM_asFollowUpOfReferredBP")
  
  pharmacy_master
}

cvd_tidy<-tidy_pharm()
library(openxlsx)
update = format(Sys.Date(), '%B%Y')
#define sheet names for each data frame

### Manually add new tabs with new quarter below
FY21_22_Q3 <- cvd_tidy%>% filter(Month<="2021-12-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY21_22_Q4 = cvd_tidy%>%filter(Month>"2021-12-01"& Month<="2022-03-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY22_23_Q1 = cvd_tidy%>%filter(Month>"2022-03-01"& Month<="2022-06-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY22_23_Q2 = cvd_tidy%>%filter(Month>"2022-06-01"& Month<="2022-09-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY22_23_Q3 = cvd_tidy%>%filter(Month>"2022-09-01"& Month<="2022-12-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY22_23_Q4 = cvd_tidy%>%filter(Month>"2022-12-01"& Month<="2023-03-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY23_24_Q1 = cvd_tidy%>%filter(Month>"2023-03-01"& Month<="2023-06-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY23_24_Q2 = cvd_tidy%>%filter(Month>"2023-06-01"& Month<="2023-09-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY23_24_Q3 = cvd_tidy%>%filter(Month>"2023-09-01"& Month<="2023-12-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY23_24_Q4 = cvd_tidy%>%filter(Month>"2023-12-01"& Month<="2024-03-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY24_25_Q1 = cvd_tidy%>%filter(Month>"2024-03-01"& Month<="2024-06-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY24_25_Q2 = cvd_tidy%>%filter(Month>"2024-06-01"& Month<="2024-09-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY24_25_Q3 = cvd_tidy%>%filter(Month>"2024-09-01"& Month<="2024-12-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY24_25_Q4 = cvd_tidy%>%filter(Month>"2024-12-01"& Month<="2025-03-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY25_26_Q1 = cvd_tidy%>%filter(Month>"2025-03-01"& Month<="2025-06-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY25_26_Q2 = cvd_tidy%>%filter(Month>"2025-06-01"& Month<="2025-09-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY25_26_Q3 = cvd_tidy%>%filter(Month>"2025-09-01"& Month<="2025-12-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
FY25_26_Q4 = cvd_tidy%>%filter(Month>"2025-12-01"& Month<="2026-03-01")%>%mutate(Month=format(Month, '%b-%Y'))%>%collect()
Data_Dictionary = dict()
dataset_names1 <- list('Data dictionary'= Data_Dictionary,
                       'FY21_21 Q3' = FY21_22_Q3, 
                       'FY21_22 Q4' = FY21_22_Q4, 
                       'FY22_23 Q1' = FY22_23_Q1, 
                       'FY22_23 Q2' = FY22_23_Q2,
                       'FY22_23 Q3' = FY22_23_Q3, 
                       'FY22_23 Q4' = FY22_23_Q4, 
                       'FY23_24 Q1' = FY23_24_Q1,
                       'FY23_24 Q2' = FY23_24_Q2,
                       'FY23_24 Q3' = FY23_24_Q3,
                       'FY23_24 Q4' = FY23_24_Q4,
                       'FY24_25 Q1' = FY24_25_Q1,
                       'FY24_25 Q2' = FY24_25_Q2,
                       'FY24_25 Q3' = FY24_25_Q3,
                       'FY24_25 Q4' = FY24_25_Q4,
                       'FY25_26 Q1' = FY25_26_Q1,
                       'FY25_26 Q2' = FY25_26_Q2,
                       'FY25_26 Q3' = FY25_26_Q3,
                       'FY25_26 Q4' = FY25_26_Q4)

tidy_ics<-function(){
  colnames(stp_master) <- c("Month",
                            "ICS",
                            "ICS_Name",
                            "no_pharm",
                            "Total_patients",
                            "min_patient_age",
                            "Avg_patient_age",
                            "max_patient_age",
                            "Total_checks",
                            "Opportunistic_BP_checks",
                            "Avg_oppBP_patient_age",
                            "BP_Opp_over40",
                            "%BPopp_over40",
                            "BPopp_result_low",
                            "BPopp_result_veryhigh",
                            "Bpopp_result_high",
                            "BPopp_result_refer",
                            "Bpopp_result_normal",
                            "BPopp_result_other",
                            "Refer_BP_checks",
                            "Avg_BPref_age",
                            "BP_ref_over40",
                            "%BPref_over40",
                            "BPref_result_low",
                            "BPref_result_veryhigh",
                            "BPref_result_high",
                            "BPopp_result_refer",
                            "Bpopp_result_normal",
                            "BPref_result_other",
                            "Opportunistic_ABPM",
                            "Avg_ABPMopp_age",
                            "ABPMopp_result_low",
                            "ABPMopp_result_normal",
                            "ABPMopp_result_Stage1",
                            "ABPMopp_result_Stage2",
                            "ABPMopp_result_Severe",
                            "ABPMopp_result_other",
                            "Refer_ABPM",
                            "Avg_ABPMref_age",
                            "ABPMref_result_low",
                            "ABPMref_result_normal",
                            "ABPMref_result_Stage1",
                            "ABPMref_result_Stage2",
                            "ABPMref_result_Severe",
                            "ABPMref_result_other",
                            "ABPM_asFollowUpOfBPopp",
                            "ABPM_asFollowUpOfReferredBP")
  stp_master$Month=as.character(stp_master$Month)
  stp_master
}

tidy_regional<-function(){
  colnames(regional_master) <- c("Month",
                                 "Region_Name",
                                 "no_pharm",
                                 "Total_patients",
                                 "min_patient_age",
                                 "Avg_patient_age",
                                 "max_patient_age",
                                 "Total_checks",
                                 "Opportunistic_BP_checks",
                                 "Avg_oppBP_patient_age",
                                 "BP_Opp_over40",
                                 "%BPopp_over40",
                                 "BPopp_result_low",
                                 "BPopp_result_veryhigh",
                                 "Bpopp_result_high",
                                 "BPopp_result_refer",
                                 "Bpopp_result_normal",
                                 "BPopp_result_other",
                                 "Refer_BP_checks",
                                 "Avg_BPref_age",
                                 "BP_ref_over40",
                                 "%BPref_over40",
                                 "BPref_result_low",
                                 "BPref_result_veryhigh",
                                 "BPref_result_high",
                                 "BPopp_result_refer",
                                 "Bpopp_result_normal",
                                 "BPref_result_other",
                                 "Opportunistic_ABPM",
                                 "Avg_ABPMopp_age",
                                 "ABPMopp_result_low",
                                 "ABPMopp_result_normal",
                                 "ABPMopp_result_Stage1",
                                 "ABPMopp_result_Stage2",
                                 "ABPMopp_result_Severe",
                                 "ABPMopp_result_other",
                                 "Refer_ABPM",
                                 "Avg_ABPMref_age",
                                 "ABPMref_result_low",
                                 "ABPMref_result_normal",
                                 "ABPMref_result_Stage1",
                                 "ABPMref_result_Stage2",
                                 "ABPMref_result_Severe",
                                 "ABPMref_result_other",
                                 "ABPM_asFollowUpOfBPopp",
                                 "ABPM_asFollowUpOfReferredBP")
  regional_master$Month=as.character(regional_master$Month)
  regional_master
}

tidy_national<-function(){
  colnames(national_master) <- c("Month",
                                 "no_pharm",
                                 "Total_patients",
                                 "min_patient_age",
                                 "Avg_patient_age",
                                 "max_patient_age",
                                 "Total_checks",
                                 "Opportunistic_BP_checks",
                                 "Avg_oppBP_patient_age",
                                 "BP_Opp_over40",
                                 "%BPopp_over40",
                                 "BPopp_result_low",
                                 "BPopp_result_veryhigh",
                                 "Bpopp_result_high",
                                 "BPopp_result_refer",
                                 "Bpopp_result_normal",
                                 "BPopp_result_other",
                                 "Refer_BP_checks",
                                 "Avg_BPref_age",
                                 "BP_ref_over40",
                                 "%BPref_over40",
                                 "BPref_result_low",
                                 "BPref_result_veryhigh",
                                 "BPref_result_high",
                                 "BPopp_result_refer",
                                 "Bpopp_result_normal",
                                 "BPref_result_other",
                                 "Opportunistic_ABPM",
                                 "Avg_ABPMopp_age",
                                 "ABPMopp_result_low",
                                 "ABPMopp_result_normal",
                                 "ABPMopp_result_Stage1",
                                 "ABPMopp_result_Stage2",
                                 "ABPMopp_result_Severe",
                                 "ABPMopp_result_other",
                                 "Refer_ABPM",
                                 "Avg_ABPMref_age",
                                 "ABPMref_result_low",
                                 "ABPMref_result_normal",
                                 "ABPMref_result_Stage1",
                                 "ABPMref_result_Stage2",
                                 "ABPMref_result_Severe",
                                 "ABPMref_result_other",
                                 "ABPM_asFollowUpOfBPopp",
                                 "ABPM_asFollowUpOfReferredBP")
  
  national_master$Month=as.character(national_master$Month)
  national_master
}
ICS<- tidy_ics()#%>%mutate(Month=as.character(as.Date(Month), '%b-%Y'))%>%collect()
Regional <- tidy_regional()#%>%mutate(Month=as.character(as.Date(Month), '%b-%Y'))%>%collect()
National <- tidy_national()#%>%mutate(Month=as.character(as.Date(Month), '%b-%Y'))%>%collect()
dataset_names2 <- list('ICS' = ICS, 'Regional' = Regional, 'National' = National)

Signup = subset(CVDclaims, select = -c(`Month`))%>%mutate(Signup_Month=format(Signup_Month, "%Y-%m-%d"))
dataset_names3 <- list('Signup'= Signup)


#export each data frame to separate sheets in same Excel file
openxlsx::write.xlsx(dataset_names1, file = paste0('~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/Outputs\\Pharmacy_Activity_data_', update, '.xlsx')) 
openxlsx::write.xlsx(dataset_names2, file = paste0('~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/Outputs\\Aggregated_Activity_data_', update, '.xlsx')) 
openxlsx::write.xlsx(dataset_names3, file = paste0('~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/Outputs\\Pharmacy_Signup_data_', update, '.xlsx')) 

dt= cvd_tidy
write.csv(dt,"~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/Outputs\\NHS Community Pharmacy Blood Pressure Check Service_Pharmacy Activity.csv", row.names = FALSE)

BP_result_other <-cvd%>% filter(bp_result =="Other")%>%collect()
write.csv(BP_result_other,"~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/Outputs\\BPcheck_result_other.csv", row.names = FALSE)

