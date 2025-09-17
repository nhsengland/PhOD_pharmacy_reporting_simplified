#Render cvd report
personal= "jin.tong" #Please replace!!!

# Set working directory
setwd(paste0("C:/Users/", personal,"/Documents/Rprojects/PhOD_pharmacy_reporting_simplified"))

source("~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/01 DataPrep.R")
source("~/Rprojects/PhOD_pharmacy_reporting_simplified/02_CVD_monthly_report/02_Producing_Excel_outputs.R")
