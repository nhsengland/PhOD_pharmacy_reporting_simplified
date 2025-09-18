#Render reports and maps
### Please note Sign-in needs to be approved every time when a new R session is launched 
personal= "jin.tong" #Please replace!!!

# Set working directory
setwd(paste0("C:/Users/", personal,"/Documents/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure"))

source("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/01_import_unplanned_closures_data - UDAL.R")

print(paste0("Excel output saved as [collated_unplanned_closures_data_Oct21_", data_date, "_INTERNAL.xlsx] at C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data/Internal stakeholders"))
print(paste0("Excel output saved as [collated_unplanned_closures_data_Oct21_", data_date, "_External.xlsx] at C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data/External stakeholders"))
print(paste0("A master excel data file saved as [unplannedClosuresData.xlsx] at C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Collated data"))

rmarkdown::render(input = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_EXTERNAL.Rmd",
                  output_format = "xaringan::moon_reader",
                  output_file = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_EXTERNAL_",format(Sys.Date()%m-% months(1), '%B%Y'), ".html")


rmarkdown::render(input = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_INTERNAL.Rmd",
                  output_format = "xaringan::moon_reader",
                  output_file = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_INTERNAL_",format(Sys.Date()%m-% months(1), '%B%Y'), ".html")

#Please make sure 3-letter month names are corrected first---
source("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/03_unplanned_closures_mapping_July_working_version.R")