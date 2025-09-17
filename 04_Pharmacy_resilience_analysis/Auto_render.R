
personal= "jin.tong" #Please replace!!!

# Set working directory
setwd(paste0("C:/Users/", personal,"/Documents/Rprojects/PhOD_pharmacy_reporting_simplified"))

rmarkdown::render(input = "~/Rprojects/PhOD_pharmacy_reporting_simplified/04_Pharmacy_resilience_analysis/rmarkdown/Pharmacy_sector_reselience_analysis_internal.Rmd",
                  output_format = "xaringan::moon_reader",
                  output_file = paste0("../Pharmacy_sector_reselience_analysis_internal_", Sys.Date(), ".html"))


