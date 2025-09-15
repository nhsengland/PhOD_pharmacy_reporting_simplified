#Render reports and maps
### Please note Sign-in needs to be approved every time when a new R session is launched 
source("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/01_import_unplanned_closures_data - UDAL.R")

rmarkdown::render(input = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_EXTERNAL.Rmd",
                  output_format = "xaringan::moon_reader",
                  output_file = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_EXTERNAL_",format(Sys.Date()%m-% months(1), '%B%Y'), ".html"
                  , envir= knitr::knit_global())


rmarkdown::render(input = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_INTERNAL.Rmd",
                  output_format = "xaringan::moon_reader",
                  output_file = "~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/rmarkdown/Unplanned_closures_monthly_pack_INTERNAL_",format(Sys.Date()%m-% months(1), '%B%Y'), ".html")

#Please make sure 3-letter month names are corrected first---
source("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/03_unplanned_closures_mapping_July_working_version.R")