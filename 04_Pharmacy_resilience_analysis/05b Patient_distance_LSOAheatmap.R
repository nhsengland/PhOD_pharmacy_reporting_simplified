library(ggplot2)
library(GGally)
library(ggExtra)
library(ggalluvial)
library(plotly)
library(tidyverse)
library(odbc)
library(dplyr)
library(scales)
library(formattable)
library(sf)
library(tmap)
library(openxlsx)

dis_patients<-read.xlsx("N:/_Everyone/Primary Care Group/Pharmacy SMT data pack/Clustering/MPI_LSOA2011_dispensing_patients.xlsx", sheet= "Sheet1")

Pharmacy_Patient_Dist2 <- readRDS("~/Rprojects/Pharmacy_Resilience_Report/pharm_patient_distance2.rds")

Pharmacy_Patient_Dist2 <-Pharmacy_Patient_Dist2 %>%
  left_join(dis_patients, by = c("Patient LSOA" = "LSOA_2011_Code"))%>%
   mutate(noPharm_cat=ifelse(`noPharm_5km`<1,"No pharmacy",ifelse(`noPharm_5km`<5,"between 1 and 5",ifelse(`noPharm_5km`<10,"between 5 and 10",ifelse(`noPharm_5km`<20,"between 10 and 20",ifelse(`noPharm_5km`>=20,"20 or more"))))))%>%
   rename(`LSOA (2011) name`=`lsoa11nm`,
          `To nearest pharmacy`=`min_dist_cat`,
          `Distance to nearest pharmacy (km)`= `min_dist`,
          `Number of pharmacies within 5km`=`noPharm_5km`,
          `NoPharm_within5km`=`noPharm_cat`,
          `Local population`=`n_livingpatients`, 
          `Dispensing patients`=`n_dispensingpatients`,
          `Dispensing Patients (% of local population)`=`%dispensingpatients`)%>%
  mutate(`Distance to nearest pharmacy (km)`=paste0(round(`Distance to nearest pharmacy (km)`,2),"km"), 
         `Number of pharmacies within 5km per 1k population`=round(`Number of pharmacies within 5km`/`Local population`*1000,0),
         `NoPharm(within5km)_per_1kPopulation`=ifelse(`Number of pharmacies within 5km per 1k population`<1,"No pharmacy",ifelse(`Number of pharmacies within 5km per 1k population`<5,"between 1 and 5",ifelse(`Number of pharmacies within 5km per 1k population`<10,"between 5 and 10",ifelse(`Number of pharmacies within 5km per 1k population`<20,"between 10 and 20",ifelse(`Number of pharmacies within 5km per 1k population`>=20,"20 or more"))))),
         `Proportion of dispensing patients`= ifelse(`Dispensing Patients (% of local population)`==0,"No dispensing patients",ifelse(`Dispensing Patients (% of local population)`<0.05,"Less than 5%",ifelse(`Dispensing Patients (% of local population)`<0.1,"between 5% and 10%",ifelse(`Dispensing Patients (% of local population)`<0.2,"between 10% and 20%",ifelse(`Dispensing Patients (% of local population)`<0.5,"between 20% and 50%",ifelse(`Dispensing Patients (% of local population)`>=0.5,"50% or more")))))),
         `Dispensing Patients (% of local population)`=paste0(round(`Dispensing Patients (% of local population)`*100, 1),"%"))
  

Pharmacy_Patient_Dist2$`To nearest pharmacy`<-factor( Pharmacy_Patient_Dist2$`To nearest pharmacy`, levels = c("< 1km","between 1 and 2km","between 2 and 5km","between 5 and 10km","between 10 and 20km","20km or further" ))
Pharmacy_Patient_Dist2$`NoPharm_within5km`<-factor( Pharmacy_Patient_Dist2$`NoPharm_within5km`, levels = c("20 or more","between 10 and 20","between 5 and 10","between 1 and 5", "No pharmacy"))
Pharmacy_Patient_Dist2$`NoPharm(within5km)_per_1kPopulation`<-factor( Pharmacy_Patient_Dist2$`NoPharm(within5km)_per_1kPopulation`, levels = c("20 or more","between 10 and 20","between 5 and 10","between 1 and 5", "No pharmacy"))
Pharmacy_Patient_Dist2$`Proportion of dispensing patients`<-factor( Pharmacy_Patient_Dist2$`Proportion of dispensing patients`, levels = c("50% or more", "between 20% and 50%","between 10% and 20%","between 5% and 10%","Less than 5%","No dispensing patients"))



LSOA_Boundaries <- sf::st_read("N:/_Everyone/Primary Care Group/Pharmacy SMT data pack/Clustering/LSOA_2011_Boundaries_Super_Generalised_Clipped_BSC_EW_V4_6760014548972368257.gpkg", quiet = TRUE)


LSOA_to_nearest_Pharmacy <- dplyr::left_join(Pharmacy_Patient_Dist2, LSOA_Boundaries, by = c("Patient LSOA" = "LSOA11CD"))

# Transform data into sf
LSOA_to_nearest_Pharmacy<- sf::st_as_sf(LSOA_to_nearest_Pharmacy)

plot_lsoa_map <- function(){

  LSOA_population<-LSOA_to_nearest_Pharmacy
  Pharmacies_within_5km<-LSOA_to_nearest_Pharmacy
  Pharmacies_within_5km_per1000Population<- LSOA_to_nearest_Pharmacy
  Dispensing_Patients<- LSOA_to_nearest_Pharmacy
  
    map_title <- "Access to pharmacies at LSOA(2011) level"
    varlist <- c("LSOA (2011) name","Local population","Dispensing patients","Dispensing Patients (% of local population)","Distance to nearest pharmacy (km)","Number of pharmacies within 5km","Number of pharmacies within 5km per 1k population")
 
   # tmap_options(check.and.fix = TRUE)
    tmap::tmap_mode("view")
  
  map1 <- tm_shape(LSOA_to_nearest_Pharmacy) +
    tm_polygons(col = "To nearest pharmacy",
                n = 6,
                style = "quantile",
                id = "Patient LSOA",
                title = str_wrap("To nearest pharmacy (crow flies distance)",2),
                palette = "Blues",
                contrast = 1, alpha = 1,
                #borders.col = "black"
                #borders.col = NA
                lwd=0, 
                popup.var = varlist,
                popup.format=list()) +
    tm_scale_bar() +
    tm_compass(size = 3, position = c("0.85", "0.85")) 
  
  map2 <- tm_shape(LSOA_population) +
    tm_polygons(col = "Local population",
                n = 6,
                style = "quantile",
                id = "Patient LSOA",
                title = "LSOA population",
                palette = "Reds",
                contrast = 1, alpha = 1,
                #borders.col = "black"
                lwd=0, 
                popup.var = varlist,
                popup.format=list()) +
    tm_scale_bar() +
    tm_compass(size = 3, position = c("0.85", "0.85")) 
  
  map3 <- tm_shape(Pharmacies_within_5km) +
    tm_polygons(col = "NoPharm_within5km",
                n = 5,
                style = "quantile",
                id = "Patient LSOA",
                title = "Number of pharmacies within 5km",
                width =2,
                palette = "Greens",
                contrast = 1, alpha = 1,
                #borders.col = "black"
                lwd=0, 
                popup.var = varlist,
                popup.format=list()) +
    tm_scale_bar() +
    tm_compass(size = 3, position = c("0.85", "0.85")) 
  
  map4 <- tm_shape(Pharmacies_within_5km_per1000Population) +
    tm_polygons(col = "NoPharm(within5km)_per_1kPopulation",
                n = 5,
                style = "quantile",
                id = "Patient LSOA",
                title = "Number of pharmacies (within 5km) per 1000 population",
                width =2,
                palette = "Purples",
                contrast = 1, alpha = 1,
                #borders.col = "black"
                lwd=0, 
                popup.var = varlist,
                popup.format=list()) +
    tm_scale_bar() +
    tm_compass(size = 3, position = c("0.85", "0.85")) 
  
  map5 <- tm_shape(Dispensing_Patients) +
    tm_polygons(col = "Proportion of dispensing patients",
                n = 6,
                style = "quantile",
                id = "Patient LSOA",
                title = "Proportion of population registered as dispensing patients",
                width =2,
                palette = "Greens",
                contrast = 1, alpha = 1,
                #borders.col = "black"
                lwd=0, 
                popup.var = varlist,
                popup.format=list()) +
    tm_scale_bar() +
    tm_compass(size = 3, position = c("0.85", "0.85")) 
  
  #each output uses a different title argument
    map <- map5 + map4 + map1+ #map2 + map3+
      tm_layout(title = map_title,
                main.title.position = "center",
                main.title.size = 5,
                legend.title.size = 0.85,
                legend.format = list(fun = function(x) formatC(x, digits = 0, big.mark = " ", format = "f")))
  
  map
}

LSOA_patient_distance <- plot_lsoa_map()

tmap_save(LSOA_patient_distance, filename = "maps/LSOA_patients_nearestPharm_heatmap.html")

summary1<-Pharmacy_Patient_Dist2%>%
  group_by(`To nearest pharmacy`)%>%
  summarise(population_within_distance=sum(`Local population`, na.rm=T))%>%
  ungroup()%>%
  mutate(England_population=sum(population_within_distance),population_percentage=paste0(round(population_within_distance/England_population*100,1), "%"))

n_lsoa = n_distinct(Pharmacy_Patient_Dist2$`Patient LSOA`)

summary2<-Pharmacy_Patient_Dist2%>%
  group_by(`NoPharm_within5km`)%>%
  summarise(no_LSOA=n_distinct(`Patient LSOA`))%>%
  ungroup()%>%
  mutate(England_LSOA=n_lsoa, percentage=paste0(round(no_LSOA/England_LSOA*100,1), "%"))%>%
  rename(`Number of pharamcies within 5km (crow flies distance`=`NoPharm_within5km`, `Number of LSOA`=no_LSOA)

summary3<-Pharmacy_Patient_Dist2%>%
  group_by(`Patient LSOA`)%>%
  summarise(`Total Population`=sum(`Local population`, na.rm=T), 
            `Total dispensing patients`=sum(as.numeric(`Dispensing patients`), na.rm=T))
 

dataset_names<- list('LSOA_summary1'=summary1
                     ,'LSOA_summary2'=summary2
                     ,'LSOA_dispensing_patients'=summary3
)


openxlsx::write.xlsx(dataset_names, file = paste0('maps/heatmap_summary_table_', Sys.Date(), '.xlsx')) 

