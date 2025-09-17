# install.packages("sf")
# install.packages("tmap")
# install.packages("dplyr")
# install.packages("readr")
# install.packages("stringr")

library(tidyverse)
library(sf) 
library(tmap)
library(dplyr)
library(readr)
library(stringr)
library(tmap)
library(readxl)
#https://cran.r-project.org/web/packages/tmap/vignettes/tmap-getstarted.html

#Load in co-ordinates data and join to Pharmlist

co_ords_data <- read_csv("N:/_Everyone/Primary Care Group/Unplanned Closures PHARM-2022_23-003/Data files for monthly pack/ONSPD_NOV_2022_UK.csv") %>%
  select(pcds,
         oseast1m,
         osnrth1m)

DSP<-read.xlsx("N:/_Everyone/Primary Care Group/Pharmacy SMT data pack/Pharm_O_C_R/Data for DSPs mapping.xlsx", sheet= "Jun24PL")


pharmacy_postcode_coords <- DSP %>%
  rename(`pcds`=`Post.Code`)%>%
  left_join(co_ords_data, by = "pcds") %>%
  rename(`Pharmacy ODS code (F-code)` = `Pharmacy.ODS.Code.(F-Code)`,
         `Health and Wellbeing Board` = `Health.and.Wellbeing.Board`,
         `Pharmacy Trading Name`= `Pharmacy.Trading.Name`,
         `Organisation Name`=`Organisation.Name`,
         `Postcode`=`pcds`,
         `Contractor Type`=DSP)%>%
  collect()

DSP_postcode_coords <- pharmacy_postcode_coords %>%
  filter(`Contractor Type`=="DSP")%>%
  collect()

################################################################################
create_map <- function(){
  
  
  #get data into right format
  data1 <- pharmacy_postcode_coords %>%
    filter(is.na(`Contractor Type`))%>%
    mutate(colour = "Green") %>%
    ungroup()%>%
    filter(!is.na(Postcode) & !is.na(oseast1m)) 
  
  data2<- DSP_postcode_coords%>%
    mutate(colour = "Red") %>%
    ungroup()%>%
    filter(!is.na(Postcode) & !is.na(oseast1m)) 
  
  #make into shape file
  Bricks_and_Mortar <- sf::st_as_sf(x = data1, coords = c("oseast1m", "osnrth1m"), crs = 27700)
  DSP<- sf::st_as_sf(x = data2, coords = c("oseast1m", "osnrth1m"), crs = 27700)
  
  palette_name1<-c(Green='green')
  label_name1 <- c("Bricks & Mortar Pharmacies")
  title1 =paste0("Distance Selling Pharmacies (DSP) in Sep24 Pharmaceutical List" )
  
  palette_name2<-c( Red='red')
  label_name2 <- c("Distance Selling Pharmacies")
  
  
  Bricks_and_Mortar<-Bricks_and_Mortar%>%
    mutate(`Contractor Type`="Bricks & Mortar")%>%collect()
  
  var_list <- c("Pharmacy ODS code (F-code)","Health and Wellbeing Board", "Pharmacy Trading Name", "Organisation Name", "Address.Field.1", "Address.Field.2", "Address.Field.3", "Address.Field.4","Postcode", "Contractor Type")
  
  #get boundaries geopackage
  ICB_boundaries <- sf::st_read("N:/_Everyone/Primary Care Group/Unplanned Closures PHARM-2022_23-003/Data files for monthly pack/ICB_boundaries.gpkg")
  region_boundaries <- sf::st_read("N:/_Everyone/Primary Care Group/Unplanned Closures PHARM-2022_23-003/Data files for monthly pack/Region_boundaries.gpkg")
  `ICB boundaries` <- st_make_valid(ICB_boundaries)
  
  #plot map
  tmap::tmap_mode("view")
  #tmap::tmap_options(check.and.fix = TRUE)
  
  tm_shape(Bricks_and_Mortar,
           bbox = st_bbox(c(xmin =-7.57216793459, xmax = 1.68153079591, ymax = 58.6350001085, ymin = 49.959999905), crs = st_crs(4326))) +
    tm_dots(col = "colour",
            palette = palette_name1,
            labels = label_name1,
            title = "",
            popup.vars = var_list,
            legend.show = TRUE) +
    tm_shape(DSP,
             bbox = st_bbox(c(xmin =-7.57216793459, xmax = 1.68153079591, ymax = 58.6350001085, ymin = 49.959999905), crs = st_crs(4326))) +
    tm_dots(col = "colour",
            palette = palette_name2,
            labels = label_name2,
            title="",
            popup.vars = var_list,
            legend.show = TRUE) +
    tm_shape(`ICB boundaries`,
             labels = "ICB boundaries") +
    tm_borders(col = "grey40", lwd = 2, lty = "solid", alpha = 0.5) +
    tm_layout(title = title1) +
    tm_scale_bar(position =c("left", "bottom"))
  
}


tmap<- create_map()

tmap_save(tmap, filename = "maps/DSP_Sep24PL_map.html")


