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

co_ords_data <- read_csv(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ONSPD_NOV_2022_UK.csv")) %>%
  select(pcds,
         oseast1m,
         osnrth1m)

closure_postcode_coords<-function(){
  
  pharm_list_most_recent <- pharm_active %>%
    filter(until>=(max(until)%m-% months(12))) %>% # changed in this version to compare against same time in previous year
    rename(ODS.CODE=FCode,pcds = postcode)
  
  
  
  pharmacy_postcode_coords <- pharm_list_most_recent %>%
    left_join(co_ords_data, by = "pcds") %>%
    rename(ODS.Code = ODS.CODE,
           postcode = pcds)
  
  pharmacy_postcode_coords
}

hours_postcode_coords<-function(){
  
  pharm_list_most_recent <-pharm_list%>%
    filter(SnapshotMonth==(max(SnapshotMonth)%m-% months(12))) %>% # changed in this version to compare against same time in previous year
    rename(pcds = PostCode, total=`Weekly Total`, Month=SnapshotMonth)%>%
    select(FCode, pcds,total, Month, HWB, `Pharmacy Trading Name`, `100hourPharmacy`,`IMD_Decile`,`ContractType`,`Short_ICB_Name`,`Region_Name` )
  
  pharm_list1<-pharm_list%>%filter(SnapshotMonth==max(SnapshotMonth)) %>% 
    rename(total=`Weekly Total`, Month=SnapshotMonth)%>%select(FCode, Month, total)
  
  pharm_list_most_recent<-pharm_list_most_recent%>%
    right_join(pharm_list1, "FCode")%>%
    mutate(change=(total.y-total.x)/total.x) 
  
  
  pharmacy_postcode_coords <- pharm_list_most_recent %>%
    left_join(co_ords_data, by = "pcds") %>%
    rename(ODS.Code = FCode,
           postcode = pcds)
  
  pharmacy_postcode_coords
  
}

income_postcode_coords<-function(Q="FY2022/23 Q2", income_type ="DispensingFee"){
  
  pharm_list_most_recent <-pharm_list%>%
    filter(SnapshotMonth==max(SnapshotMonth)) %>%
    rename(pcds = PostCode)%>%
    select(FCode, pcds, `Pharmacy Trading Name`, `HWB`,`ContractType`,`100hourPharmacy`,`Short_ICB_Name`,`Region_Name`,`IMD_Decile`)%>%
    distinct()
  
  Q_p<-paste0(substr(`Q`,1,4), as.character(as.numeric(substr(`Q`,5,6))-1), "/", substr(`Q`,5,6),substr(`Q`,10,12))
  
  data1<-Pharm_income%>%filter(FY_Qs== Q,type==income_type)%>%select(FCode,FY_Qs, Income,type)
  data2<-Pharm_income%>%filter(FY_Qs== Q_p,type==income_type)%>%select(FCode,FY_Qs, Income,type)
  
  data<-data1%>%
    left_join(data2, c("FCode", "type"))%>%
    mutate(change = (Income.x-Income.y)/Income.y)%>%
    left_join(pharm_list_most_recent, "FCode")
  
  income_postcode_coords <- data %>%
    left_join(co_ords_data, by = "pcds") %>%
    rename(ODS.Code = FCode,
           postcode = pcds)
  
  income_postcode_coords
  
}


################################################################################
create_map_latest_month <- function(content="closure"){
  
  if(content=="closure"){
    #get data into right format
    data <- closure_postcode_coords()  %>%
      mutate(colour = case_when(!is.na(closed) ~ "Red",
                                `Fcode_change` == "YES"~ "Amber",
                                TRUE ~ "Green")) %>%
      ungroup()%>%
      filter(!is.na(postcode) & !is.na(oseast1m)) 
    
    data$Short_ICB_Name<-ifelse(is.na(data$Short_ICB_Name), data$`STP Code`, data$Short_ICB_Name)
    
    #make into shape file
    data_sf <- sf::st_as_sf(x = data, coords = c("oseast1m", "osnrth1m"), crs = 27700)
    
    #bring red dots to the front
    data_sf$colour <- factor(data_sf$colour, levels = c("Green", "Amber", "Red"))
    data_sf <- dplyr::arrange(data_sf, colour)
    
    palette_name<-c(Green='green', Amber ='yellow',  Red='red')
    label_name <- c("Still active", "Changed ownership", "Permanently closed")
    title1 =paste0("Permanent Closures and change of ownerships between ", as.character(format(max(pharm_active$until)%m-% months(12),'%B %Y'))," and ", as.character(format(max(pharm_active$until),'%B %Y'))," (Pharmaceutical List ", as.character(format(max(pharm_active$until),'%b-%Y')), " publication)" )
    
    data_sf<-data_sf%>%rename(`ODS code` = `ODS.Code`,`FCode changed to` = `FCode_new`,`ICB`= `Short_ICB_Name`)
    
    var_list <- c("ODS code","FCode changed to", "HWB", "ICB", "Region_Name", "IMD_Decile")
  }
  else if (content =="hours"){
    data <- hours_postcode_coords()  %>%
      mutate(colour = case_when(change<= -0.05 ~ "Red",
                                change> -0.05 & change < 0 ~ "Amber",
                                change>=0 ~ "Green")) %>%
      ungroup()%>%
      filter(!is.na(postcode) & !is.na(oseast1m)) 
    
    
    #make into shape file
    data_sf <- sf::st_as_sf(x = data, coords = c("oseast1m", "osnrth1m"), crs = 27700)
    
    #bring red dots to the front
    data_sf$colour <- factor(data_sf$colour, levels = c("Green","Amber", "Red"))
    data_sf <- dplyr::arrange(data_sf, colour)
    
    palette_name<-c(Green='green',  Amber ='yellow',  Red='red')
    label_name <- c("Increased opening hours or no change", "Reduced hours and reduction is less than 5%", "Reduced more than 5% of opening hours")
    title1 =paste0("Reduced weekly total opening hours between ", as.character(format(max(pharm_active$until)%m-% months(12),'%B %Y'))," and ", as.character(format(max(pharm_active$until),'%B %Y'))," (Pharmaceutical List ", as.character(format(max(pharm_active$until),'%b-%Y')), " publication)")
    
    data_sf<-data_sf%>%rename(`ODS code` = `ODS.Code`,`Pharmacy Name`=`Pharmacy Trading Name`,`ICB`= `Short_ICB_Name`,`Previous Weekly Total hours`= `total.x`, `Current Weekly Total hours`= `total.y`)
    
    var_list <- c("ODS code","Pharmacy Name", "ContractType","100hourPharmacy", "HWB", "ICB", "Region_Name", "IMD_Decile", "Previous Weekly Total hours", "Current Weekly Total hours")
    
  }
  else {
    q= "FY2023/24 Q1" ##need updating
    data <- income_postcode_coords(q, content)  %>%
      mutate(colour = case_when(change<= -0.25 ~ "Red",
                                change>=0.25 ~ "Green",
                                TRUE ~ "Amber")) %>%
      ungroup()%>%
      filter(!is.na(postcode) & !is.na(oseast1m)) 
    
    
    #make into shape file
    data_sf <- sf::st_as_sf(x = data, coords = c("oseast1m", "osnrth1m"), crs = 27700)
    
    #bring red dots to the front
    data_sf$colour <- factor(data_sf$colour, levels = c("Green","Amber", "Red"))
    data_sf <- dplyr::arrange(data_sf, colour)
    
    palette_name<-c(Green='green',  Amber ='yellow',  Red='red')
    label_name <- c("Total income increased 25% or more", "Income remained stable (change less than 25%)", "Total income reduced 25% or more")
    title1 =paste0("Quarterly total ", content, " per pharamcy in ", q, " in comparison to same quarter in previous year" )
    
    data_sf<-data_sf%>%rename(`ODS code` = `ODS.Code`,`Pharmacy Name`=`Pharmacy Trading Name`,`ICB`= `Short_ICB_Name`,`Income for this quarter last year`= `Income.x`, `Income for this quarter this year`= `Income.y`)
    
    var_list <- c("ODS code","Pharmacy Name", "ContractType","100hourPharmacy", "HWB", "ICB", "Region_Name", "IMD_Decile", "Income for this quarter last year", "Income for this quarter this year")
    
  }
  
  
  
  #get boundaries geopackage
  ICB_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ICB_boundaries.gpkg"))
  region_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/Region_boundaries.gpkg"))
  `ICB boundaries` <- st_make_valid(ICB_boundaries)
  
  #plot map
  tmap::tmap_mode("view")
  #tmap::tmap_options(check.and.fix = TRUE)
  
  tm_shape(data_sf,
           bbox = st_bbox(c(xmin =-7.57216793459, xmax = 1.68153079591, ymax = 58.6350001085, ymin = 49.959999905), crs = st_crs(4326))) +
    tm_dots(col = "colour",
            palette = palette_name,
            labels = label_name,
            title = "Colour",
            popup.vars = var_list,
            legend.show = TRUE) +
    tm_shape(`ICB boundaries`,
             labels = "ICB boundaries") +
    tm_borders(col = "grey40", lwd = 2, lty = "solid", alpha = 0.5) +
    tm_layout(title = title1) +
    tm_scale_bar(position =c("left", "bottom"))
  
}


perm_closure <- create_map_latest_month("closure")
hours<- create_map_latest_month("hours")
NHSincome<- create_map_latest_month("NHS_Income")
DispensingFee<- create_map_latest_month("DispensingFee")
CPCFincome<- create_map_latest_month("CPCFIncome")

tmap_save(perm_closure, filename = "maps/PermanentClosures_pharmacy_map.html")
tmap_save(hours, filename = "maps/ReducedOpeningHours_pharmacy_map.html")
tmap_save(NHSincome, filename = "maps/ReducedNHSincome_pharmacy_map.html")
tmap_save(CPCFincome, filename = "maps/ReducedCPCFIncome_pharmacy_map.html")
tmap_save(DispensingFee, filename = "maps/ReducedDispensingFee_pharmacy_map.html")

