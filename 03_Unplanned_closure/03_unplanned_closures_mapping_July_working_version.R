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

unplannedClosuresData <- readRDS("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/Data files for monthly pack/unplannedClosuresData.rds")
#all_data

full_pharmlist <- pull_pharm_list()


################################################################################
get_working_hours_per_month <- function(data = unplannedClosuresData,
                                        pharm_list = full_pharmlist, 
                                        latest_month){
  
  #get most recent pharm list
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth))
  
  most_recent_month_start_date <- lubridate::floor_date(latest_month, unit = "months")#max(data$month_of_closure)
  most_recent_month_end_data <- most_recent_month_start_date + lubridate::period("1 month") - lubridate::period("1 day")
  days_in_month <- data.frame(date = seq(from = most_recent_month_start_date, to = most_recent_month_end_data, by = "days"))

  weekdays_in_month <- days_in_month %>%
    mutate(day_of_week = weekdays(date)) %>%
    group_by(day_of_week) %>%
    count()

  pharm_list <- pharm_list %>%
    mutate(total_monthly_Monday_hours = Mon.Total * dplyr::filter(weekdays_in_month, day_of_week == "Monday")$n) %>%
    mutate(total_monthly_Tuesday_hours = Tues.Total * dplyr::filter(weekdays_in_month, day_of_week == "Tuesday")$n) %>%
    mutate(total_monthly_Wednesday_hours = Wed.Total * dplyr::filter(weekdays_in_month, day_of_week == "Wednesday")$n) %>%
    mutate(total_monthly_Thursday_hours = Thurs.Total * dplyr::filter(weekdays_in_month, day_of_week == "Thursday")$n) %>%
    mutate(total_monthly_Friday_hours = Fri.Total * dplyr::filter(weekdays_in_month, day_of_week == "Friday")$n) %>%
    mutate(total_monthly_Saturday_hours = Sat.Total * dplyr::filter(weekdays_in_month, day_of_week == "Saturday")$n) %>%
    mutate(total_monthly_Sunday_hours = Sun.Total * dplyr::filter(weekdays_in_month, day_of_week == "Sunday")$n) %>%
    rowwise() %>%
    mutate(total_opening_hours_this_month = sum(total_monthly_Monday_hours, total_monthly_Tuesday_hours,
                                                total_monthly_Wednesday_hours, total_monthly_Thursday_hours,
                                                total_monthly_Friday_hours, total_monthly_Saturday_hours,
                                                total_monthly_Sunday_hours, na.rm = TRUE)) %>%
    select(-total_monthly_Monday_hours,
           -total_monthly_Tuesday_hours,
           -total_monthly_Wednesday_hours,
           -total_monthly_Thursday_hours,
           -total_monthly_Friday_hours,
           -total_monthly_Saturday_hours,
           -total_monthly_Sunday_hours) %>%
    select(ODS.CODE, total_opening_hours_this_month)
  
  # print(paste("start date ", most_recent_month_start_date))
  # print(paste("end date ", most_recent_month_end_data))
}

################################################################################
add_total_opening_hours_to_pharm_list <- function(data = unplannedClosuresData,
                                                  pharm_list = full_pharmlist, 
                                                  latest_month){
  #get most recent pharm list
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth))
  
  latest_month_hours <- get_working_hours_per_month(data = data,
                                                    pharm_list = pharm_list,
                                                    latest_month = latest_month)
  
  previous_month_hours <- get_working_hours_per_month(data = data,
                                                      pharm_list = pharm_list,
                                                      latest_month = latest_month - lubridate::duration("4 weeks"))
  
  previous_month_hours <- previous_month_hours %>%
    rename(total_opening_hours_previous_month = total_opening_hours_this_month)
  
  previous_2month_hours <- get_working_hours_per_month(data = data,
                                                       pharm_list = pharm_list,
                                                       latest_month = latest_month - lubridate::duration("8 weeks"))
  
  previous_2month_hours <- previous_2month_hours %>%
    rename(total_opening_hours_previous_2month = total_opening_hours_this_month)
  
  pharm_list <- pharm_list %>%
    left_join(latest_month_hours, by = "ODS.CODE") %>%
    left_join(previous_month_hours, by = "ODS.CODE") %>%
    left_join(previous_2month_hours, by = "ODS.CODE") %>%
    rowwise() %>%
    mutate(total_opening_hours_last_three_months = sum(total_opening_hours_this_month, total_opening_hours_previous_month, total_opening_hours_previous_2month, na.rm = TRUE)) 

}

################################################################################
# get_pharmacy_coordinates <- function(){
#   
#   pharmacy_postcode_coords <- readRDS("~/R-Projects/Unplanned-pharmacy-closures/data/geo_spatial_data/pharmacy_postcode_coords.rds")
#   
#   Unplanned_closures_durations_pharmacy_level <- read_excel("data/mapping/Unplanned_closures_durations_pharmacy_level.xlsx")
#   
#   names(Unplanned_closures_durations_pharmacy_level) <- names(Unplanned_closures_durations_pharmacy_level) %>% make.names()
#   
#   pharmacy_postcode_coords  <- pharmacy_postcode_coords  %>%
#     select(ODS.Code = contractor_code,
#            postcode,
#            oseast1m,
#            osnrth1m)
# }

################################################################################

#Load in co-ordinates data and join to Pharmlist

 pharm_list_most_recent <- full_pharmlist %>%
   dplyr::filter(SnapshotMonth == '2023-09-01') %>%
   select(ODS.CODE,
          postcode) %>%
   rename(pcds = postcode)

 co_ords_data <- read_csv(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ONSPD_NOV_2022_UK.csv")) %>%
   select(pcds,
          oseast1m,
          osnrth1m)

    pharmacy_postcode_coords <- pharm_list_most_recent %>%
    left_join(co_ords_data, by = "pcds") %>%
    rename(ODS.Code = ODS.CODE,
             postcode = pcds)




get_closure_map_data <- function(data = unplannedClosuresData,
                                 pharm_list = full_pharmlist, 
                                 latest_month_or_3_months = "Lastest month"){
  
  #get most recent pharm list
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth))
  
  #add columns to pharm list
  pharm_list <- add_total_opening_hours_to_pharm_list(data = data,
                                                      pharm_list = pharm_list,
                                                      latest_month = max(data$month_of_closure))
  
  pharm_list <- pharm_list %>%
    select(ODS.CODE, 
           ParentOrgName = Organisation.Name, 
           ICB.Name,
           Pharmacy.Opening.Hours.Monday,
           Pharmacy.Opening.Hours.Tuesday,
           Pharmacy.Opening.Hours.Wednesday,
           Pharmacy.Opening.Hours.Thursday,
           Pharmacy.Opening.Hours.Friday,
           Pharmacy.Opening.Hours.Saturday,
           Pharmacy.Opening.Hours.Sunday, 
           total_opening_hours_this_month,
           total_opening_hours_last_three_months,
           ICB.Name,
           X100.Hour.Pharmacy)
  
  #get rid of errors and convert duration format
  data <- data %>%
    dplyr::filter(DURATION.OF.CLOSURE < 1000) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) 
  
  data <- data %>%
    group_by(month_of_closure, Region_Name, STP.Name, ParentOrgName, ODS.CODE) %>%
    summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) 
  
  data <- data %>%
    #dplyr::filter(!is.na(STP.Name)) %>%
    dplyr::filter(month_of_closure >= lubridate::floor_date(Sys.Date() - lubridate::weeks(12), unit = "month")) #filter to get most recent three months of data
  
  #get totals over three months
  data_totals <- data %>%
    group_by(ODS.CODE) %>%
    summarise(total_closure_duration_last_3_months = sum(closure_duration_hours, na.rm = TRUE))
  
  data <- data %>%
    mutate(month_of_closure = format(month_of_closure, "%b-%Y")) %>%
    pivot_wider(names_from = month_of_closure,
                values_from = closure_duration_hours,
                values_fill = 0,
                names_prefix = "Closure duration ") %>%
    left_join(data_totals, by = "ODS.CODE") %>%
    full_join(pharm_list, by = "ODS.CODE") %>%
    mutate(ParentOrgName = case_when(is.na(ParentOrgName.x) ~ ParentOrgName.y,
                                         TRUE ~ ParentOrgName.x)) %>%
    select(ODS.Code = ODS.CODE,
           `Organisation Name` = ParentOrgName,
           `ICB Name` = ICB.Name,
           `Region Name` = Region_Name,
           `100 hour pharmacy?` = X100.Hour.Pharmacy,
           everything()) %>%
    select(-c(ParentOrgName.x, ParentOrgName.y)) %>%
    mutate(total_closure_duration_last_3_months = if_else(is.na(total_closure_duration_last_3_months),
                                                          0,
                                                          total_closure_duration_last_3_months)) %>%
    mutate(`Closure duration Sep-2025` = if_else(is.na(`Closure duration Sep-2025`),
                                                 0,
                                                 `Closure duration Sep-2025`))
  
  if(latest_month_or_3_months == "Latest month"){

    data <- data %>%
      mutate(perc_hours_lost_to_closure_last_month = `Closure duration Sep-2025` * 100 / total_opening_hours_this_month) 
    
    # upper_quartile <- summary(dplyr::filter(data, perc_hours_lost_to_closure_last_month != 0)$perc_hours_lost_to_closure_last_month)
    # upper_quartile <- as.numeric(upper_quartile["3rd Qu."])
    # 
    # data <- data  %>%
    #   mutate(colour = case_when(perc_hours_lost_to_closure_last_month == 0 ~ "Green",
    #                             perc_hours_lost_to_closure_last_month <= upper_quartile ~ "Amber",
    #                             perc_hours_lost_to_closure_last_month > upper_quartile ~ "Red",
    #                             TRUE ~ "Invalid closure duration")) %>%
    #   rename(`Percentage of opening hours lost due to closure` = perc_hours_lost_to_closure_last_month) %>%
    #   ungroup()
    data <- data  %>%
      mutate(colour = case_when(`Closure duration Sep-2025` == 0 ~ "Green",
                                `Closure duration Sep-2025` <= 8 ~ "Amber",
                                `Closure duration Sep-2025` > 8 ~ "Red",
                                TRUE ~ "Invalid closure duration")) %>%
      rename(`Percentage of opening hours lost due to closure` = perc_hours_lost_to_closure_last_month) %>%
      ungroup()
    
  }else{
    data <- data %>%
      mutate(perc_hours_lost_to_closure_last_3_months = total_closure_duration_last_3_months * 100 / total_opening_hours_last_three_months)
    
    data <- data  %>%
      mutate(colour = case_when(total_closure_duration_last_3_months == 0 ~ "Green",
                                total_closure_duration_last_3_months <= 24 ~ "Amber",
                                total_closure_duration_last_3_months > 24 ~ "Red",
                                TRUE ~ "Invalid closure duration")) %>%
      rename(`Percentage of opening hours lost due to closure` = perc_hours_lost_to_closure_last_3_months) %>%
      ungroup()
    
  }
  
  
  
  #get pharm coords
  pharm_coords <- pharmacy_postcode_coords

  #filter out pharmacies with no postcode data - means that they are not on the pharm list and must have closed
  #There are three pharmacies with postcodes that do not map to an ONS coordinate
  data <- data %>%
    left_join(pharm_coords, by = "ODS.Code") %>%
    dplyr::filter(!is.na(postcode) & !is.na(oseast1m)) 
}



################################################################################
create_map_latest_month <- function(data = unplannedClosuresData,
                                    pharm_list = full_pharmlist
                                    ){
  
  #get most recent pharm list
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth))

  #filter out pharmacies no longer in the pharm list 
  data <- data %>%
    dplyr::filter(ODS.CODE %in% pharm_list$ODS.CODE)
    
  #get data into right format
  data <- get_closure_map_data(data = data,
                               pharm_list = pharm_list,
                               latest_month_or_3_months = "Latest month")
  
  #round percentages for display
  data <- data %>%
    mutate(`Percentage of opening hours lost due to closure` = round(`Percentage of opening hours lost due to closure`, 1))
  
  #make into shape file
  closures_sf <- sf::st_as_sf(x = data, coords = c("oseast1m", "osnrth1m"), crs = 27700)

  #bring red dots to the front
  closures_sf$colour <- factor(closures_sf$colour, levels = c("Green", "Amber", "Red"))
  closures_sf <- dplyr::arrange(closures_sf, colour)
  
  #get data just for 100hr pharms
  `100 hour pharmacies` <- closures_sf %>%
    dplyr::filter(`100 hour pharmacy?` == "Yes")
  
  #get data just for 40hr pharms
  `40 hour pharmacies` <- closures_sf %>%
    dplyr::filter(`100 hour pharmacy?` == "No")
  
  #get boundaries geopackage
  ICB_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ICB_boundaries.gpkg"))
  region_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/Region_boundaries.gpkg"))
  `ICB boundaries` <- st_make_valid(ICB_boundaries)
  
  #plot map
  tmap::tmap_mode("view")
  #tmap::tmap_options(check.and.fix = TRUE)

    tm_shape(`100 hour pharmacies`,
             bbox = st_bbox(c(xmin =-7.57216793459, xmax = 1.68153079591, ymax = 58.6350001085, ymin = 49.959999905), crs = st_crs(4326))) +
    tm_dots(col = "colour",
            palette = c(Amber ='yellow',  Red='red',Green='green'),
            labels = c("No reported closures", "Total closure duration < 8 hours", "Total closure duration > 8 hours"),
            title = "Colour",
            popup.vars = c("ODS code" = "ODS.Code",
                           "Org name" = "Organisation Name",
                           "ICB" = "ICB Name",
                           "Pharmacy Opening Hours Monday" = "Pharmacy.Opening.Hours.Monday",
                           "Pharmacy Opening Hours Tuesday" = "Pharmacy.Opening.Hours.Tuesday",
                           "Pharmacy Opening Hours Wednesday" = "Pharmacy.Opening.Hours.Wednesday",
                           "Pharmacy Opening Hours Thursday" = "Pharmacy.Opening.Hours.Thursday",
                           "Pharmacy Opening Hours Friday" = "Pharmacy.Opening.Hours.Friday",
                           "Pharmacy Opening Hours Saturday" = "Pharmacy.Opening.Hours.Saturday",
                           "Pharmacy Opening Hours Sunday" = "Pharmacy.Opening.Hours.Sunday",
                           "100 hour pharmacy?" = "100 hour pharmacy?",
                           "Total possible opening hours this month" = "total_opening_hours_this_month",
                           "Total number of hours lost due closure this month" = "Closure duration Sep-2025",
                           "Percentage of opening hours lost due to closure (%)" = "Percentage of opening hours lost due to closure"),
            legend.show = FALSE) +
    tm_shape(`40 hour pharmacies`,
             bbox = st_bbox(c(xmin =-7.57216793459, xmax = 1.68153079591, ymax = 58.6350001085, ymin = 49.959999905), crs = st_crs(4326))) +
    tm_dots(col = "colour",
            palette = c(Amber ='yellow',  Red='red',Green='green'),
            labels = c("No reported closures", "Total closure duration < 8 hours", "Total closure duration > 8 hours"),
            popup.vars = c("ODS code" = "ODS.Code",
                           "Org name" = "Organisation Name",
                           "ICB" = "ICB Name",
                           "Pharmacy Opening Hours Monday" = "Pharmacy.Opening.Hours.Monday",
                           "Pharmacy Opening Hours Tuesday" = "Pharmacy.Opening.Hours.Tuesday",
                           "Pharmacy Opening Hours Wednesday" = "Pharmacy.Opening.Hours.Wednesday",
                           "Pharmacy Opening Hours Thursday" = "Pharmacy.Opening.Hours.Thursday",
                           "Pharmacy Opening Hours Friday" = "Pharmacy.Opening.Hours.Friday",
                           "Pharmacy Opening Hours Saturday" = "Pharmacy.Opening.Hours.Saturday",
                           "Pharmacy Opening Hours Sunday" = "Pharmacy.Opening.Hours.Sunday",
                           "100 hour pharmacy?" = "100 hour pharmacy?",
                           "Total possible opening hours this month" = "total_opening_hours_this_month",
                           "Total number of hours lost due closure this month" = "Closure duration Sep-2025",
                           "Percentage of opening hours lost due to closure (%)" = "Percentage of opening hours lost due to closure"),
            legend.show = TRUE) +
    tm_shape(`ICB boundaries`,
             labels = "ICB boundaries") +
    tm_borders(col = "grey40", lwd = 2, lty = "solid", alpha = 0.5) +
    tm_layout(title = 'Oct 2024 - Unplanned Pharmacy Closures') +
    tm_scale_bar(position =c("left", "bottom"))

}

################################################################################
create_map_latest_3_months <- function(data = unplannedClosuresData,
                                       pharm_list = full_pharmlist
                                       ){
  
  #get most recent pharm list
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth))
  
  #filter out pharmacies no longer in the pharm list 
  data <- data %>%
    dplyr::filter(ODS.CODE %in% pharm_list$ODS.CODE)
  
  #get data into right format
  data <- get_closure_map_data(data = data,
                               pharm_list = pharm_list,
                               latest_month_or_3_months = "3 months")
  
  #round percentages for display
  data <- data %>%
    mutate(`Percentage of opening hours lost due to closure` = round(`Percentage of opening hours lost due to closure`, 1))
  
  #make into shape file
  closures_sf <- sf::st_as_sf(x = data, coords = c("oseast1m", "osnrth1m"), crs = 27700)
  
  #bring red dots to the front
  closures_sf$colour <- factor(closures_sf$colour, levels = c("Green", "Amber", "Red"))
  closures_sf <- dplyr::arrange(closures_sf, colour)
  
  #get data just for 100hr pharms
  `100 hour pharmacies` <- closures_sf %>%
    dplyr::filter(`100 hour pharmacy?` == "Yes")
  
  #get data just for 40hr pharms
  `40 hour pharmacies` <- closures_sf %>%
    dplyr::filter(`100 hour pharmacy?` == "No")

  
  #get boundaries geopackage
  ICB_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ICB_boundaries.gpkg"))
  region_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/Region_boundaries.gpkg"))
  `ICB boundaries` <- st_make_valid(ICB_boundaries)
  
  #plot map
  tmap::tmap_mode("view")
  #tmap::tmap_options(check.and.fix = TRUE)
  
  tm_shape(`100 hour pharmacies`) +
    tm_dots(col = "colour",
            palette = c(Amber ='yellow',  Red='red',Green='green'),
            labels = c("No reported closures", "Total closure duration < 24 hours", "Total closure duration > 24 hours"),
            title = "Colour",
            popup.vars = c("ODS code" = "ODS.Code",
                           "Org name" = "Organisation Name",
                           "ICB" = "ICB Name",
                           "Pharmacy Opening Hours Monday" = "Pharmacy.Opening.Hours.Monday",
                           "Pharmacy Opening Hours Tuesday" = "Pharmacy.Opening.Hours.Tuesday",
                           "Pharmacy Opening Hours Wednesday" = "Pharmacy.Opening.Hours.Wednesday",
                           "Pharmacy Opening Hours Thursday" = "Pharmacy.Opening.Hours.Thursday",
                           "Pharmacy Opening Hours Friday" = "Pharmacy.Opening.Hours.Friday",
                           "Pharmacy Opening Hours Saturday" = "Pharmacy.Opening.Hours.Saturday",
                           "Pharmacy Opening Hours Sunday" = "Pharmacy.Opening.Hours.Sunday",
                           "100 hour pharmacy?" = "100 hour pharmacy?",
                           "Total possible opening hours over the last three months" = "total_opening_hours_last_three_months",
                           "Total number of hours lost due closure over the last three months" = "total_closure_duration_last_3_months",
                           "Percentage of opening hours lost due to closure (%)" = "Percentage of opening hours lost due to closure"),
            legend.show = FALSE) +
    tm_shape(`40 hour pharmacies`) +
    tm_dots(col = "colour",
            palette = c(Amber ='yellow',  Red='red',Green='green'),
            labels = c("No reported closures", "Total closure duration < 24 hours", "Total closure duration > 24 hours"),
            popup.vars = c("ODS code" = "ODS.Code",
                           "Org name" = "Organisation Name",
                           "ICB" = "ICB Name",
                           "Pharmacy Opening Hours Monday" = "Pharmacy.Opening.Hours.Monday",
                           "Pharmacy Opening Hours Tuesday" = "Pharmacy.Opening.Hours.Tuesday",
                           "Pharmacy Opening Hours Wednesday" = "Pharmacy.Opening.Hours.Wednesday",
                           "Pharmacy Opening Hours Thursday" = "Pharmacy.Opening.Hours.Thursday",
                           "Pharmacy Opening Hours Friday" = "Pharmacy.Opening.Hours.Friday",
                           "Pharmacy Opening Hours Saturday" = "Pharmacy.Opening.Hours.Saturday",
                           "Pharmacy Opening Hours Sunday" = "Pharmacy.Opening.Hours.Sunday",
                           "100 hour pharmacy?" = "100 hour pharmacy?",
                           "Total possible opening hours over the last three months" = "total_opening_hours_last_three_months",
                           "Total number of hours lost due closure over the last three months" = "total_closure_duration_last_3_months",
                           "Percentage of opening hours lost due to closure (%)" = "Percentage of opening hours lost due to closure"),
            legend.show = TRUE) +
    tm_shape(`ICB boundaries`,
             labels = "ICB boundaries") +
    tm_borders(col = "grey40", lwd = 2, lty = "solid", alpha = 0.5) +
    tm_layout(title = 'Three month summary: to Oct 2024 - Unplanned Pharmacy Closures') +
    tm_scale_bar(position =c("left", "bottom"))
  
}

tm_this_month <- 
  create_map_latest_month()
tm_latest_3_months <- create_map_latest_3_months()
tmap_save(tm_this_month, filename = "maps/unplanned_closures_pharmacy_map_Oct2024.html")
tmap_save(tm_latest_3_months, filename = "maps/unplanned_closures_pharmacy_map_Oct2024_3_month_summary.html")
