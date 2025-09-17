library(tidyverse)
library(DBI)

#################################################################################
#load in data

items_dispensed <- pull_raw_items_dispensed()
Ref_Contractor <- pull_ref_contractor()

#most recent pharm list
full_pharmlist <-pull_pharm_list() 

pharmlist<-full_pharmlist%>%
  dplyr::filter(SnapshotMonth == max(SnapshotMonth))%>%
  left_join(Ref_Contractor, "ODS.CODE")%>%
  select(`ODS.CODE`, `Organisation Name`=`Organisation.Name`, `STP Name`=`STP.Name`, `Region_Name`)%>%
  collect()

Ref_PharmList1<-pharmlist%>%select(`ODS CODE`=`ODS.CODE`, `Organisation Name`,`STP Name`)

unplannedClosuresData <- readRDS("~/Rprojects/PhOD_pharmacy_reporting_simplified/03_Unplanned_closure/Data files for monthly pack/unplannedClosuresData.rds")

#filter out DAC closures 
unplannedClosuresData <- unplannedClosuresData %>%
  dplyr::filter(ContractType != "DAC")

eps_nominations <- read_excel(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/eps_nom_report+220722.xlsx"), 
                              sheet = "Dispenser Nominations")
eps_nominations <- eps_nominations %>%
  select(ODS.CODE = `ODS Code`, eps_nominations = "Current Active Nominations")

nearest_pharm <- readRDS(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/nearest_pharm.rds"))


################################################################################
plot_closures_by_month <- function(data = unplannedClosuresData,
                                   level = "National",
                                   region_STP_name = NULL,
                                   per_100_pharms = FALSE, 
                                   plotChart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  pharm_list <- full_pharmlist %>%
    dplyr::filter(SnapshotMonth == max(full_pharmlist$SnapshotMonth)) %>%
    select(`ODS CODE` = ODS.CODE,
           `Organisation Name` = Organisation.Name,
           `STP Name` = ICB.Name,
           STP.Code)
  
  
  if(level == "National" & per_100_pharms == FALSE){
    
    data_total <- data %>% 
      group_by(month_of_closure) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Total")
    
    data <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      mutate(Organisation.Name.Cat = Region_Name) %>%
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      count() 
    
    #maxYscale <- max(data_total$n) + 40
    scale_ratio<- max(data_total$n) / max(data$n)-1
    
    title <- "Total number of unplanned pharmacy closures"
    
    legendTitle <- "Region"
    
    subtitle <- "England"
    
    a <- " (National)"
    b <- " (Regional)"
    
  }else if(level != "National" & per_100_pharms == FALSE){
    
    data_total <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Region_Name) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Total")
    
    data <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      dplyr::filter(!is.na(STP.Name)) %>%
      group_by(month_of_closure, STP.Name) %>%
      count() %>%
      rename(Organisation.Name.Cat = STP.Name)
    
    #maxYscale <- 800
    scale_ratio<- max(data_total$n) / max(data$n)
    
    title <- "Number of unplanned pharmacy closures"
    
    legendTitle <- "ICB"
    
    subtitle <- region_STP_name
    
    a <- " (Regional)"
    b <- " (ICB level)"
    
  }
  
  if(plotChart == TRUE){
    ggplot() +
      geom_line(data = data,
                mapping = aes(x = month_of_closure,
                              y = n*scale_ratio,
                              colour = Organisation.Name.Cat),
                size = 1) +
      geom_point(data = data,
                 mapping = aes(x = month_of_closure,
                               y = n*scale_ratio,
                               colour = Organisation.Name.Cat),
                 size = 1) +
      geom_line(data = data_total,
                mapping = aes(x = month_of_closure,
                              y = n,
                              size = Organisation.Name.Cat)) +
      geom_point(data = data_total,
                 mapping = aes(x = month_of_closure,
                               y = n),
                 size = 2) +
      # ggrepel::geom_text_repel(data = data,
      #                          mapping = aes(x = month_of_closure,
      #                                        y = n,
      #                                        label = round(n),
      #                                        colour = Organisation.Name.Cat),
      #                          size = 3.5,
      #                          box.padding = unit(0.2, "lines")) +
      ggrepel::geom_label_repel(data = data_total,
                                mapping = aes(x = month_of_closure,
                                              y = n,
                                              label = round(n)),
                                size = 3.5,
                                label.size = NA,
                                box.padding = unit(0.3, "lines")) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
      #scale_y_continuous(sec.axis = sec_axis(~./scale_ratio, name = paste0("Number of closures",b))) +
      scale_size_manual(values = 1.5) +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
      labs(title = title,
           subtitle = subtitle,
           x = "Month of closure",
           y = paste0("Number of closures",a),
           colour = legendTitle,
           size = "") +
      theme_bw()
  }else{
    data
  }
  
  
}

################################################################################
plot_closures_by_month_per100pharm <- function(data = unplannedClosuresData,
                                   # pharm_list = Ref_PharmList1,
                                   #pharm_list = pharmacy_list_september2022,
                                   pharm_list = full_pharmlist,
                                   contractor_list = Ref_Contractor,
                                   level = "National",
                                   region_STP_name = NULL,
                                   per_100_pharms = FALSE, 
                                   plotChart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(pharm_list$SnapshotMonth)) %>%
    select(`ODS CODE` = ODS.CODE,
           `Organisation Name` = Organisation.Name,
           `STP Name` = ICB.Name,
           STP.Code)
  
  
  if(level == "National" & per_100_pharms == TRUE){
    
    names(pharm_list) <- names(pharm_list) %>% make.names()
    
    #get closures per 100 pharmacies 
    
    #join to get region column in pharm list
    ods_region_lookup <- contractor_list %>%
      select(ODS.CODE, Region_Name) %>%
      distinct()
    
    names(pharm_list) <- names(pharm_list) %>% make.names()
    
    #get number of pharmacies in each region
    pharms_per_region <- pharm_list %>%
      left_join(ods_region_lookup, by = "ODS.CODE") %>%
      group_by(Region_Name) %>%
      count() %>%
      rename(pharms_per_region = n)
    
    #get number of pharmacies in England
    pharms_in_england <- nrow(pharm_list)
    
    
    #get closures in the region per 100 pharmacies
    data_total <- data %>% 
      group_by(month_of_closure) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Across whole country") %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_in_england) %>%
      mutate(n = closures_per_100_pharms) 
    
    #get closures in each org per 100 pharmacies
    data <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      group_by(month_of_closure, Region_Name) %>%
      count() %>%
      left_join(pharms_per_region, by = "Region_Name") %>%
      rename(Organisation.Name.Cat = Region_Name) %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_per_region) %>%
      mutate(n = closures_per_100_pharms) 
    
    #maxYscale <- 230
    
    title <- "Number of unplanned pharmacy closures per 100 pharmacies"
    
    legendTitle <- "Region"
    
    subtitle <- "England"
    
  }else if(level != "National" & per_100_pharms == TRUE){
    
    #get closures per 100 pharmacies 
    
    #join to get region column in pharm list
    ods_region_lookup <- contractor_list %>%
      select(ODS.CODE, Region_Name) %>%
      distinct()
    
    names(pharm_list) <- names(pharm_list) %>% make.names()
    
    #get number of pharmacies in each STP
    pharms_per_stp <- pharm_list %>%
      group_by(STP.Code) %>%
      count() %>%
      rename(pharms_per_STP = n)
    
    #get number of pharmacies in each region
    pharms_per_region <- pharm_list %>%
      left_join(ods_region_lookup, by = "ODS.CODE") %>%
      group_by(Region_Name) %>%
      count() %>%
      rename(pharms_per_region = n)
    
    #get closures in the region per 100 pharmacies
    data_total <- data %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Region_Name) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Across whole region") %>%
      left_join(pharms_per_region, by = "Region_Name") %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_per_region) %>%
      mutate(n = closures_per_100_pharms)
    
    #get closures in each STP per 100 pharmacies
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      dplyr::filter(!is.na(STP.Name)) %>%
      rename(STP.Code = STP) %>%
      group_by(month_of_closure, STP.Name, STP.Code) %>%
      count() %>%
      left_join(pharms_per_stp, by = "STP.Code") %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_per_STP) %>%
      mutate(n = closures_per_100_pharms) %>%
      rename(Organisation.Name.Cat = STP.Name)
    
    #maxYscale <- 230
    
    title <- "Number of unplanned pharmacy closures per 100 pharmacies"
    
    legendTitle <- "ICB"
    
    subtitle <- region_STP_name
    
  }
  
  if(plotChart == TRUE){
    ggplot() +
      geom_line(data = data,
                mapping = aes(x = month_of_closure,
                              y = n,
                              colour = Organisation.Name.Cat),
                size = 1) +
      geom_point(data = data,
                 mapping = aes(x = month_of_closure,
                               y = n,
                               colour = Organisation.Name.Cat),
                 size = 1) +
      geom_line(data = data_total,
                mapping = aes(x = month_of_closure,
                              y = n,
                              size = Organisation.Name.Cat)) +
      geom_point(data = data_total,
                 mapping = aes(x = month_of_closure,
                               y = n),
                 size = 2) +
      # ggrepel::geom_text_repel(data = data,
      #                          mapping = aes(x = month_of_closure,
      #                                        y = n,
      #                                        label = round(n),
      #                                        colour = Organisation.Name.Cat),
      #                          size = 3.5,
      #                          box.padding = unit(0.2, "lines")) +
      ggrepel::geom_label_repel(data = data_total,
                                mapping = aes(x = month_of_closure,
                                              y = n,
                                              label = round(n)),
                                size = 3.5,
                                label.size = NA,
                                box.padding = unit(0.3, "lines")) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      scale_y_continuous() +
      scale_size_manual(values = 1.5) +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
      labs(title = title,
           subtitle = subtitle,
           x = "Month of closure",
           y = "Number of closures",
           colour = legendTitle,
           size = "") +
      theme_bw()
  }else{
    data
  }
  
  
}
################################################################################
plot_closures_by_month_by_org <- function(data = unplannedClosuresData,
                                                      pharm_list = Ref_PharmList1,
                                                      contractor_list = Ref_Contractor,
                                                      level = "National",
                                                      region_STP_name = NULL,
                                                      per_100_pharms = FALSE, 
                                                      plotChart = TRUE){
  
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  if(level == "National" & per_100_pharms == FALSE){
    
    # data <- data %>% 
    #   dplyr::filter(ParentOrgName %in% c("TESCO STORES LIMITED", "BOOTS UK LIMITED", "LLOYDS PHARMACY LTD", "L ROWLAND & CO (RETAIL) LTD", "ASDA STORES LTD")) %>%
    #   mutate(Organisation.Name.Cat = ParentOrgName)
    
    data_total <- data %>% 
      group_by(month_of_closure) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Total")
    
    data <- data %>%
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      count()
    
    # maxYscale <- max(data_total$n) + 40
    scale_ratio<- max(data_total$n) / max(data$n)-0.5
    
    title <- "Total number of unplanned pharmacy closures"
    
    legendTitle <- "Organisation"
    
    subtitle <- "England"
    
    a <- " (National)"
    b <- " (Organisational level)"
    
  }else if(level != "National" & per_100_pharms == FALSE){
    
    data_total <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Total")
    
    data <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      count() 
    
    #maxYscale <- 740
    scale_ratio<- max(data_total$n) / max(data$n)
    
    title <- "Number of unplanned pharmacy closures"
    
    legendTitle <- "Organisation"
    
    subtitle <- region_STP_name

    
    a <- " (ICB level)"
    b <- " (Organsiational level)"
    
  }
  
  if(plotChart == TRUE){
    ggplot() +
      geom_line(data = data,
                mapping = aes(x = month_of_closure,
                              y = n*scale_ratio,
                              colour = Organisation.Name.Cat),
                size = 1) +
      geom_point(data = data,
                 mapping = aes(x = month_of_closure,
                               y = n*scale_ratio,
                               colour = Organisation.Name.Cat),
                 size = 1) +
      geom_line(data = data_total,
                mapping = aes(x = month_of_closure,
                              y = n,
                              size = Organisation.Name.Cat)) +
      geom_point(data = data_total,
                 mapping = aes(x = month_of_closure,
                               y = n),
                 size = 2) +
      # ggrepel::geom_text_repel(data = data,
      #                          mapping = aes(x = month_of_closure,
      #                                        y = n,
      #                                        label = round(n),
      #                                        colour = Organisation.Name.Cat),
      #                          size = 3.5,
      #                          box.padding = unit(0.2, "lines")) +
      ggrepel::geom_label_repel(data = data_total,
                                mapping = aes(x = month_of_closure,
                                              y = n,
                                              label = round(n)),
                                size = 3.5,
                                label.size = NA,
                                box.padding = unit(0.3, "lines")) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
      #scale_y_continuous(sec.axis = sec_axis(~./scale_ratio, name = paste0("Number of closures",b))) +
      scale_size_manual(values = 1.5) +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
      labs(title = title,
           subtitle = subtitle,
           x = "Month of closure",
           y = paste0("Number of closures",a),
           colour = legendTitle,
           size = "") +
      theme_bw()
  }else{
    data_total
  }
  
  
}
################################################################################
plot_closures_by_month_by_org_per100pharm <- function(data = unplannedClosuresData,
                                          pharm_list = Ref_PharmList1,
                                          contractor_list = Ref_Contractor,
                                          level = "National",
                                          region_STP_name = NULL,
                                          per_100_pharms = FALSE, 
                                          plotChart = TRUE){
  
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  if(level == "National" & per_100_pharms == TRUE){
    
    names(pharm_list) <- names(pharm_list) %>% make.names()
    
    #get closures per 100 pharmacies 
    
    #get number of pharmacies per organisation
    pharms_per_org <- pharm_list %>%
      mutate(Organisation.Name.Cat = case_when(Organisation.Name=="LLOYDS PHARMACY LTD" ~"Lloyds",
                                               Organisation.Name=="BOOTS UK LIMITED" ~"Boots",
                                               Organisation.Name=="BESTWAY NATIONAL CHEMISTS LIMITED" ~"Bestway",
                                               Organisation.Name=="L ROWLAND & CO (RETAIL) LTD" ~"Rowland",
                                               Organisation.Name=="ASDA STORES LTD" ~"ASDA",
                                               Organisation.Name=="TESCO STORES LIMITED" ~"Tesco",
                                               TRUE ~ "All Other Pharmacies")) %>%
      group_by(Organisation.Name.Cat) %>%
      count() %>%
      rename(pharms_per_organisation = n)
    
    #get number of pharmacies in England
    pharms_in_england <- nrow(pharm_list)
    
    
    #get closures in the region per 100 pharmacies
    data_total <- data %>% 
      group_by(month_of_closure) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Across whole country") %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_in_england) %>%
      mutate(n = closures_per_100_pharms) 
    
    #get closures in each org per 100 pharmacies
    data <- data %>% 
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      count() %>%
      left_join(pharms_per_org, by = "Organisation.Name.Cat") %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_per_organisation) %>%
      mutate(n = closures_per_100_pharms) 
    
   # maxYscale <- 230
    
    title <- "Number of unplanned pharmacy closures per 100 pharmacies"
    
    legendTitle <- "Organisation"
    
    subtitle <- "England"
    
  }else if(level != "National" & per_100_pharms == TRUE){
    
    #get closures per 100 pharmacies 
    
    #join to get region column in pharm list
    ods_region_lookup <- contractor_list %>%
      select(ODS.CODE, Region_Name) %>%
      distinct()
    
    names(pharm_list) <- names(pharm_list) %>% make.names()
    
    #get number of pharmacies per organisation
    pharms_per_org <- pharm_list %>%
      left_join(ods_region_lookup, by = "ODS.CODE") %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      mutate(Organisation.Name.Cat = case_when(Organisation.Name=="LLOYDS PHARMACY LTD" ~"Lloyds",
                                               Organisation.Name=="BOOTS UK LIMITED" ~"Boots",
                                               Organisation.Name=="BESTWAY NATIONAL CHEMISTS LIMITED" ~"Bestway",
                                               Organisation.Name=="L ROWLAND & CO (RETAIL) LTD" ~"Rowland",
                                               Organisation.Name=="ASDA STORES LTD" ~"ASDA",
                                               Organisation.Name=="TESCO STORES LIMITED" ~"Tesco",
                                               TRUE ~ "All Other Pharmacies")) %>%
      group_by(Organisation.Name.Cat) %>%
      count() %>%
      rename(pharms_per_organisation = n)
    
    #get number of pharmacies in region
    pharms_per_region <- pharm_list %>%
      left_join(ods_region_lookup, by = "ODS.CODE") %>%
      dplyr::filter(Region_Name == region_STP_name) %>% 
      group_by(Region_Name) %>%
      count() %>%
      rename(pharms_per_region = n)
    
    #get closures in the region per 100 pharmacies
    data_total <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Region_Name) %>%
      count() %>%
      mutate(Organisation.Name.Cat = "Across whole region") %>%
      left_join(pharms_per_region, by = "Region_Name") %>%
      mutate(closures_per_100_pharms = n * 100 / pharms_per_region) %>%
      mutate(n = closures_per_100_pharms) 
    
    #get closures in each STP per 100 pharmacies
    data <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      count() %>%
      left_join(pharms_per_org, by = "Organisation.Name.Cat") %>% 
      mutate(closures_per_100_pharms = n * 100 / pharms_per_organisation) %>%
      mutate(n = closures_per_100_pharms) 
    
    #maxYscale <- max(230, max(data$n))
    
    title <- "Number of unplanned pharmacy closures per 100 pharmacies"
    
    legendTitle <- "Organisation"
    
    subtitle <- region_STP_name
    
  }
  
  if(plotChart == TRUE){
    ggplot() +
      geom_line(data = data,
                mapping = aes(x = month_of_closure,
                              y = n,
                              colour = Organisation.Name.Cat),
                size = 1) +
      geom_point(data = data,
                 mapping = aes(x = month_of_closure,
                               y = n,
                               colour = Organisation.Name.Cat),
                 size = 1) +
      geom_line(data = data_total,
                mapping = aes(x = month_of_closure,
                              y = n,
                              size = Organisation.Name.Cat)) +
      geom_point(data = data_total,
                 mapping = aes(x = month_of_closure,
                               y = n),
                 size = 2) +
      # ggrepel::geom_text_repel(data = data,
      #                          mapping = aes(x = month_of_closure,
      #                                        y = n,
      #                                        label = round(n),
      #                                        colour = Organisation.Name.Cat),
      #                          size = 3.5,
      #                          box.padding = unit(0.2, "lines")) +
      ggrepel::geom_label_repel(data = data_total,
                                mapping = aes(x = month_of_closure,
                                              y = n,
                                              label = round(n)),
                                size = 3.5,
                                label.size = NA,
                                box.padding = unit(0.3, "lines")) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      scale_y_continuous() +
      scale_size_manual(values = 1.5) +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
      labs(title = title,
           subtitle = subtitle,
           x = "Month of closure",
           y = "Number of closures",
           colour = legendTitle,
           size = "") +
      theme_bw() 
  }else{
    data_total
  }
  
  
}


################################################################################
plot_total_duration_of_closures <- function(data = unplannedClosuresData,
                                            level = "National",
                                            region_STP_name = NULL,
                                            plotChart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  #get rid of errors and convert duration format
  data <- data %>%
    dplyr::filter(DURATION.OF.CLOSURE < 1000) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) 
  
  data_regional <- data %>%
    group_by(ODS.CODE, month_of_closure, Region_Name, STP.Name, Organisation.Name.Cat) %>%
    summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE))
  
  if(level == "National"){
    
    data_total <- data %>%
      group_by(month_of_closure) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
      mutate(Organisation.Name.Cat = "Total")
    
    data <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      group_by(month_of_closure, Region_Name) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
      rename(Organisation.Name.Cat = Region_Name)
    
    #maxYscale <- max(data_total$closure_duration_hours) + 40
    scale_ratio<- max(data_total$closure_duration_hours) / max(data$closure_duration_hours)-1
    
    #title <- "Total number of unplanned pharmacy closures"
    
    legendTitle <- "Region"
    
    subtitle <- "England"
    
    a <- " (National)"
    b <- " (Regional)"
    
  }else{
    
    data_total <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Region_Name) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
      mutate(Organisation.Name.Cat = "Total")
    
    data <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      dplyr::filter(!is.na(STP.Name)) %>%
      group_by(month_of_closure, STP.Name) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
      rename(Organisation.Name.Cat = STP.Name)
    
    #maxYscale <- 3500
    scale_ratio<- max(data_total$closure_duration_hours) / max(data$closure_duration_hours)
    
    #title <- "Number of unplanned pharmacy closures"
    
    legendTitle <- "ICB"
    
    subtitle <- region_STP_name
    
    b <- " (ICB level)"
    a <- " (Regional)"
    
  }
  
  if(plotChart == TRUE){ggplot() +
      geom_line(data = data,
                mapping = aes(x = month_of_closure,
                              y = closure_duration_hours*scale_ratio,
                              colour = Organisation.Name.Cat),
                size = 1) +
      geom_point(data = data,
                 mapping = aes(x = month_of_closure,
                               y = closure_duration_hours*scale_ratio,
                               colour = Organisation.Name.Cat),
                 size = 1) +
      geom_line(data = data_total,
                mapping = aes(x = month_of_closure,
                              y = closure_duration_hours,
                              size = Organisation.Name.Cat)) +
      geom_point(data = data_total,
                 mapping = aes(x = month_of_closure,
                               y = closure_duration_hours),
                 size = 2) +
      # ggrepel::geom_text_repel(data = data,
      #                          mapping = aes(x = month_of_closure,
      #                                        y = closure_duration_hours,
      #                                        label = round(closure_duration_hours),
      #                                        colour = Organisation.Name.Cat),
      #                          size = 3.5,
      #                          box.padding = unit(0.2, "lines")) +
      ggrepel::geom_label_repel(data = data_total,
                                mapping = aes(x = month_of_closure,
                                              y = closure_duration_hours,
                                              label = round(closure_duration_hours)),
                                size = 3.5,
                                label.size = NA,
                                box.padding = unit(0.5, "lines")) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      #scale_y_continuous(sec.axis = sec_axis(~./scale_ratio, name = paste0("Hours",b))) +
      scale_size_manual(values = 1.5) +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
      labs(title = "Total Duration of Unplanned Pharmacy Closures (Hours)",
           subtitle = subtitle,
           x = "Month of closure",
           y = paste0("Hours",a),
           colour = legendTitle,
           size = "") +
      theme_bw()
  }else{
    data_regional <- data_regional %>%
      rename(`Month of closure` = month_of_closure,
             `Region Name` = Region_Name,
             `STP Name` = STP.Name,
             Organisation = Organisation.Name.Cat,
             `Closure Duration (hours)`  = closure_duration_hours)
  }
  
  
}

################################################################################
plot_total_duration_of_closures_by_org <- function(data = unplannedClosuresData,
                                                   level = "National",
                                                   region_STP_name = NULL,
                                                   plotChart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  data <- data %>%
    dplyr::filter(DURATION.OF.CLOSURE < 1000) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) 
  
  data_regional <- data %>%
    group_by(month_of_closure, Region_Name, STP.Name, Organisation.Name.Cat) %>%
    summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE))
  
  if(level == "National"){
    
    data_total <- data %>%
      group_by(month_of_closure) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
      mutate(Organisation.Name.Cat = "Total across country")
    
    data <- data %>% 
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE))
    
    #maxYscale <- max(data_total$closure_duration_hours) + 40
    scale_ratio<- max(data_total$closure_duration_hours) / max(data$closure_duration_hours)-1
    
    #title <- "Total number of unplanned pharmacy closures"
    
    legendTitle <- "Organisation"
    
    subtitle <- "England"
    
    a <- " (National)"
    b <- " (Organsiational level)"
    
    
  }else{
    
    data_total <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Region_Name) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
      mutate(Organisation.Name.Cat = "Total across region")
    
    data <- data %>% 
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(month_of_closure, Organisation.Name.Cat) %>%
      summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) 
    
    #maxYscale <- max(3000, max(data$closure_duration_hours))
    scale_ratio<- max(data_total$closure_duration_hours) / max(data$closure_duration_hours)
    
    #title <- "Number of unplanned pharmacy closures"
    
    legendTitle <- "Organisation"
    
    subtitle <- region_STP_name
    
    a <- " (ICB level)"
    b <- " (Organsiational level)"
    
  }
  
  if(plotChart == TRUE){ggplot() +
      geom_line(data = data,
                mapping = aes(x = month_of_closure,
                              y = closure_duration_hours*scale_ratio,
                              colour = Organisation.Name.Cat),
                size = 1) +
      geom_point(data = data,
                 mapping = aes(x = month_of_closure,
                               y = closure_duration_hours*scale_ratio,
                               colour = Organisation.Name.Cat),
                 size = 1) +
      geom_line(data = data_total,
                mapping = aes(x = month_of_closure,
                              y = closure_duration_hours,
                              size = Organisation.Name.Cat)) +
      geom_point(data = data_total,
                 mapping = aes(x = month_of_closure,
                               y = closure_duration_hours),
                 size = 2) +
      # ggrepel::geom_text_repel(data = data,
      #                          mapping = aes(x = month_of_closure,
      #                                        y = closure_duration_hours,
      #                                        label = round(closure_duration_hours),
      #                                        colour = Organisation.Name.Cat),
      #                          size = 3.5,
      #                          box.padding = unit(0.2, "lines")) +
      ggrepel::geom_label_repel(data = data_total,
                                mapping = aes(x = month_of_closure,
                                              y = closure_duration_hours,
                                              label = round(closure_duration_hours)),
                                size = 3.5,
                                label.size = NA,
                                box.padding = unit(0.5, "lines")) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      #scale_y_continuous(sec.axis = sec_axis(~./scale_ratio, name = paste0("Hours",b))) +
      scale_size_manual(values = 1.5) +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                                                                                                        "#F0E442", "#0072B2", "#D55E00", "#CC79A7")) +
      labs(title = "Total Duration of Unplanned Pharmacy Closures (Hours)",
           subtitle = subtitle,
           x = "Month of closure",
           y = paste0("Hours",a),
           colour = "Pharmacy type",
           size = "") +
      theme_bw()
  }else{
    data_regional <- data_regional %>%
      rename(`Month of closure` = month_of_closure,
             `Region Name` = Region_Name,
             `STP Name` = STP.Name,
             Organisation = Organisation.Name.Cat,
             `Closure Duration (hours)`  = closure_duration_hours)
  }
  
  
}


################################################################################
plot_perc_closures_under_4hrs <- function(data = unplannedClosuresData,
                                          level = "National",
                                          region_STP_name = NULL,
                                          plotChart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  data <- data %>%
    dplyr::filter(DURATION.OF.CLOSURE < 1000) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) %>%
    mutate(DURATION.OF.CLOSURE.CAT = case_when(closure_duration_hours < 4 ~ "<4 Hours",
                                               closure_duration_hours < 8 ~ "4-8 Hours",
                                               TRUE ~ ">8 Hours"
    ))
  
  if(level != "National"){
    
    #to get regional line
    data_overall <- data %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      dplyr::filter(!is.na(STP.Name))
    
    data_overall_totals <- data_overall %>% 
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month) %>%
      count() %>%
      rename(total_closures = n) 
    
    data_overall <- data_overall %>% 
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month, DURATION.OF.CLOSURE.CAT) %>%
      count() %>%
      left_join(data_overall_totals, by = c("month")) %>%
      mutate(perc_closures = n * 100 / total_closures) %>%
      dplyr::filter(DURATION.OF.CLOSURE.CAT == "<4 Hours") %>%
      mutate(STP.Name = "Across whole region")
    
    #to get STP lines
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      dplyr::filter(!is.na(STP.Name))
    
    data_totals <- data %>% 
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month, STP.Name) %>%
      count() %>%
      rename(total_closures = n)
    
    data <- data %>% 
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month, STP.Name, DURATION.OF.CLOSURE.CAT) %>%
      count() %>%
      left_join(data_totals, by = c("month", "STP.Name")) %>%
      mutate(perc_closures = n * 100 / total_closures) %>%
      dplyr::filter(DURATION.OF.CLOSURE.CAT == "<4 Hours")
    
    subtitle <- region_STP_name
    legendLabel <- "STP"
    
  }else{
    
    #to get National line
    data_overall_totals <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month) %>%
      count() %>%
      rename(total_closures = n) 
    
    data_overall <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month, DURATION.OF.CLOSURE.CAT) %>%
      count() %>%
      left_join(data_overall_totals, by = c("month")) %>%
      mutate(perc_closures = n * 100 / total_closures) %>%
      dplyr::filter(DURATION.OF.CLOSURE.CAT == "<4 Hours")  %>%
      mutate(STP.Name = "Across whole country")
    
    #to get regional lines
    data_totals <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month, Region_Name) %>%
      count() %>%
      rename(total_closures = n)
    
    data <- data %>% 
      dplyr::filter(!is.na(Region_Name)) %>%
      mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
      group_by(month, Region_Name, DURATION.OF.CLOSURE.CAT) %>%
      count() %>%
      left_join(data_totals, by = c("month", "Region_Name")) %>%
      mutate(perc_closures = n * 100 / total_closures) %>%
      dplyr::filter(DURATION.OF.CLOSURE.CAT == "<4 Hours") %>%
      rename(STP.Name = Region_Name)
    
    subtitle <- "England"
    legendLabel <- "Region"
    
    
  }
  
  if(plotChart == TRUE){
    ggplot() +
      geom_line(data,
                mapping = aes(x = month,
                              y = perc_closures,
                              colour = STP.Name),
                size = 1) +
      geom_line(data_overall,
                mapping = aes(x = month,
                              y = perc_closures,
                              size = STP.Name),
                colour = "black") +
      # geom_text(data = dplyr::filter(data, month == max(data$month)),
      #           mapping = aes(x = month,
      #                         y = perc_closures + 3,
      #                         label = paste0(round(perc_closures), "%"),
      #                         colour = STP.Name),
      #           size = 3) +
      geom_text(data = dplyr::filter(data_overall, month == max(data_overall$month)),
                mapping = aes(x = month,
                              y = perc_closures + 5,
                              label = paste0(round(perc_closures), "%")),
                colour = "black",
                size = 5) +
      scale_y_continuous(breaks = seq(0, 110, 10)) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
      labs(title = "Percentage of closures under 4 hours",
           subtitle = subtitle,
           y = "Percentage (%)",
           colour = legendLabel,
           size = "") +
      theme_bw()
  }else{
    data
  }
  
  
}

################################################################################
plot_perc_closures_under_4hrs_by_org <- function(data = unplannedClosuresData,
                                                 level = "National",
                                                 region_STP_name = NULL,
                                                 plotChart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  data <- data %>%
    dplyr::filter(DURATION.OF.CLOSURE < 1000) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) %>%
    mutate(DURATION.OF.CLOSURE.CAT = case_when(closure_duration_hours < 4 ~ "<4 Hours",
                                               closure_duration_hours < 8 ~ "4-8 Hours",
                                               TRUE ~ ">8 Hours"
    ))
  
  if(level != "National"){
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle <- region_STP_name
  }else{
    subtitle <- "England"
  }
  
  #to get totals line
  data_overall_totals <- data %>% 
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    group_by(month) %>%
    count() %>%
    rename(total_closures = n) 
  
  data_overall <- data %>% 
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    group_by(month, DURATION.OF.CLOSURE.CAT) %>%
    count() %>%
    left_join(data_overall_totals, by = c("month")) %>%
    mutate(perc_closures = n * 100 / total_closures) %>%
    dplyr::filter(DURATION.OF.CLOSURE.CAT == "<4 Hours") %>%
    mutate(Organisation.Name.Cat = "Total")
  
  #get org lines
  data_totals <- data %>% 
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    group_by(month, Organisation.Name.Cat) %>%
    count() %>%
    rename(total_closures = n) 
  
  data <- data %>% 
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    group_by(month, Organisation.Name.Cat, DURATION.OF.CLOSURE.CAT) %>%
    count() %>%
    left_join(data_totals) %>%
    mutate(perc_closures = n * 100 / total_closures) %>%
    dplyr::filter(DURATION.OF.CLOSURE.CAT == "<4 Hours")
  
  
  ggplot() +
    geom_line(data,
              mapping = aes(x = month,
                            y = perc_closures,
                            colour = Organisation.Name.Cat),
              size = 1) +
    # geom_text(data = dplyr::filter(data, month == max(data$month)),
    #           mapping = aes(x = month,
    #               y = perc_closures + 3,
    #               label = paste0(round(perc_closures), "%"),
    #               colour = Organisation.Name.Cat),
    #           size = 3) +
    geom_line(data_overall,
              mapping = aes(x = month,
                            y = perc_closures,
                            size = Organisation.Name.Cat),
              colour = "black") +
    geom_text(data = dplyr::filter(data_overall, month == max(data_overall$month)),
              mapping = aes(x = month,
                            y = perc_closures + 5,
                            label = paste0(round(perc_closures), "%")),
              colour = "black",
              size = 5) +
    scale_y_continuous() +
    scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
    scale_colour_manual(labels = function(x) str_wrap(x, width = 30), values = colorBlindGrey8) +
    labs(title = "Percentage of closures under 4 hours by Organisation",
         subtitle = subtitle,
         y = "Percentage (%)",
         colour = "Organisation",
         size = "") +
    theme_bw() 
  
}


################################################################################
plot_closures_by_ICS <- function(data = unplannedClosuresData,
                                 Month = as.Date("2022-01-01"),
                                 per_pharm = FALSE,
                                 pharm_list = Ref_PharmList1){
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    dplyr::filter(month == Month) %>%
    group_by(STP.Name) %>%
    count() %>%
    arrange(desc(n)) %>%
    mutate(STP.Name_abbrev = substr(STP.Name,1,30)) %>%
    mutate(y = n)
  
  title <- "Closures by ICS"
  ytitle <- "Closures"
  
  if(per_pharm == TRUE){
    
    names(pharm_list) <- names(pharm_list) %>% make.names()
    
    pharms_per_stp <- pharm_list %>%
      group_by(STP.Name) %>%
      count() %>%
      rename(pharms_per_STP = n)
    
    data <- data %>%
      left_join(pharms_per_stp) %>%
      mutate(closures_per_1000_pharms = n * 100 / pharms_per_STP) %>%
      mutate(y = closures_per_1000_pharms)
    
    title <- "Closures per 100 pharmacies by ICS"
    ytitle <- "Closures per 100 pharmacies"
  }
  
  ggplot(data) +
    geom_bar(aes(x = reorder(STP.Name_abbrev, -y),
                 y = y),
             stat = "identity",
             fill = "steelblue"
    ) +
    geom_text(aes(x = reorder(STP.Name_abbrev, -y),
                  y = y + 3,
                  label = round(y)),
              size = 2.5) +
    scale_y_continuous(breaks = seq(0, max(data$y, na.rm = T), 20)) +
    labs(title = title,
         y = ytitle,
         x = "",
         colour = "",
         subtitle = format(Month, "%B-%Y")) +
    theme_bw() + theme(axis.text.x = element_text(angle = 90, vjust = 0.6, hjust=0.2))
  
}


################################################################################
plot_reason_for_closure <- function(data = unplannedClosuresData,
                                    Month = as.Date("2022-01-01"),
                                    level = "National",
                                    region_STP_name = NULL,
                                    plot_chart = TRUE){
  
  subtitle2 <-"England"
  
  if(level != "National"){
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle2 <- region_STP_name
  }
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    dplyr::filter(month == Month) %>%
    group_by(Organisation.Name.Cat, REASON.FOR.CLOSURE) %>%
    count() %>%
    rename(closures = n) %>%
    group_by(Organisation.Name.Cat) %>%
    mutate(percentage = round(closures*100/sum(closures), 1)) %>%
    arrange(desc(closures)) %>%
    mutate(REASON.FOR.CLOSURE = case_when(REASON.FOR.CLOSURE == "Other issue with the building (power cut, water supply issue, bomb scare etc)" ~ "Other issue with the building",
                                          REASON.FOR.CLOSURE == "Adverse weather (including snow, flood, flooded building)" ~ "Adverse weather",
                                          REASON.FOR.CLOSURE == "Other, please fill in column U" ~ "Other reason",
                                          REASON.FOR.CLOSURE %in% c("No cover found", "locum could not be found", "No Staff", 
                                                                    "locum cancelled at short notice", "Locum coulnd not be found", 
                                                                    "Locum cancelled at short notice", "Locum not booked on for shift") ~ "Locum could not be found",
                                          REASON.FOR.CLOSURE %in% c("Pharmacist called in sick", "Sickness", "pharmacy manager unwell", "Covid-19", "sickness") ~ "Short notice staff sickness",
                                          REASON.FOR.CLOSURE %in% c("Pharmacist stuck in traffic / Car trouble") ~ "Late arrival due to traffic",
                                          is.na(REASON.FOR.CLOSURE) ~ "Reason not given",
                                          TRUE ~ REASON.FOR.CLOSURE
    )) %>%
    mutate(REASON.FOR.CLOSURE = case_when(!(REASON.FOR.CLOSURE %in% c("Locum could not be found",
                                                                      "Other reason",
                                                                      "Short notice staff sickness",
                                                                      "Reason not given",
                                                                      "Late arrival due to traffic",
                                                                      "Other issue with the building")) ~ "Reason not given",
                                          TRUE ~ REASON.FOR.CLOSURE)
    )
  # mutate(REASON.FOR.CLOSURE = 
  #          ifelse(REASON.FOR.CLOSURE == "Other issue with the building (power cut, water supply issue, bomb scare etc)",
  #                 "Other issue with the building",
  #                 REASON.FOR.CLOSURE)) %>%
  # mutate(REASON.FOR.CLOSURE = 
  #          ifelse(REASON.FOR.CLOSURE == "Adverse weather (including snow, flood, flooded building)",
  #                 "Adverse weather",
  #                 REASON.FOR.CLOSURE)) %>%
  # mutate(REASON.FOR.CLOSURE = 
  #          ifelse(REASON.FOR.CLOSURE == "Other, please fill in column U",
  #                 "Other reason",
  #                 REASON.FOR.CLOSURE)) %>%
  # mutate(REASON.FOR.CLOSURE = 
  #          ifelse(is.na(REASON.FOR.CLOSURE),
  #                 "Reason not given",
  #                 REASON.FOR.CLOSURE))
  
  data$REASON.FOR.CLOSURE <- factor(data$REASON.FOR.CLOSURE,
                                    levels = c("Locum could not be found",
                                               "Other reason",
                                               "Short notice staff sickness",
                                               "Reason not given",
                                               "Late arrival due to traffic",
                                               "Other issue with the building"))
  
  data$Organisation.Name.Cat <- factor(data$Organisation.Name.Cat, levels = c("ASDA",
                                                                              "Bestway",
                                                                              "Boots",
                                                                              "Lloyds",
                                                                              "Rowland",
                                                                              "Tesco",
                                                                              "All Other Pharmacies"))
  
  if(plot_chart == TRUE){
    ggplot(data) +
      geom_bar(aes(x = Organisation.Name.Cat,
                   y = percentage,
                   fill = REASON.FOR.CLOSURE),
               stat = "identity",
               position = "dodge"
      ) +
      scale_y_continuous(breaks = seq(0, 100, 10)) +
      labs(title = "Reason for closure by organisation category",
           x = "Organisation",
           y = "Percentage of closures with given reason",
           fill = "",
           subtitle = paste0(subtitle2, "\n", format(Month, "%B-%Y"))) +
      theme_bw() #+ theme(axis.text.x = element_text(angle = 90, vjust = 0.6, hjust=0.2))
  }else{
    data
  }
  
  
}

################################################################################
plot_reason_for_closure_timeseries <- function(data = unplannedClosuresData,
                                               level = "National",
                                               region_STP_name = NULL,
                                               plot_chart = TRUE){
  
  colorBlindGrey8   <- c("#999999", "#E69F00", "#56B4E9", "#009E73", 
                         "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
                         "darkorchid2", "blue", "chartreuse")
  
  
  if(level != "National"){
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle2 <- region_STP_name
  }else{
    subtitle2 <-"England"
  }
  
  data <- data %>%
    mutate(REASON.FOR.CLOSURE = case_when(REASON.FOR.CLOSURE == "Other issue with the building (power cut, water supply issue, bomb scare etc)" ~ "Other issue with the building",
                                          REASON.FOR.CLOSURE == "Adverse weather (including snow, flood, flooded building)" ~ "Adverse weather",
                                          REASON.FOR.CLOSURE == "Other, please fill in column U" ~ "Other reason",
                                          REASON.FOR.CLOSURE %in% c("No cover found", "locum could not be found", "No Staff", 
                                                                    "locum cancelled at short notice", "Locum coulnd not be found", 
                                                                    "Locum cancelled at short notice", "Locum not booked on for shift") ~ "Locum could not be found",
                                          REASON.FOR.CLOSURE %in% c("Pharmacist called in sick", "Sickness", "pharmacy manager unwell", "Covid-19", "sickness") ~ "Short notice staff sickness",
                                          REASON.FOR.CLOSURE %in% c("Pharmacist stuck in traffic / Car trouble") ~ "Late arrival due to traffic",
                                          is.na(REASON.FOR.CLOSURE) ~ "Reason not given",
                                          TRUE ~ REASON.FOR.CLOSURE
    )) %>%
    mutate(REASON.FOR.CLOSURE = case_when(!(REASON.FOR.CLOSURE %in% c("Locum could not be found",
                                                                      "Other reason",
                                                                      "Short notice staff sickness",
                                                                      "Reason not given",
                                                                      "Late arrival due to traffic",
                                                                      "Other issue with the building")) ~ "Reason not given",
                                          TRUE ~ REASON.FOR.CLOSURE)
    ) %>%
    group_by(month_of_closure, REASON.FOR.CLOSURE) %>%
    count()  %>%
    rename(closures = n) %>%
    #rowwise() %>%
    group_by(month_of_closure) %>%
    mutate(percentage = round(closures*100/sum(closures), 1))
  
  
  if(plot_chart == TRUE){
    ggplot(data) +
      geom_line(aes(x = month_of_closure,
                    y = percentage,
                    colour = REASON.FOR.CLOSURE),
                size = 1
      ) + 
      geom_point(aes(x = month_of_closure,
                     y = percentage,
                     colour = REASON.FOR.CLOSURE),
                 size = 1
      ) +
      scale_y_continuous(breaks = seq(0, 100, 10)) +
      scale_x_date(date_breaks = "2 month", date_labels = "%b-%y") +
      scale_colour_manual(#labels = function(x) str_wrap(REASON.FOR.CLOSURE, width = 30), 
        values = colorBlindGrey8) +
      labs(title = "Percentage of unplanned closures with each reason for closure",
           x = "Month",
           y = "Percentage of closures with given reason (%)",
           colour = "",
           subtitle = subtitle2
      ) +
      theme_bw() + 
      theme(axis.text.x = element_text(angle = 90, vjust = 0.6, hjust=0.2))
  }else{
    data
  }
  
  
}


################################################################################
plot_closures_by_category <- function(data = unplannedClosuresData,
                                      Month = as.Date("2022-01-01"),
                                      per_pharm = FALSE,
                                      pharm_list = Ref_PharmList1,
                                      category = SettlementCategory,
                                      title = "Closures by settlement category"){
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    dplyr::filter(month == Month) %>%
    group_by({{ category }}) %>%
    count() %>%
    arrange(desc(n)) %>%
    mutate(y = n) %>%
    rename(category = {{ category }}) %>%
    mutate(category = stringr::str_wrap(category, 15))
  
  
  ggplot(data) +
    geom_bar(aes(x = reorder(category, -y),
                 y = y),
             stat = "identity",
             fill = "steelblue"
    ) +
    geom_text(aes(x = reorder(category, -y),
                  y = y + 5,
                  label = round(y)),
              size = 3.5) +
    scale_y_continuous(breaks = seq(0, max(data$y, na.rm = T), 50)) +
    labs(title = title,
         x = "",
         y = "Count",
         colour = "",
         subtitle = format(Month, "%B-%Y")) +
    theme_bw() #+ theme(axis.text.x = element_text(angle = 45, vjust = 0.6, hjust=0.2))
  
}


################################################################################
plot_closures_per_pharm <- function(data = unplannedClosuresData,
                                    Month = as.Date("2022-01-01"),
                                    per_pharm = FALSE,
                                    pharm_list = pharmlist,
                                    category = SettlementCategory,
                                    title = "Closures by settlement category",
                                    level = "National",
                                    region_STP_name = NULL,
                                    plotChart = TRUE){
  
  
  if(level == "National"){
    
    num_pharms_per_chain <- pharm_list %>%
      group_by(`Organisation Name`) %>%
      count() %>%
      rename(Number.of.Pharmacies = n,
             ParentOrgName = `Organisation Name`)
    
    subtitle2 <-"England"
    
  }else{
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    num_pharms_per_chain <- pharm_list %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(`Organisation Name`, Region_Name) %>%
      count() %>%
      rename(Number.of.Pharmacies = n,
             ParentOrgName = `Organisation Name`)
    
    subtitle2 <- region_STP_name
  }
  
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    dplyr::filter(month == Month) %>%
    group_by(ParentOrgName) %>% 
    count() %>%
    rename(num_closures = n) %>%
    left_join(num_pharms_per_chain) %>%
    mutate(closures_per_pharm = num_closures / Number.of.Pharmacies) %>%
    arrange(desc(Number.of.Pharmacies)) %>%
    head(10) %>%
    mutate(ParentOrgName = stringr::str_wrap(ParentOrgName, 15))
  
  
  if(plotChart == TRUE){
    ggplot(data) +
      geom_bar(aes(x = reorder(ParentOrgName, -closures_per_pharm),
                   y = closures_per_pharm),
               stat = "identity",
               fill = "steelblue"
      ) +
      geom_text(aes(x = reorder(ParentOrgName, -closures_per_pharm),
                    y = closures_per_pharm + 0.05,
                    label = round(closures_per_pharm,2)),
                size = 3.5) +
      #scale_y_continuous(breaks = seq(0, max(data$closures_per_pharm, na.rm = T), 0.5)) +
      labs(title = "Closures per pharmacy",
           x = "",
           y = "Closure rate",
           colour = "",
           subtitle = paste0(subtitle2, "\n", format(Month, "%B-%Y"))) +
      theme_bw() + theme(axis.text.x = element_text(angle = 90, vjust = 0.6, hjust=0.2))
  }else{
    data
  }
  
  
}

################################################################################
plot_pharms_with_closure_per_chain <- function(data = unplannedClosuresData,
                                               Month = as.Date("2022-01-01"),
                                               per_pharm = FALSE,
                                               pharm_list = pharmlist,
                                               level = "National",
                                               region_STP_name = NULL,
                                               plot_chart = TRUE){
  
  if(level == "National"){
    
    num_pharms_per_chain <- pharm_list %>%
      group_by(`Organisation Name`) %>%
      count() %>%
      rename(Number.of.Pharmacies = n,
             ParentOrgName = `Organisation Name`)
    
    subtitle2 <-"England"
    
  }else{
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    num_pharms_per_chain <- pharm_list %>%
      dplyr::filter(Region_Name == region_STP_name) %>%
      group_by(`Organisation Name`, Region_Name) %>%
      count() %>%
      rename(Number.of.Pharmacies = n,
             ParentOrgName = `Organisation Name`)
    
    subtitle2 <- region_STP_name
  }
  
  # if(level != "National"){
  #   
  #   data <- data %>%
  #     dplyr::filter(Region_Name == region_STP_name)
  #   
  #   subtitle2 <- region_STP_name
  # }
  # 
  # num_pharms_per_chain <- data %>%
  #   mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
  #   dplyr::filter(month == Month) %>%
  #   select(ParentOrgName, Number.of.Pharmacies) %>%
  #   distinct() 
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    dplyr::filter(month == Month) %>% #####################
  group_by(ParentOrgName) %>% ######################
  summarise(num_pharms_with_closures = n_distinct(ODS.CODE)) %>%
    left_join(num_pharms_per_chain) %>%
    mutate(perc_pharms_with_closure = num_pharms_with_closures * 100 / Number.of.Pharmacies) %>%
    arrange(desc(Number.of.Pharmacies)) %>% ########################
  head(10) %>% ############################
  mutate(ParentOrgName = stringr::str_wrap(ParentOrgName, 15)) ##################
  
  
  if(plot_chart == TRUE){
    ggplot(data) +
      geom_bar(aes(x = reorder(ParentOrgName, -perc_pharms_with_closure),
                   y = perc_pharms_with_closure),
               stat = "identity",
               fill = "steelblue"
      ) +
      geom_text(aes(x = reorder(ParentOrgName, -perc_pharms_with_closure),
                    y = perc_pharms_with_closure + 1,
                    label = paste0(round(perc_pharms_with_closure), "%")),
                size = 3.5) +
      scale_y_continuous(breaks = seq(0, max(data$perc_pharms_with_closure, na.rm = T), 10)) +
      labs(title = "Percentage of pharmacies in chain with at least one closure",
           x = "",
           y = "Percentage (%)",
           colour = "",
           subtitle = paste0(subtitle2, "\n", format(Month, "%B-%Y"))) +
      theme_bw() + theme(axis.text.x = element_text(angle = 90, vjust = 0.6, hjust=0.2))
  }else{
    data
  }
  
  
}

################################################################################

plot_closure_duration_vs_nearest_pharm_dist <- function(data = unplannedClosuresData,
                                                        nearest_pharm_data = nearest_pharm,
                                                        Month = as.Date("2022-06-01"),
                                                        level = "National",
                                                        region_STP_name = NULL,
                                                        external_view = FALSE,
                                                        plotChart = TRUE){
  
  subtitle2 <-"England"
  
  if(level != "National"){
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle2 <- region_STP_name
  }
  
  data <- data %>%
    dplyr::filter(month_of_closure == Month) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) %>%
    dplyr::filter(closure_duration_hours > 0) %>%
    group_by(ODS.CODE, Region_Name) %>%
    summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = T)) %>%
    left_join(nearest_pharm_data) %>%
    arrange(desc(metres_to_nearest_pharmacy))
  
  top_durations <- data %>%
    arrange(desc(closure_duration_hours)) 
  top_durations <- head(top_durations, 5)
  
  top_distances <- data %>%
    arrange(desc(metres_to_nearest_pharmacy)) 
  top_distances <- head(top_distances, 5)
  
  data_to_highlight <- data %>%
    dplyr::filter(ODS.CODE %in% top_durations$ODS.CODE | ODS.CODE %in% top_distances$ODS.CODE)
  
  if(plotChart){
    
    if(level == "National"){
      p <- ggplot() +
        geom_point(data,
                   mapping = aes(x = metres_to_nearest_pharmacy,
                                 y = closure_duration_hours,
                                 colour = Region_Name)
        )
    }else{
      p <- ggplot() +
        geom_point(data,
                   mapping = aes(x = metres_to_nearest_pharmacy,
                                 y = closure_duration_hours,
                                 colour = Region_Name),
                   colour = "steelblue"
        )
    }
    
    if(external_view == FALSE){
      p <-  p +
        ggrepel::geom_text_repel(data_to_highlight,
                                 mapping = aes(x = metres_to_nearest_pharmacy,
                                               y = closure_duration_hours,
                                               label = ODS.CODE),
                                 size = 3,
                                 box.padding = unit(0.2, "lines"))
    }
    
    p <- p  +
      scale_x_continuous(breaks = scales::breaks_pretty()) +
      scale_y_continuous(breaks = scales::breaks_pretty()) +
      labs(title = "Next nearest pharmacy distance Vs Total duration of closures",
           x = "Distance to next nearest pharmacy (m)",
           y = "Total duration of closures (hrs)",
           colour = "",
           subtitle = paste0(subtitle2, "\n", format(Month, "%B-%Y"))) +
      theme_bw()
    p
    
  }else{
    data
  }
  
}

################################################################################

plot_closure_duration_vs_eps_nom <- function(data = unplannedClosuresData,
                                             eps_data = eps_nominations,
                                             Month = as.Date("2022-06-01"),
                                             level = "National",
                                             region_STP_name = NULL,
                                             external_view = FALSE,
                                             plotChart = TRUE){
  
  subtitle2 <-"England"
  
  if(level != "National"){
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle2 <- region_STP_name
  }
  
  data <- data %>%
    dplyr::filter(month_of_closure == Month) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) %>%
    dplyr::filter(closure_duration_hours > 0) %>%
    group_by(ODS.CODE, Region_Name) %>%
    summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = T)) %>%
    left_join(eps_data) %>%
    arrange(desc(eps_nominations))
  
  top_durations <- data %>%
    arrange(desc(closure_duration_hours)) 
  top_durations <- head(top_durations, 5)
  
  top_nominations <- data %>%
    arrange(desc(eps_nominations)) 
  top_nominations <- head(top_nominations, 5)
  
  data_to_highlight <- data %>%
    dplyr::filter(ODS.CODE %in% top_durations$ODS.CODE | ODS.CODE %in% top_nominations$ODS.CODE)
  
  if(plotChart){
    
    if(level == "National"){
      p <- ggplot() +
        geom_point(data,
                   mapping = aes(x = eps_nominations,
                                 y = closure_duration_hours,
                                 colour = Region_Name)
        )
    }else{
      p <- ggplot() +
        geom_point(data,
                   mapping = aes(x = eps_nominations,
                                 y = closure_duration_hours,
                                 colour = Region_Name),
                   colour = "steelblue"
        )
    }
    
    if(external_view == FALSE){
      
      p <- p +
        ggrepel::geom_text_repel(data_to_highlight,
                                 mapping = aes(x = eps_nominations,
                                               y = closure_duration_hours,
                                               label = ODS.CODE),
                                 size = 3,
                                 box.padding = unit(0.2, "lines"))
    }
    
    p <- p +
      scale_x_continuous(breaks = scales::breaks_pretty()) +
      scale_y_continuous(breaks = scales::breaks_pretty()) +
      labs(title = "Number of pharmacy EPS nominations Vs Total duration of closures",
           x = "Number of EPS nominations",
           y = "Total duration of closures (hrs)",
           colour = "",
           subtitle = paste0(subtitle2, "\n", format(Month, "%B-%Y"))) +
      theme_bw()
    
    p
    
  }else{
    data
  }
  
}







############################# checking survery data ############################
################################################################################
plot_pharms_with_closures <- function(data = unplannedClosuresData,
                                      ref_data = Ref_PharmList1,
                                      Month = as.Date("2022-01-01"),
                                      per_pharm = FALSE,
                                      pharm_list = Ref_PharmList1,
                                      plot_chart = TRUE){
  
  num_pharms <- ref_data %>%
    select(`ODS CODE`) %>%
    distinct(`ODS CODE`) 
  
  num_pharms <- nrow(num_pharms)
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    dplyr::filter(month == Month) %>%
    summarise(num_pharms_with_closures = n_distinct(ODS.CODE))# %>%
  
  
  
  if(plot_chart == TRUE){
    ggplot(data) +
      geom_bar(aes(x = reorder(ParentOrgName, -perc_pharms_with_closure),
                   y = perc_pharms_with_closure),
               stat = "identity",
               fill = "steelblue"
      ) +
      geom_text(aes(x = reorder(ParentOrgName, -perc_pharms_with_closure),
                    y = perc_pharms_with_closure + 1,
                    label = paste0(round(perc_pharms_with_closure), "%")),
                size = 3.5) +
      scale_y_continuous(breaks = seq(0, max(data$perc_pharms_with_closure, na.rm = T), 10)) +
      labs(title = "Percentage of pharmacies in chain with at least one closure",
           x = "",
           y = "Percentage (%)",
           colour = "",
           subtitle = format(Month, "%B-%Y")) +
      theme_bw() + theme(axis.text.x = element_text(angle = 90, vjust = 0.6, hjust=0.2))
  }else{
    data
  }
  
  
}


################################################################################
get_total_closure_hours <- function(data = unplannedClosuresData){
  
  data_unique_pharms_with_closures <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    group_by(month) %>%
    summarise(num_pharms_with_closures = n_distinct(ODS.CODE))
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    select(month, ODS.CODE, DURATION.OF.CLOSURE) %>%
    mutate(closure_hours = as.numeric(substr(DURATION.OF.CLOSURE, 1, 2))) %>%
    mutate(closure_minutes = as.numeric(substr(DURATION.OF.CLOSURE, 4, 5))) %>%
    mutate(closure_duration_minutes = closure_hours * 60 + closure_minutes) %>%
    mutate(closure_duration_hours = closure_duration_minutes / 60) %>%
    select(-closure_hours, -closure_minutes) %>%
    group_by(month) %>%
    summarise(total_closure_duration_minutes = sum(closure_duration_minutes, na.rm = TRUE),
              total_closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
    left_join(data_unique_pharms_with_closures) %>%
    mutate(mean_closure_hours_per_pharm = total_closure_duration_hours / num_pharms_with_closures)
  
}


################################################################################
get_closures_dispensing_data <- function(data = unplannedClosuresData,
                                         items_data = items_dispensed,
                                         nearest_pharm_dist = nearest_pharm){
  
  data_num_closures <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    group_by(month, ODS.CODE) %>%
    count() %>%
    rename(number_of_closures = n)
  
  data <- data %>%
    mutate(month = lubridate::floor_date(DATE.OF.CLOSURE, unit = "month")) %>%
    select(month, ODS.CODE, DURATION.OF.CLOSURE) %>%
    mutate(closure_hours = as.numeric(substr(DURATION.OF.CLOSURE, 1, 2))) %>%
    mutate(closure_minutes = as.numeric(substr(DURATION.OF.CLOSURE, 4, 5))) %>%
    mutate(closure_duration_minutes = closure_hours * 60 + closure_minutes) %>%
    mutate(closure_duration_hours = closure_duration_minutes / 60) %>%
    select(-closure_hours, -closure_minutes) %>%
    group_by(month, ODS.CODE) %>%
    summarise(total_closure_duration_minutes = sum(closure_duration_minutes, na.rm = TRUE),
              total_closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) %>%
    left_join(data_num_closures) %>%
    dplyr::filter(month >= as.Date("2021-10-01"))
  
  items_data <- items_data %>%
    mutate(Month = as.Date(Month)) %>%
    select(month = Month,
           ODS.CODE = `Contractor Code`,
           items_dispensed = Figure)
  
  data <- data %>%
    left_join(items_data, by = c("month", "ODS.CODE")) %>%
    left_join(nearest_pharm_dist)
  
}


#################################################################################
get_closures_by_STP_table <- function(data = unplannedClosuresData,
                                      externalUse = FALSE){
  
  data <- data %>%
    dplyr::filter(month_of_closure < (Sys.Date() - lubridate::weeks(4)))
  
  if(externalUse == FALSE){
    data <- data %>%
      group_by(Region_Name, STP.Name, Organisation.Name.Cat, month_of_closure)
  }else{
    data <- data %>%
      group_by(Region_Name, STP.Name, month_of_closure)
  }
  
  data <- data %>%
    count() 
    # mutate(month_of_closure = format(month_of_closure, "%b%y")) %>%
    # pivot_wider(names_from = month_of_closure,
    #             values_from = n) %>%
    # dplyr::filter(!is.na(STP.Name)) 
  
  if(externalUse == FALSE){
    data <- data %>%
      select(`Month of Closure` = month_of_closure, 
             `Region Name` = Region_Name,
             `ICB Name` = STP.Name,
             `Organisation` = Organisation.Name.Cat,
             `Number of closures` = n)
  }else{
    data <- data %>%
      rename(`Month of Closure` = month_of_closure, 
             `Region Name` = Region_Name,
             `ICB Name` = STP.Name,
             `Number of closures` = n)
  }
  
}

#################################################################################
get_closures_per_100_pharms_by_STP_table <- function(data = unplannedClosuresData,
                                                     pharm_list = full_pharmlist,#Ref_PharmList1,
                                                     contractor_list = Ref_Contractor){
  
  pharm_list <- pharm_list %>%
    dplyr::filter(SnapshotMonth == max(pharm_list$SnapshotMonth))
  
  #get closures per 100 pharmacies 
  
  #join to get region column in pharm list
  ods_region_lookup <- contractor_list %>%
    select(ODS.CODE, Region_Name) %>%
    distinct()
  
  names(pharm_list) <- names(pharm_list) %>% make.names()
  
  #get number of pharmacies in each STP
  pharms_per_stp <- pharm_list %>%
    rename(STP = STP.Code) %>%
    group_by(STP) %>%
    count() %>%
    rename(pharms_per_STP = n)
  
  # #get number of pharmacies in each region
  # pharms_per_region <- pharm_list %>%
  #   left_join(ods_region_lookup, by = "ODS.CODE") %>%
  #   group_by(Region_Name) %>%
  #   count() %>%
  #   rename(pharms_per_region = n)
  
  # #get closures in the region per 100 pharmacies
  # data_total <- data %>% 
  #   group_by(month_of_closure, Region_Name) %>%
  #   count() %>%
  #   mutate(STP.Name = "Across whole region") %>%
  #   left_join(pharms_per_region, by = "Region_Name") %>%
  #   mutate(closures_per_100_pharms = n * 100 / pharms_per_region) %>%
  #   mutate(n = closures_per_100_pharms) 
  
  #get closures in each STP per 100 pharmacies
  data <- data %>%
    dplyr::filter(!is.na(STP.Name)) %>%
    group_by(month_of_closure, Region_Name, STP.Name, STP) %>%
    count() %>%
    left_join(pharms_per_stp, by = "STP") %>%
    mutate(closures_per_100_pharms = n * 100 / pharms_per_STP) %>%
    mutate(n = closures_per_100_pharms) 
  
  data <- data %>%
    group_by(Region_Name, STP.Name, month_of_closure) %>%
    summarise(closures_per_100_pharms = round(sum(closures_per_100_pharms, na.rm = TRUE),1)) %>%
    # mutate(month_of_closure = format(month_of_closure, "%b%y")) %>%
    # pivot_wider(names_from = month_of_closure,
    #             values_from = closures_per_100_pharms) %>%
    select(`Month of Closure` = month_of_closure,
           `Region Name` = Region_Name,
           `ICB Name` = STP.Name,
           `Closures per 100 pharmacies` = closures_per_100_pharms)
  
}


#################################################################################
plot_closure_start_time_density <- function(data = unplannedClosuresData,
                                            plotChart = TRUE){
  
  data <- data %>%
    mutate(closure_start_time = as.numeric(HOURS.FROM) * 24)
  
  if(plotChart == TRUE){
    ggplot(data) +
      geom_histogram(aes(x = closure_start_time),
                     binwidth = 0.5,
                     plsotion = "dodge",
                     fill = "steelblue") +
      theme_bw() +
      scale_x_continuous(breaks = seq(0, 23, 1)) +
      labs(title = "Unplanned closure start time frequency",
           x = "Hour of the day that closure started",
           y = "Number of closures starting at this time")
  }else{
    
    data <- data %>%
      mutate(closure_start_hour = plyr::round_any(closure_start_time, 0.5)) %>%
      group_by(closure_start_hour) %>%
      count() %>%
      rename(`Closure start hour` = closure_start_hour,
             `Number of closures starting at this time` = n)
  }
  
  
}


################################################################################
plot_items_SPC <- function(unplanned_closures_data = unplannedClosuresData,
                           items_data = items_dispensed,
                           contractor_table = Ref_Contractor,
                           with_closures = TRUE,
                           level = "National",
                           region_STP_name = NULL,
                           plotChart = TRUE){
  
  #join in region column 
  pharm_region_lookup <- contractor_table %>%
    select(`Contractor Code` = ODS.CODE, 
           Region_Name,
           ContractType) %>%
    distinct()
  
  items_data <- items_data %>% 
    left_join(pharm_region_lookup, by = "Contractor Code") %>%
    dplyr::filter(ContractType != "DAC" & ContractType != "DSP" & ContractType != "LPS")
  
  #filter down to required level
  subtitle2 <-"England"
  
  if(level != "National"){
    
    items_data <- items_data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle2 <- region_STP_name
  }
  
  #vector of pharmacies that have had at least one closure
  #pharms_with_closures <- unplanned_closures_data$ODS.CODE
  pharms_with_closures <- unplanned_closures_data %>%
    dplyr::filter(month_of_closure >= max(unplanned_closures_data$month_of_closure) - months(3))
  
  pharms_with_closures <- unique(pharms_with_closures$ODS.CODE)
  
  items_data <- items_data %>%
    dplyr::filter(`Contractor Type` == "Pharmacy") %>%
    select(month = Month,
           ODS.CODE = `Contractor Code`,
           items_dispensed_by_pharm = Figure)
  
  if(with_closures == TRUE){
    data <- items_data %>%
      dplyr::filter(ODS.CODE %in% pharms_with_closures) 
    
    title <- "Cohort 2: Pharmacies with closures in observation period"
    
  }else{
    data <- items_data %>%
      dplyr::filter(!(ODS.CODE %in% pharms_with_closures))
    
    titlesub <- "Cohort 1: Pharmacies with no closures in observation period"
    title <- paste0("Monthly Average Number of Items Dispensed per Pharmacy Per Day \n", titlesub)
  }
  
  #num_pharms <- length(unique(data$ODS.CODE))
  
  number_of_pharms <- data %>%
    group_by(month) %>%
    count() %>%
    rename(num_pharms = n)
  
  num_pharms <- number_of_pharms %>%
    dplyr::filter(month == max(number_of_pharms$month))
  num_pharms <- num_pharms$num_pharms[1]
  
  data <- data %>%
    group_by(month) %>%
    summarise(monthly_items = sum(items_dispensed_by_pharm, na.rm = TRUE)) %>%
    left_join(number_of_pharms, by = "month") %>%
    mutate(itemsPerPharm = monthly_items / num_pharms) %>%
    mutate(month_number = substr(month, 6,7)) %>%
    mutate(daysInMonth = case_when(month_number == "01" ~ 31,
                                   month_number == "02" ~ 28,
                                   month_number == "03" ~ 31,
                                   month_number == "04" ~ 30,
                                   month_number == "05" ~ 31,
                                   month_number == "06" ~ 30,
                                   month_number == "07" ~ 31,
                                   month_number == "08" ~ 31,
                                   month_number == "09" ~ 30,
                                   month_number == "10" ~ 31,
                                   month_number == "11" ~ 30,
                                   month_number == "12" ~ 31)) %>%
    mutate(averageItemsPerPharmPerDay = itemsPerPharm/daysInMonth) %>%
    select(-month_number, -daysInMonth) %>%
    mutate(month = as.Date(month))
  
  
  autospc::plot_auto_SPC(data,
                         x = month,
                         y = averageItemsPerPharmPerDay,
                         title = title,
                         subtitle = paste0(subtitle2, " (",num_pharms," pharmacies)"), 
                         override_x_title = "Date", 
                         override_y_title = "Items dispensed",
                         noRecals = T,
                         includeAnnotations = F
                         #caption = "Observation period is the period that we have data on unplanned closures. \nThis is October 2021 to the most recent month. N.B. Dispensing data has a lag of three months."
  )
}


################################################################################
plot_items_spc_facet <- function(unplanned_closures_data = unplannedClosuresData,
                                 items_data = items_dispensed,
                                 contractor_table = Ref_Contractor,
                                 level = "National",
                                 region_STP_name = NULL,
                                 plotChart = TRUE){
  
  p1 <- plot_items_SPC(unplanned_closures_data = unplanned_closures_data,
                       items_data = items_data,
                       contractor_table = contractor_table,
                       level = level,
                       region_STP_name = region_STP_name,
                       plotChart = TRUE,
                       with_closures = FALSE)
  
  p2 <- plot_items_SPC(unplanned_closures_data = unplanned_closures_data,
                       items_data = items_data,
                       contractor_table = contractor_table,
                       level = level,
                       region_STP_name = region_STP_name,
                       plotChart = TRUE,
                       with_closures = TRUE)
  
  ggpubr::ggarrange(p1 + theme(axis.title.x=element_blank(),
                               axis.text.x=element_blank(),
                               axis.ticks.x=element_blank()) +
                      labs(caption = NULL), 
                    p2 +
                      labs(caption = NULL), 
                    nrow = 2, ncol = 1,
                    common.legend = TRUE,
                    legend = "bottom",
                    align = "v")
  
}

################################################################################
plot_EPS_SPC <- function(unplanned_closures_data = unplannedClosuresData,
                         eps_data = EPS_nominations_historic,
                         contractor_table = Ref_Contractor,
                         with_closures = TRUE,
                         level = "National",
                         region_STP_name = NULL,
                         plotChart = TRUE){
  
  # #join in region column 
  # pharm_region_lookup <- contractor_table %>%
  #   select(`Contractor Code` = ODS.CODE, 
  #          Region_Name,
  #          ContractType) %>%
  #   distinct()
  # 
  # items_data <- items_data %>% 
  #   left_join(pharm_region_lookup, by = "Contractor Code") %>%
  #   dplyr::filter(ContractType != "DAC" & ContractType != "DSP" & ContractType != "LPS")
  
  #filter down to required level
  subtitle2 <-"England"
  
  if(level != "National"){
    
    eps_data <- eps_data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle2 <- region_STP_name
  }
  
  #vector of pharmacies that have had at least one closure
  pharms_with_closures <- unplanned_closures_data$ODS.CODE
  
  eps_data <- eps_data %>%
    #dplyr::filter(`Contractor Type` == "Pharmacy") %>%
    select(month, ODS.CODE, Current.Active.Nominations)
  
  if(with_closures == TRUE){
    data <- eps_data %>%
      dplyr::filter(ODS.CODE %in% pharms_with_closures) 
    
    title <- "Cohort 2: Pharmacies with closures in observation period"
    
  }else{
    data <- eps_data %>%
      dplyr::filter(!(ODS.CODE %in% pharms_with_closures))
    
    titlesub <- "Cohort 1: Pharmacies with no closures in observation period"
    title <- paste0("Total number of active EPS nominations\n", titlesub)
  }
  
  #num_pharms <- length(unique(data$ODS.CODE))
  
  number_of_pharms <- data %>%
    group_by(month) %>%
    count() %>%
    rename(num_pharms = n)
  
  num_pharms <- number_of_pharms %>%
    dplyr::filter(month == max(number_of_pharms$month))
  num_pharms <- num_pharms$num_pharms[1]
  
  data <- data %>%
    group_by(month) %>%
    summarise(monthly_eps = sum(Current.Active.Nominations, na.rm = TRUE)) 
  
  
  autospc::plot_auto_SPC(data,
                         x= month,
                         y = monthly_eps,
                         title = title,
                         subtitle = paste0(subtitle2, " (",num_pharms," pharmacies)"), 
                         override_x_title = "Date", 
                         override_y_title = "Total EPS nominations",
                         noRecals = T,
                         includeAnnotations = F
                         #caption = "Observation period is the period that we have data on unplanned closures. \nThis is October 2021 to the most recent month. N.B. Dispensing data has a lag of three months."
  )
}

################################################################################
get_small_chain_analysis <- function(data = unplannedClosuresData,
                                     per_pharm = FALSE,
                                     pharm_list = Ref_PharmList1,
                                     contractor_list = Ref_Contractor,
                                     plot_chart = TRUE,
                                     items_data = items_dispensed,
                                     eps_data = eps_nominations){
  
  
  #get region col in pharm list
  pharm_list <- pharm_list %>%
    rename(ODS.CODE = `ODS CODE`,
           ParentOrgName = `Organisation Name`) %>%
    left_join(Ref_Contractor)
  
  #num pharms by region
  num_pharms_per_chain_per_reg <- pharm_list %>%
    group_by(Region_Name, ParentOrgName) %>%
    count() %>%
    rename(num_pharms_per_chain_regionally = n)
  
  #num pharms nationally
  num_pharms_per_chain <- pharm_list %>%
    group_by(ParentOrgName) %>%
    count() %>%
    rename(num_pharms_per_chain_nationally = n)
  
  #get closures regionally
  regional_closures <- data %>%
    group_by(Region_Name, ParentOrgName) %>% ######################
  summarise(num_pharms_with_closures_in_region = n_distinct(ODS.CODE)) %>%
    ungroup() 
  
  #get closures nationally
  national_closures <- data %>%
    group_by(ParentOrgName) %>%
    summarise(num_pharms_with_closures_in_country = n_distinct(ODS.CODE)) %>%
    ungroup() 
  
  data <- regional_closures %>%
    left_join(national_closures, by = "ParentOrgName") %>%
    left_join(num_pharms_per_chain_per_reg, by = c("Region_Name", "ParentOrgName")) %>%
    left_join(num_pharms_per_chain, by = "ParentOrgName") %>%
    dplyr::filter(!is.na(ParentOrgName)) %>%
    mutate(perc_pharms_with_closure_in_region = num_pharms_with_closures_in_region * 100 / num_pharms_per_chain_regionally) %>%
    mutate(perc_pharms_with_closure_in_country = num_pharms_with_closures_in_country * 100 / num_pharms_per_chain_nationally) %>%
    arrange(ParentOrgName)
  
  
  #get dispensing data regionally and nationally 
  items_data <- items_data %>%
    rename(ODS.CODE = `Contractor Code`) %>%
    dplyr::filter(Month >= as.Date("2021-04-01") & Month <= as.Date("2022-03-01")) %>%
    left_join(pharm_list, by = "ODS.CODE") 
  
  items_regional <- items_data %>%
    group_by(ParentOrgName, Region_Name) %>%
    summarise(items_dispensed_2021_22_regionally = sum(Figure, na.rm = TRUE))
  
  items_national <- items_data %>%
    group_by(ParentOrgName) %>%
    summarise(items_dispensed_2021_22_nationally = sum(Figure, na.rm = TRUE))
  
  #get eps data regionally and nationally
  eps_data <- eps_data %>%
    left_join(pharm_list, by = "ODS.CODE")
  
  eps_regional <- eps_data %>%
    group_by(ParentOrgName, Region_Name) %>%
    summarise(eps_noms_2021_22_regionally = sum(eps_nominations, na.rm = TRUE))
  
  eps_national <- eps_data %>%
    group_by(ParentOrgName) %>%
    summarise(eps_noms_2021_22_nationally = sum(eps_nominations, na.rm = TRUE))
  
  #join in items data
  data <- data %>%
    left_join(items_regional, by = c("Region_Name", "ParentOrgName")) %>%
    left_join(items_national, by = "ParentOrgName") %>%
    left_join(eps_regional, by = c("Region_Name", "ParentOrgName")) %>%
    left_join(eps_national, by = "ParentOrgName")
  
  #get total eps and items data
  items_total_regional <- items_data %>%
    group_by(Region_Name) %>%
    summarise(total_items_dispensed_2021_22_regionally = sum(Figure, na.rm = TRUE))
  
  items_total_national <- items_data %>%
    summarise(total_items_dispensed_2021_22_nationally = sum(Figure, na.rm = TRUE))
  
  eps_total_regional <- eps_data %>%
    group_by(Region_Name) %>%
    summarise(total_eps_noms_2021_22_regionally = sum(eps_nominations, na.rm = TRUE))
  
  eps_total_national <- eps_data %>%
    summarise(total_eps_noms_2021_22_nationally = sum(eps_nominations, na.rm = TRUE))
  
  #join in totals
  data <- data %>%
    left_join(items_total_regional, by = "Region_Name") %>%
    left_join(items_total_national) %>%
    left_join(eps_total_regional, by = "Region_Name") %>%
    left_join(eps_total_national)
  
}


################################################################################
get_closures_by_IMD_and_region <- function(data = unplannedClosuresData,
                                           pharm_list = pharmlist,
                                           contractor_data = Ref_Contractor,
                                           level = "National",
                                           region_STP_name = NULL,
                                           plotChart = TRUE,
                                           data_month = as.Date("2022-09-01")){
  
  ods_decile_lookup <- contractor_data %>%
    select(ODS.CODE, IMD_Decile)
  
  pharmlist <- pharmlist %>%
    left_join(ods_decile_lookup, by = "ODS.CODE") 
  
  data <- data %>%
    dplyr::filter(month_of_closure == data_month)
  
  if(level != "National"){
    
    pharmlist <- pharmlist %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    data <- data %>%
      dplyr::filter(Region_Name == region_STP_name)
    
    subtitle <- region_STP_name
  }else{
    
    subtitle <- "England"
  }
  
  if(plotChart == TRUE){
    
    num_pharms_in_decile <- pharmlist %>%
      group_by(IMD_Decile) %>%
      count() %>%
      rename(total_num_pharms_in_decile = n)
    
    data <- data %>%
      group_by(IMD_Decile) %>%
      count() %>%
      rename(total_num_unplanned_closures = n) %>%
      left_join(num_pharms_in_decile, by = c("IMD_Decile")) %>%
      mutate(IMD_Decile = as.numeric(IMD_Decile)) %>%
      arrange(IMD_Decile) %>%
      mutate(closure_rate = total_num_unplanned_closures / total_num_pharms_in_decile) %>%
      dplyr::filter(!is.na(IMD_Decile))
    
    ggplot(data) +
      geom_col(aes(x = IMD_Decile,
                   y = closure_rate),
               fill = "steelblue") +
      theme_bw() +
      scale_x_continuous(breaks = seq(1, 10, 1)) +
      labs(title = "Closure rate across IMD Deciles",
           subtitle = paste0(subtitle, "\n", format(data_month, "%b-%Y")),
           x = "<- Most deprived    IMD Decile    Least deprived ->",
           y = "Number of closures per pharmacy")
    
  }else{
    
    num_pharms_in_decile <- num_pharms_in_decile %>%
      group_by(Region_Name, IMD_Decile) %>%
      count() %>%
      rename(total_num_pharms_in_decile = n)
    
    data <- data %>%
      group_by(Region_Name, IMD_Decile) %>%
      count() %>%
      rename(total_num_unplanned_closures = n) %>%
      left_join(num_pharms_in_decile, by = c("Region_Name", "IMD_Decile")) %>%
      mutate(IMD_Decile = as.numeric(IMD_Decile)) %>%
      arrange(Region_Name, IMD_Decile) %>%
      mutate(closure_rate = total_num_unplanned_closures / total_num_pharms_in_decile) %>%
      dplyr::filter(!is.na(Region_Name)) %>%
      dplyr::filter(!is.na(IMD_Decile))
    
  }
  
  
}


################################################################################
plot_closures_by_day_of_the_week <- function(data = unplannedClosuresData,
                                             pharm_list = pharmlist,
                                             contractor_data = Ref_Contractor,
                                             level = "National",
                                             region_STP_name = NULL,
                                             plotChart = TRUE,
                                             date_month = as.Date("2021-10-01")){
  
  #get rid of SOuth West July data which has everything set to the first of the month
  if(date_month == as.Date("2022-07-01")){
    
    data <- data %>%
      dplyr::filter(Region_Name != "South West")
    
    caption <- "N.B. South West data for this month had data quality issues so has been excluded from this graph."
  }else{
    caption <- ""
  }
  
  data <- data %>%
    mutate(DATE.OF.CLOSURE = as.Date(DATE.OF.CLOSURE)) %>%
    dplyr::filter(month_of_closure == date_month) %>%
    mutate(day_of_week = weekdays(DATE.OF.CLOSURE)) %>%
    group_by(day_of_week) %>%
    count()
  
  data$day_of_week <- factor(data$day_of_week, levels = c("Monday",
                                                          "Tuesday",
                                                          "Wednesday",
                                                          "Thursday",
                                                          "Friday",
                                                          "Saturday",
                                                          "Sunday"))
  
  ggplot(data) +
    geom_col(aes(x = day_of_week,
                 y = n),
             fill = "steelblue") +
    geom_text(aes(x = day_of_week,
                  y = n + 10,
                  label = n),
              size = 3) +
    theme_bw() +
    labs(title = "Number of closures by day of the week",
         subtitle = format(date_month, "%b-%Y"),
         x = "Day of the week",
         y = "Number of closures",
         caption = caption)
  
}

################################################################################
get_closures_split_100hr_vs_40hr <- function(data = unplannedClosuresData,
                                             pharm_list = pharmlist,
                                             contractor_data = Ref_Contractor){
  
  #get total number across england
  Is100hourPharmacy_lookup <- contractor_data %>%
    select(ODS.CODE, Is100hourPharmacy)
  
  totals_data <- pharmlist %>%
    left_join(Is100hourPharmacy_lookup, by = "ODS.CODE") %>%
    group_by(Is100hourPharmacy) %>%
    count() %>%
    rename(england_total_number_of_pharmacies = n)
  
  data <- data %>%
    group_by(Is100hourPharmacy) %>%
    count() %>%
    rename(number_of_closures = n) %>%
    left_join(totals_data, by = "Is100hourPharmacy") %>%
    mutate(closures_per_pharm = number_of_closures / england_total_number_of_pharmacies)
  
}



################################################################################
#pull_eps <- function(){
  
  #con <- dbConnect(odbc::odbc(), "NCDR")
  
 # sql <- " select *   FROM [NHSE_Sandbox_NHSBSA_PrescribingDispensing].[eps].[PrescriptionsCreated]"
 # result <- dbSendQuery(con,sql)
  #Ref_Contractor_full <- dbFetch(result)
 # dbClearResult(result)
  
#}


################################################################################
plot_yearly_items_dispensed <- function(data = items_dispensed,
                                        contractor_table = Ref_Contractor,
                                        exclude_DAC_DSP_LPS = TRUE){
  
  #join in region column 
  pharm_region_lookup <- contractor_table %>%
    select(`Contractor Code` = ODS.CODE, 
           Region_Name,
           ContractType) %>%
    distinct()
  
  if(exclude_DAC_DSP_LPS){
    data <- data %>% 
      left_join(pharm_region_lookup, by = "Contractor Code") %>%
      dplyr::filter(ContractType != "DAC" & ContractType != "DSP" & ContractType != "LPS")
    
    subtitle <- "Excluding DAC, DSP and LPS contracts"
  }else{
    
    subtitle <- "Including DAC, DSP and LPS contracts"
  }
  
  data <- data %>%
    mutate(Month = as.Date(Month)) %>%
    mutate(year = lubridate::floor_date(Month, unit = "years")) %>%
    mutate(financial_year = case_when(Month >= as.Date("2014-04-01") & Month < as.Date("2015-04-01") ~ "2014/15",
                                      Month >= as.Date("2015-04-01") & Month < as.Date("2016-04-01") ~ "2015/16",
                                      Month >= as.Date("2016-04-01") & Month < as.Date("2017-04-01") ~ "2016/17",
                                      Month >= as.Date("2017-04-01") & Month < as.Date("2018-04-01") ~ "2017/18",
                                      Month >= as.Date("2018-04-01") & Month < as.Date("2019-04-01") ~ "2018/19",
                                      Month >= as.Date("2019-04-01") & Month < as.Date("2020-04-01") ~ "2019/20",
                                      Month >= as.Date("2020-04-01") & Month < as.Date("2021-04-01") ~ "2020/21",
                                      Month >= as.Date("2021-04-01") & Month < as.Date("2022-04-01") ~ "2021/22",
                                      Month >= as.Date("2022-04-01") & Month < as.Date("2023-04-01") ~ "2022/23"
    )) %>%
    group_by(financial_year) %>%
    summarise(total_items_dispensed = sum(Figure, na.rm = TRUE)) %>%
    mutate(total_items_dispensed = if_else(financial_year == "2022/23", 
                                           total_items_dispensed * 12/3, #data up to june
                                           total_items_dispensed)) %>%
    dplyr::filter(financial_year != "2014/15" & financial_year != "2022/23")
  
  ggplot(data) +
    geom_col(aes(x = financial_year,
                 y = total_items_dispensed / 1000000),
             fill = "steelblue") +
    theme_bw() +
    scale_y_continuous() +
    labs(title = "Total number of items dispensed per year (millions)",
         subtitle = subtitle,
         x = "Financial year",
         y = "Items dispensed (millions)"#,
         #caption = "There is only data up to June for 2022/23 data so this has been multiplied up to a full year."
    ) +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  
}


################################################################################
plot_monthly_items_dispensed <- function(data = items_dispensed,
                                         contractor_table = Ref_Contractor,
                                         exclude_DAC_DSP_LPS = TRUE){
  
  #join in region column 
  pharm_region_lookup <- contractor_table %>%
    select(`Contractor Code` = ODS.CODE, 
           Region_Name,
           ContractType) %>%
    distinct()
  
  if(exclude_DAC_DSP_LPS){
    data <- data %>% 
      left_join(pharm_region_lookup, by = "Contractor Code") %>%
      dplyr::filter(ContractType != "DAC" & ContractType != "DSP" & ContractType != "LPS")
    
    subtitle <- "Excluding DAC, DSP and LPS contracts"
  }else{
    
    subtitle <- "Including DAC, DSP and LPS contracts"
  }
  
  data <- data %>%
    mutate(Month = as.Date(Month)) %>%
    mutate(year = lubridate::floor_date(Month, unit = "years")) %>%
    mutate(financial_year = case_when(Month >= as.Date("2014-04-01") & Month < as.Date("2015-04-01") ~ "2014/15",
                                      Month >= as.Date("2015-04-01") & Month < as.Date("2016-04-01") ~ "2015/16",
                                      Month >= as.Date("2016-04-01") & Month < as.Date("2017-04-01") ~ "2016/17",
                                      Month >= as.Date("2017-04-01") & Month < as.Date("2018-04-01") ~ "2017/18",
                                      Month >= as.Date("2018-04-01") & Month < as.Date("2019-04-01") ~ "2018/19",
                                      Month >= as.Date("2019-04-01") & Month < as.Date("2020-04-01") ~ "2019/20",
                                      Month >= as.Date("2020-04-01") & Month < as.Date("2021-04-01") ~ "2020/21",
                                      Month >= as.Date("2021-04-01") & Month < as.Date("2022-04-01") ~ "2021/22",
                                      Month >= as.Date("2022-04-01") & Month < as.Date("2023-04-01") ~ "2022/23",
                                      Month >= as.Date("2023-04-01") & Month < as.Date("2024-04-01") ~ "2023/24",
                                      Month >= as.Date("2024-04-01") & Month < as.Date("2025-04-01") ~ "2024/25"
    )) %>%
    group_by(Month) %>%
    summarise(total_items_dispensed = sum(Figure, na.rm = TRUE))

  ggplot(data) +
    geom_line(aes(x = Month,
                  y = total_items_dispensed / 1000000),
              colour = "steelblue") +
    geom_point(aes(x = Month,
                   y = total_items_dispensed / 1000000),
               colour = "steelblue") +
    theme_bw() +
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y") +
    scale_y_continuous() +
    labs(title = "Total number of items dispensed per month (millions)",
         subtitle = subtitle,
         x = "Month",
         y = "Items dispensed (millions)") +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1)) #+
  # geom_smooth(aes(x = Month,
  #                 y = total_items_dispensed / 1000000),
  #             method=lm,
  #             se=FALSE,
  #             colour = "steelblue",
  #             size = 0.5)

  
}


################################################################################
get_closure_durations_for_pharm_level_maps <- function(data = unplannedClosuresData,
                                                       pharm_list = pharmlist){
  
  
  #get rid of errors and convert duration format
  data <- data %>%
    dplyr::filter(DURATION.OF.CLOSURE < 1000) %>%
    mutate(closure_duration_hours = as.numeric(DURATION.OF.CLOSURE) * 24) 
  
  data <- data %>%
    group_by(month_of_closure, Region_Name, STP.Name, ParentOrgName, ODS.CODE) %>%
    summarise(closure_duration_hours = sum(closure_duration_hours, na.rm = TRUE)) 
  
  data <- data %>%
    dplyr::filter(!is.na(STP.Name)) %>%
    dplyr::filter(month_of_closure >= lubridate::floor_date(Sys.Date() - lubridate::weeks(16), unit = "month")) #filter to get most recent three months of data
  
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
    left_join(data_totals, by = "ODS.CODE")
  
  pharm_list <- pharm_list %>%
    rename(STP.Name = `STP Name`,
           ParentOrgName = `Organisation Name`)
  
  data <- data %>%
    full_join(pharm_list, by = "ODS.CODE") %>%
    mutate(Region_Name = case_when(is.na(Region_Name.x) ~ Region_Name.y,
                                   TRUE ~ Region_Name.x)) %>%
    mutate(STP.Name = case_when(is.na(STP.Name.x) ~ STP.Name.y,
                                TRUE ~ STP.Name.x)) %>%
    mutate(ParentOrgName = case_when(is.na(ParentOrgName.x) ~ ParentOrgName.y,
                                     TRUE ~ ParentOrgName.x)) %>%
    select(`ODS Code` = ODS.CODE,
           `Organisation Name` = ParentOrgName,
           `STP Name` = STP.Name,
           `Region Name` = Region_Name,
           everything()) %>%
    select(-c(Region_Name.x, Region_Name.y, STP.Name.x, STP.Name.y, ParentOrgName.x, ParentOrgName.y))
  
  upper_quartile <- summary(data$total_closure_duration_last_3_months)
  upper_quartile <- as.numeric(upper_quartile["3rd Qu."])
  
  data <- data %>%
    mutate(total_closure_duration_last_3_months = if_else(is.na(total_closure_duration_last_3_months),
                                                          0,
                                                          total_closure_duration_last_3_months)) %>%
    mutate(colour = case_when(total_closure_duration_last_3_months == 0 ~ "Green",
                              total_closure_duration_last_3_months <= upper_quartile ~ "Amber",
                              total_closure_duration_last_3_months > upper_quartile ~ "Red",
                              TRUE ~ "Invalid closure duration")) %>%
    rename(`Total closure duration over last 3 months (hours)` = total_closure_duration_last_3_months)
  
  
}

################################################################################
get_scorecard_data <- function(data = unplannedClosuresData,
                               pharm_list = full_pharmlist, #pharmacy_list_september2022,
                               contractor_list = Ref_Contractor){
  
  #get closures per 100 pharmacies 
  
  #join to get region column in pharm list
  ods_region_lookup <- contractor_list %>%
    select(ODS.CODE, Region_Name) %>%
    distinct()
  
  pharm_list <- pharm_list %>%
    dplyr::filter(Contract.Type != "DAC") %>%
    dplyr::filter(SnapshotMonth >= as.Date("2021-09-01")) %>%
    select(ODS.CODE = ODS.CODE,
           ICB.Name,
           ICB.Code = STP.Code,
           SnapshotMonth)
  
  #get number of pharmacies in each ICB for each snapshot month 
  pharms_per_ICB <- pharm_list %>%
    group_by(SnapshotMonth, ICB.Code) %>%
    count() %>%
    rename(pharms_per_ICB = n)
  
  #get number of pharmacies in each ICB for latest snapshot month only
  latest_pharms_per_ICB <- pharms_per_ICB %>%
    dplyr::filter(SnapshotMonth == max(pharm_list$SnapshotMonth)) %>%
    ungroup() %>%
    select(-SnapshotMonth,
           pharms_per_ICB_latest = pharms_per_ICB)
  
  #get closures in each org per 100 pharmacies
  data <- data %>%
    rename(ICB.Code = STP) %>%
    group_by(month_of_closure, Region_Name, ICB.Code) %>%
    count() %>%
    mutate(SnapshotMonth = lubridate::floor_date(lubridate::floor_date(month_of_closure, unit = "quarter") - lubridate::month(1), unit = "month")) %>%
    left_join(pharms_per_ICB, by = c("SnapshotMonth", "ICB.Code")) %>%
    left_join(latest_pharms_per_ICB, by = c("ICB.Code")) %>%
    mutate(pharms_per_ICB = if_else(is.na(pharms_per_ICB),
                                    pharms_per_ICB_latest,
                                    pharms_per_ICB)) %>%
    mutate(closures_per_100_pharms = n * 100 / pharms_per_ICB) %>%
    select(month_of_closure,
           region_name = Region_Name,
           ICB_code = ICB.Code,
           number_of_unplanned_closures = n,
           number_of_pharmacies_in_ICB_excl_DAC = pharms_per_ICB,
           closures_per_100_pharmacies = closures_per_100_pharms)
  
  #write data to NCDR
 # con <- dbConnect(odbc::odbc(), "NCDR")

#  dbWriteTable(con, Id(catalog="NHSE_Sandbox_DispensingReporting",schema="metric",table="unplanned_closures_ICB"),
              # value = data, row.names = FALSE, append=FALSE, overwrite=TRUE)

}

################################################################################
get_num_active_pharms <- function(pharm_list_data = full_pharmlist){
  
  pharm_list_data <- pharm_list_data %>%
    ungroup() %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth)) %>%
    dplyr::filter(Contract.Type != "DAC") %>%
    count()
  
  pharm_list_data$n[[1]]
}

################################################################################
get_perc_pharms_with_closure <- function(data = unplannedClosuresData, 
                                         pharm_list_data = full_pharmlist){
  
  active_pharms <- pharm_list_data %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth)) %>%
    dplyr::filter(Contract.Type != "DAC")
  
  data <- data %>%
    select(ODS.CODE) %>%
    distinct() %>%
    dplyr::filter(ODS.CODE %in% active_pharms$ODS.CODE)
  
  num_active_pharms_with_reported_closures <- n_distinct(data$ODS.CODE)
  
  num_active_pharms <- get_num_active_pharms()
  
  perc_pharms_with_reported_closures <- num_active_pharms_with_reported_closures * 100 / num_active_pharms
  
  round(perc_pharms_with_reported_closures, 1)
  
}

################################################################################
get_100hr_to_40hr_closures_ratio <- function(data = unplannedClosuresData,
                                             pharm_list_data = full_pharmlist){
  
  #get number of active pharmacies in each category
  pharm_list_data <- pharm_list_data %>%
    dplyr::filter(SnapshotMonth == max(SnapshotMonth)) %>%
    dplyr::filter(Contract.Type != "DAC") %>%
    rename(Is100hourPharmacy = X100.Hour.Pharmacy) %>%
    group_by(Is100hourPharmacy) %>%
    count() %>%
    rename(num_pharmacies = n)
  
  data <- data %>%
    group_by(Is100hourPharmacy) %>%
    count() %>%
    rename(num_closures = n) %>%
    left_join(pharm_list_data, by = "Is100hourPharmacy") %>%
    mutate(closures_per_pharm = num_closures / num_pharmacies)
  
  closures_per_pharm_100hrs <- dplyr::filter(data, Is100hourPharmacy == "Yes")$closures_per_pharm[1]
  closures_per_pharm_40hrs <- dplyr::filter(data, Is100hourPharmacy == "No")$closures_per_pharm[1]
  
  ratio <- round(closures_per_pharm_100hrs / closures_per_pharm_40hrs, 1)
  ratio
}

################################################################################
get_closure_number_change <- function(data = unplannedClosuresData){
  
  data <- plot_closures_by_month(data,
                                 level = "National",
                                 region_STP_name = NULL,
                                 per_100_pharms = FALSE, plotChart = F)
  
  data <- data %>%
    group_by(month_of_closure) %>%
    summarise(n= sum(n)) %>%
    mutate(month_name = format(month_of_closure, "%B")) %>%
    dplyr::filter(month_name == format(max(data$month_of_closure), "%B")) %>%
    arrange(desc(month_of_closure)) %>%
    head(2)
            
  this_year <- data$n[1]  
  last_year <- data$n[2]
  
  percentage_change <- (this_year - last_year) * 100 / last_year
  round(percentage_change, 1)
  
}