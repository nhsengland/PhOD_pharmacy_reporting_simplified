library(DBI)
library(tidyverse)
library(writexl)
library(readxl)
library(openxlsx)
###################     Patient Counts #############################################################
LSOA_Coords<-read.csv("N:/_Everyone/Primary Care Group/Pharmacy SMT data pack/Clustering/Lower_Layer_Super_Output_Areas_(December_2011)_Population_Weighted_Centroids.csv")
#LSOA_IMD_Decile<-read.xlsx("N:/_Everyone/Primary Care Group/Pharmacy SMT data pack/Clustering/File_1_-_IMD2019_Index_of_Multiple_Deprivation.xlsx", sheet= "IMD2019")
LSOA_IMD_Decile<- read.csv("N:/_Everyone/Primary Care Group/Pharmacy SMT data pack/Clustering/LSOA_IMD_Deciles.csv")
  
colnames(LSOA_Coords) <- c("Patient Easting","Patient Northing","objectid","lsoa11cd","lsoa11nm")
#colnames(LSOA_IMD_Decile) <- c("FeatureCode","lsoa11name","LADcode","LADname","IMDrank","IMD Decile")

LSOA_Coords1<-LSOA_Coords 


con <- dbConnect(odbc::odbc(), "NCDR")
sql="
SELECT  
[LSOA_2011_Code]
FROM [NHSE_PSDM_0025].[QA].[tbl_MPI]
"
result<-dbSendQuery(con,sql)
MPI<-dbFetch(result)
dbClearResult(result)


Patients_Counts_by_LSOA<-MPI %>%
  group_by(LSOA_2011_Code) %>%
  summarise(n_patients=n())

Patient_Counts_by_LSOA_Coords<-Patients_Counts_by_LSOA %>%
  full_join(LSOA_Coords1,by=c("LSOA_2011_Code"="lsoa11cd")) %>%
  rename(`Patient LSOA` =`LSOA_2011_Code`)



######################Patient Counts By Pharmacy #############################################

PharmacyList_Latest2<-Ref_PharmList_full%>%
  filter(SnapshotMonth==max(SnapshotMonth))%>%
  select(FCode=`Pharmacy ODS Code (F-Code)`, `Post Code`)%>%
  mutate(postcode_lkup=str_replace(`Post Code`," ",""))%>%
  left_join(latest_contractor[c("FCode","IMD_Decile","LSOA", "LSOA_Name" )], "FCode")%>%
  collect()


Pharmacies_By_LSOA<-PharmacyList_Latest2 %>%
  group_by(LSOA)%>%
  summarise(n_pharmacies=n())


##################### LSOA Pharmacies Per Patient ################################################

LSOA_Pharmacies_Per_Patient<-Pharmacies_By_LSOA %>%
  full_join(Patient_Counts_by_LSOA_Coords,by=c("LSOA"="Patient LSOA"))


####################  LSOA Patient Distance To Pharmacy    ###########################################
#use Ctrl+Shift_c to comment out a block of codes 
#------ This block of codes are commented out to reduce reporting running time, but needs to be ran the first time ----
# Pharmacy_Patient_Dist<-PharmacyList_Latest2 %>%
#   left_join(Postcode_Lookup1,by="postcode_lkup")%>%
#   merge(Patient_Counts_by_LSOA_Coords,all=TRUE) %>%
#   filter(is.na(`Patient Easting`)==FALSE)%>%
#   mutate(Distance_km=sqrt((as.numeric(`Patient Easting`)-as.numeric(`Easting`))^2+(as.numeric(`Patient Northing`)-as.numeric(`Northing`))^2)/1000) 
# 
#saveRDS(Pharmacy_Patient_Dist,"~/Rprojects/Pharmacy_Resilience_Report/pharm_patient_distance.rds" )
#Pharmacy_Patient_Dist <- readRDS("~/Rprojects/Pharmacy_Resilience_Report/pharm_patient_distance.rds")

# Pharmacy_Patient_Dist_count<-Pharmacy_Patient_Dist %>%
#   filter(is.na(Distance_km)!=TRUE,substr(`Patient LSOA`,1,1)!="W",Distance_km<=5) %>%
#   group_by(`Patient LSOA`) %>%
#   mutate(noPharm_5km=n_distinct(FCode)) %>%
#   select(`Patient LSOA`,noPharm_5km)%>%
#   distinct()

#  Pharmacy_Patient_Dist1<-Pharmacy_Patient_Dist %>%
#    filter(is.na(Distance_km)!=TRUE,substr(`Patient LSOA`,1,1)!="W") %>%
#    group_by(`Patient LSOA`) %>%
#    mutate(min_dist=min(Distance_km)) %>%
#    filter(Distance_km==min_dist)%>%
#    mutate(min_dist_cat=ifelse(min_dist<1,"< 1km",ifelse(min_dist<2,"between 1 and 2km",ifelse(min_dist<5,"between 2 and 5km",ifelse(min_dist<10,"between 5 and 10km",ifelse(min_dist<20,"between 10 and 20km",ifelse(min_dist>=20,"20km or further")))))))
# 
#  Pharmacy_Patient_Dist2<- Pharmacy_Patient_Dist_count%>%
#     right_join(Pharmacy_Patient_Dist1, "Patient LSOA")
# 
#  Pharmacy_Patient_Dist2$noPharm_5km<-ifelse(is.na(Pharmacy_Patient_Dist2$noPharm_5km), 0, Pharmacy_Patient_Dist2$noPharm_5km)

# saveRDS(Pharmacy_Patient_Dist1,"~/Rprojects/Pharmacy_Resilience_Report/pharm_patient_distance1.rds" )
    # saveRDS(Pharmacy_Patient_Dist2,"~/Rprojects/Pharmacy_Resilience_Report/pharm_patient_distance2.rds" )
Pharmacy_Patient_Dist1 <- readRDS("~/Rprojects/Pharmacy_Resilience_Report/pharm_patient_distance1.rds")

Pharmacy_Patient_Dist_Output<-Pharmacy_Patient_Dist1 %>%
  select(-Easting,-Northing, -`Patient Easting`,-`Patient Northing`,-Longitude, -Latitude,-Postcode_1,-Objectid) %>%
  left_join(LSOA_IMD_Decile,by=c("Patient LSOA"="FeatureCode"))

patient_pharm_summary<-Pharmacy_Patient_Dist_Output%>%
  group_by(min_dist_cat)%>%
  summarise(no_patient=sum(n_patients, na.rm=T))%>%
  ungroup()%>%
  mutate(total_patients=sum(no_patient),patients_percentage=no_patient/total_patients)


patient_pharm_summary1<- Pharmacy_Patient_Dist_Output%>%
  group_by(`IMD.Decile`,`min_dist_cat`)%>%
  summarise(no_patient=sum(n_patients, na.rm=T))

patient_pharm_summary2<-Pharmacy_Patient_Dist_Output%>%
  group_by(`IMD.Decile`)%>%
  summarise(total_IMD_patients=sum(n_patients, na.rm=T))%>%
  right_join(patient_pharm_summary1, "IMD.Decile")%>%
  mutate(patients_percentage=no_patient/total_IMD_patients)

patient_pharm_summary_IMD<-patient_pharm_summary2%>%
  pivot_wider(id_cols=`IMD.Decile` ,names_from=min_dist_cat,values_from=patients_percentage)

plot_patient_pie<-function(){
  
  
    data<-patient_pharm_summary
  
  data<-data%>%
    mutate(patients_percentage=round(patients_percentage*100,1))
  
  df<-data%>%
    mutate(end = 2 * pi * cumsum(patients_percentage)/sum(patients_percentage),
           start = lag(end, default = 0),
           middle = 0.5 * (start + end),
           hjust = ifelse(middle > pi, 1, 0),
           vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
  
  df$`min_dist_cat`<-factor( df$`min_dist_cat`, levels = c("< 1km","between 1 and 2km","between 2 and 5km","between 5 and 10km","between 10 and 20km","20km or further" ))
  
  pie1<-ggplot(df) + 
    geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                     start = start, end = end, fill = min_dist_cat)) +
    geom_text(aes(x = 1 * sin(middle), y = 1 * cos(middle), label =paste0(paste(round(patients_percentage, 1), "%")),
                  hjust = hjust, vjust = vjust)) +
    coord_fixed() +
    scale_fill_brewer(palette = "Pastel1") +
    scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                       name = "", breaks = NULL, labels = NULL) +
    scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                       name = "", breaks = NULL, labels = NULL)+
    labs(fill = NULL, x = NULL,y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
         title = paste0("% of patients in England in each category of distance to nearest pharmacy"))
  
  
  pie1
}

plot_patients<- function(){
  
    data<-patient_pharm_summary2
    
    title <- paste0("% of patients in each category of distance to nearest pharmacy breakdown by IMD")
    data$`IMD.Decile`<- factor( data$`IMD.Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
    data<-data%>%mutate(x=`IMD.Decile`, y=`patients_percentage`, fill=`min_dist_cat`)
    data$fill<-factor( data$fill, levels = c("< 1km","between 1 and 2km","between 2 and 5km","between 5 and 10km","between 10 and 20km","20km or further" ))
    fill_label="Crow flies distance to nearest pharmacy"
    xlab= "IMD_Decile (2019)"
    ylab="% of patients"
  
  p1<- ggplot(data, 
              aes(x = x, 
                  y = y, 
                  fill = str_wrap(fill, 30), label= paste0(round(y*100,1), "%"))) +
  geom_col() + geom_text(position = position_stack(reverse = FALSE,vjust = 0.5))+
    theme_bw()+ 
    scale_fill_brewer(palette = "Pastel1") +
    scale_y_continuous( labels = percent_format()) +
    labs(title = str_wrap(title, 90), caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
         fill = fill_label)+
    xlab(xlab)+
    ylab(ylab) 
  
  p1
}

