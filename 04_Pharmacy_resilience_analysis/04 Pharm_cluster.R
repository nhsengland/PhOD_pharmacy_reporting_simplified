
library(geosphere)
library(dplyr)
library(DBI)
library(AER)
library(tidyverse)
library(ggplot2)
library(ggpubr)
library(stringr)
library(magrittr) # needs to be run every time you start R and want to use %>%
library(scales)
library(readxl)
library(textclean)
library(lubridate)
library(ggrepel)

con <- dbConnect(odbc::odbc(), "NCDR")

sql="

select 	 [Postcode_1]
,[Easting]
,[Northing]
,[Longitude]
,[Latitude]
,[Is_Latest]

FROM [NHSE_UKHF].[Other].[vw_National_Statistics_Postcode_Lookup_SCD]
where Is_Latest=1"


result<-dbSendQuery(con,sql)
PostcodeLookup<-dbFetch(result)
dbClearResult(result)

Postcode_Lookup1<-PostcodeLookup %>%
  mutate(postcode_lkup=str_replace(Postcode_1," ",""))

PharmacyList_cluster<-Ref_PharmList_full%>%
  filter(SnapshotMonth==max(SnapshotMonth))%>%
  select(FCode=`Pharmacy ODS Code (F-Code)`, `Post Code`,`Pharmacy Trading Name`,`Organisation Name`)%>%
  mutate(postcode_lkup=str_replace(`Post Code`," ",""))%>%
  left_join(latest_contractor[c("FCode","ParentOrgName","ParentOrgSize","IMD_Decile")], "FCode")%>%
  collect()

Pharmacy_List_Postcodes<-PharmacyList_cluster %>%
  left_join(Postcode_Lookup1,by="postcode_lkup")%>%
  filter(!is.na(Longitude))%>%
  filter(!is.na(Latitude))%>%
  mutate(Longitude=as.numeric(Longitude), Latitude=as.numeric(Latitude))

##### All pharm clustering ----

Pharmacy_List_Postcodes_copy<-Pharmacy_List_Postcodes
temp1 <- purrr::map2_dfr(Pharmacy_List_Postcodes$Longitude, Pharmacy_List_Postcodes$Latitude,
                         ~spatialrisk::points_in_circle(Pharmacy_List_Postcodes_copy, .x, .y, Longitude,
                                                        Latitude, radius = 804.672)[2,])

temp2 <- purrr::map2_dfr(Pharmacy_List_Postcodes$Longitude, Pharmacy_List_Postcodes$Latitude,
                         ~spatialrisk::points_in_circle(Pharmacy_List_Postcodes_copy, .x, .y, Longitude,
                                                        Latitude, radius = 804.672)[3,])

temp3 <- purrr::map2_dfr(Pharmacy_List_Postcodes$Longitude, Pharmacy_List_Postcodes$Latitude,
                         ~spatialrisk::points_in_circle(Pharmacy_List_Postcodes_copy, .x, .y, Longitude,
                                                        Latitude, radius = 804.672)[4,])

temp1<-temp1%>%select(`NearestPharmacy` =`FCode`, `ToNearest(m)`=`distance_m`,`1st_ParentOrg`=`ParentOrgName`)%>%collect()

temp2<- temp2%>% select(`2ndNearestPharmacy`=`FCode`, `To2ndNearest(m)`=`distance_m`,`2nd_ParentOrg`=`ParentOrgName`)%>%collect()

temp3<- temp3%>% select(`3rdNearestPharmacy`=`FCode`, `To3rdNearest(m)`=`distance_m`,`3rd_ParentOrg`=`ParentOrgName`)%>%collect()

master <-cbind(Pharmacy_List_Postcodes, temp1, temp2, temp3)

temp4<- master%>%filter(FCode==`NearestPharmacy`)
temp5<- master%>%filter(!FCode %in% temp4$FCode)

temp4<-temp4%>%
  left_join(temp5[c("FCode", "ParentOrgName","NearestPharmacy","ToNearest(m)")], "NearestPharmacy")%>%
  mutate(`NearestPharmacy`=`FCode.y`, `ToNearest(m)`=`ToNearest(m).y`, `1st_ParentOrg`=`ParentOrgName.y`, `FCode`=`FCode.x`, `ParentOrgName`=`ParentOrgName.x`)%>%
  select(-`FCode.x`,-`FCode.y`,-`ToNearest(m).x`, -`ToNearest(m).y`,-`ParentOrgName.x`, -`ParentOrgName.y`)

temp6<-temp4%>%filter(`NearestPharmacy`==`2ndNearestPharmacy`)

master<-rbind(temp4, temp5)%>%
  filter( !(`NearestPharmacy`==`2ndNearestPharmacy` &`ToNearest(m)`==`To2ndNearest(m)` & FCode %in% temp6$FCode))

master$`cluster`<- ifelse(is.na(master$NearestPharmacy)&
                            is.na(master$`2ndNearestPharmacy`)&
                            is.na(master$`3rdNearestPharmacy`), 'A- No Clustering', 
                          ifelse(!is.na(master$NearestPharmacy) &
                                   !is.na(master$`2ndNearestPharmacy`)&
                                   !is.na(master$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                 ifelse(is.na(master$NearestPharmacy) &
                                          !is.na(master$`2ndNearestPharmacy`)&
                                          !is.na(master$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                        ifelse(!is.na(master$NearestPharmacy) &
                                                 is.na(master$`2ndNearestPharmacy`)&
                                                 !is.na(master$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                               ifelse(!is.na(master$NearestPharmacy) &
                                                        !is.na(master$`2ndNearestPharmacy`)&
                                                        is.na(master$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                                      'B- Cluster of 2'
                                               )))))



Pharmacy_Cluster_Counts<-master %>%
  group_by(cluster)%>%
  summarise(n_pharmacies=n())%>%
  ungroup()%>%
  mutate(total_pharmacies=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_pharmacies)

Pharmacy_Cluster_Counts_summary <-Pharmacy_Cluster_Counts%>%
  pivot_wider(id_cols=total_pharmacies,names_from=cluster,values_from=cluster_size_percentage)



IMD_Cluster_Counts<-master %>%
  group_by(IMD_Decile,cluster)%>%
  summarise(n_pharmacies=n())%>%
  group_by(IMD_Decile)%>%
  mutate(total_IMD_decile_pharmacies=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_IMD_decile_pharmacies)
#write.csv(IMD_Cluster_Counts,"../SMT-pharmacy-report/reports\\IMD_Cluster_Counts.csv", row.names = FALSE) 


IMD_Cluster_Counts_summary<-IMD_Cluster_Counts%>%
  pivot_wider(id_cols=IMD_Decile ,names_from=cluster,values_from=cluster_size_percentage)



OrgSize_Cluster_Counts<-master %>%
  group_by(ParentOrgSize,cluster)%>%
  summarise(n_pharmacies=n())%>%
  group_by(ParentOrgSize)%>%
  mutate(total_RegionName_pharmacies=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_RegionName_pharmacies)

OrgSize_Cluster_Counts_summary<-OrgSize_Cluster_Counts%>%
  pivot_wider(id_cols=ParentOrgSize ,names_from=cluster,values_from=cluster_size_percentage)

 ##### Closures clustering (in past 12 months) ----

Closure_cluster<-function(m=12){
PharmacyList_cluster_previous<-Ref_PharmList_full%>%
  filter(SnapshotMonth==(max(SnapshotMonth)%m-% months(m)))%>%
  select(FCode=`Pharmacy ODS Code (F-Code)`, `Post Code`,`Pharmacy Trading Name`,`Organisation Name`)%>%
  mutate(postcode_lkup=str_replace(`Post Code`," ",""))%>%
  left_join(latest_contractor[c("FCode","ParentOrgName","ParentOrgSize","IMD_Decile")], "FCode")%>%
  collect()

Pharmacy_List_Postcodes_previous<-PharmacyList_cluster_previous %>%
  left_join(Postcode_Lookup1,by="postcode_lkup")%>%
  filter(!is.na(Longitude))%>%
  filter(!is.na(Latitude))%>%
  mutate(Longitude=as.numeric(Longitude), Latitude=as.numeric(Latitude))

Pharmacy_List_Postcodes_previous_copy<-Pharmacy_List_Postcodes_previous
temp1b <- purrr::map2_dfr(Pharmacy_List_Postcodes_previous$Longitude, Pharmacy_List_Postcodes_previous$Latitude,
                         ~spatialrisk::points_in_circle(Pharmacy_List_Postcodes_previous_copy, .x, .y, Longitude,
                                                        Latitude, radius = 804.672)[2,])

temp2b <- purrr::map2_dfr(Pharmacy_List_Postcodes_previous$Longitude, Pharmacy_List_Postcodes_previous$Latitude,
                         ~spatialrisk::points_in_circle(Pharmacy_List_Postcodes_previous_copy, .x, .y, Longitude,
                                                        Latitude, radius = 804.672)[3,])

temp3b <- purrr::map2_dfr(Pharmacy_List_Postcodes_previous$Longitude, Pharmacy_List_Postcodes_previous$Latitude,
                         ~spatialrisk::points_in_circle(Pharmacy_List_Postcodes_previous_copy, .x, .y, Longitude,
                                                        Latitude, radius = 804.672)[4,])

temp1b<-temp1b%>%select(`NearestPharmacy` =`FCode`, `ToNearest(m)`=`distance_m`,`1st_ParentOrg`=`ParentOrgName`)%>%collect()

temp2b<- temp2b%>% select(`2ndNearestPharmacy`=`FCode`, `To2ndNearest(m)`=`distance_m`,`2nd_ParentOrg`=`ParentOrgName`)%>%collect()

temp3b<- temp3b%>% select(`3rdNearestPharmacy`=`FCode`, `To3rdNearest(m)`=`distance_m`,`3rd_ParentOrg`=`ParentOrgName`)%>%collect()

master_previous <-cbind(Pharmacy_List_Postcodes_previous, temp1b, temp2b, temp3b)

temp4b<- master_previous%>%filter(FCode==`NearestPharmacy`)
temp5b<- master_previous%>%filter(!FCode %in% temp4$FCode)

temp4b<-temp4b%>%
  left_join(temp5b[c("FCode", "ParentOrgName","NearestPharmacy","ToNearest(m)")], "NearestPharmacy")%>%
  mutate(`NearestPharmacy`=`FCode.y`, `ToNearest(m)`=`ToNearest(m).y`, `1st_ParentOrg`=`ParentOrgName.y`, `FCode`=`FCode.x`, `ParentOrgName`=`ParentOrgName.x`)%>%
  select(-`FCode.x`,-`FCode.y`,-`ToNearest(m).x`, -`ToNearest(m).y`,-`ParentOrgName.x`, -`ParentOrgName.y`)

temp6b<-temp4b%>%filter(`NearestPharmacy`==`2ndNearestPharmacy`)

master_previous<-rbind(temp4b, temp5b)%>%
  filter( !(`NearestPharmacy`==`2ndNearestPharmacy` &`ToNearest(m)`==`To2ndNearest(m)` & FCode %in% temp6$FCode))

master_previous$`cluster`<- ifelse(is.na(master_previous$NearestPharmacy)&
                            is.na(master_previous$`2ndNearestPharmacy`)&
                            is.na(master_previous$`3rdNearestPharmacy`), 'A- No Clustering', 
                          ifelse(!is.na(master_previous$NearestPharmacy) &
                                   !is.na(master_previous$`2ndNearestPharmacy`)&
                                   !is.na(master_previous$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                 ifelse(is.na(master_previous$NearestPharmacy) &
                                          !is.na(master_previous$`2ndNearestPharmacy`)&
                                          !is.na(master_previous$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                        ifelse(!is.na(master_previous$NearestPharmacy) &
                                                 is.na(master_previous$`2ndNearestPharmacy`)&
                                                 !is.na(master_previous$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                               ifelse(!is.na(master_previous$NearestPharmacy) &
                                                        !is.na(master_previous$`2ndNearestPharmacy`)&
                                                        is.na(master_previous$`3rdNearestPharmacy`), "C- Cluster of 3 and more",
                                                      'B- Cluster of 2'
                                               )))))
closure_12m<-pharm_active%>%
  filter(!is.na(closed))%>%
  filter(closed==(max(closed)%m-% months(m-3)))%>%collect()

closure_Cluster_Counts<-master_previous %>%
  right_join(closure_12m,"FCode")%>%
  group_by(cluster, closed)%>%
  summarise(n_pharmacies=n())%>%
  ungroup()%>%
  mutate(total_pharmacies=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_pharmacies)

closure_Cluster_Counts
}

closure_Cluster_Counts<-rbind(Closure_cluster(12),Closure_cluster(9),Closure_cluster(6),Closure_cluster(3))
#write.csv(closure_Cluster_Counts,"../SMT-pharmacy-report/reports\\closure_Cluster_Counts.csv", row.names = FALSE) 

##### Pharm chains clustering -----
pharm_chain<-Pharmacy_List_Postcodes%>%filter(`ParentOrgSize`!="Independants")%>%filter(!is.na(ParentOrgName))
chain<-sort(unique(pharm_chain$`ParentOrgName`))

pharm_chain_cluster<-function(name="17TH CENTURY HEALTH FOOD LTD"){
  data1<-pharm_chain%>%filter(ParentOrgName == name)
  data2<-data1
  temp1 <- purrr::map2_dfr(data1$Longitude, data1$Latitude,
                           ~spatialrisk::points_in_circle(data2, .x, .y, Longitude,
                                                          Latitude, radius = 804.672)[2,])
  temp2 <- purrr::map2_dfr(data1$Longitude, data1$Latitude,
                           ~spatialrisk::points_in_circle(data2, .x, .y, Longitude,
                                                          Latitude, radius = 804.672)[3,])
  temp3 <- purrr::map2_dfr(data1$Longitude, data1$Latitude,
                           ~spatialrisk::points_in_circle(data2, .x, .y, Longitude,
                                                          Latitude, radius = 804.672)[4,])
 
  temp1<-temp1%>%select(`1stPharm` =`FCode`, `1st_dist`=`distance_m`)%>%collect()
  temp2<-temp2%>%select(`2ndPharm` =`FCode`, `2nd_dist`=`distance_m`)%>%collect()
  temp3<-temp3%>%select(`3rdPharm` =`FCode`, `3rd_dist`=`distance_m`)%>%collect()
  data <-cbind(data1, temp1,temp2,temp3)
  temp4<- data%>%filter(FCode==`1stPharm`)
  temp5<- data%>%filter(!FCode %in% temp4$FCode)
  
  temp4<-temp4%>%
    left_join(temp5[c("FCode", "ParentOrgName","1stPharm","1st_dist")], "1stPharm")%>%
    mutate(`1stPharm`=`FCode.y`, `1st_dist`=`1st_dist.y`,`FCode`=`FCode.x`, `ParentOrgName`=`ParentOrgName.x`)%>%
    select(-`FCode.x`,-`FCode.y`,-`1st_dist.x`, -`1st_dist.y`,-`ParentOrgName.x`, -`ParentOrgName.y`)
  
  temp6<-temp4%>%filter(`1stPharm`==`2ndPharm`)
  
  data<-rbind(temp4, temp5)%>%
    filter( !(`1stPharm`==`2ndPharm` &`1st_dist`==`2nd_dist` & FCode %in% temp6$FCode))
  
  data
}

Chain_check<-pharm_chain_cluster(chain[[1]]) 

for(k in 2:length(chain)){
  temp<-pharm_chain_cluster(chain[[k]])
  
  Chain_check<-rbind(Chain_check, temp)
}

Chain_check<-Chain_check%>%
  mutate(`cluster` = ifelse(is.na(`1stPharm`), "No same chain pharmacy within 0.5 mile radius", "At least one pharmacy from the same chain nearby (within 0.5 mile)"))


chain_Cluster_Counts<-Chain_check%>%
  group_by(cluster)%>%
  summarise(n_pharmacies=n())%>%
  ungroup()%>%
  mutate(total_chain_pharmacies=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_chain_pharmacies)

chain_Cluster_Counts_summary <-chain_Cluster_Counts%>%
  pivot_wider(id_cols=total_chain_pharmacies,names_from=cluster,values_from=cluster_size_percentage)

chain_IMD_Cluster_Counts<-Chain_check %>%
  group_by(IMD_Decile,cluster)%>%
  summarise(n_pharmacies=n())%>%
  group_by(IMD_Decile)%>%
  mutate(total_chain_pharmacies_perIMD=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_chain_pharmacies_perIMD)

#write.csv(chain_IMD_Cluster_Counts,"../SMT-pharmacy-report/reports\\chain_IMD_Cluster_Counts.csv", row.names = FALSE) 

chain_IMD_Cluster_Counts_summary<-chain_IMD_Cluster_Counts%>%
  pivot_wider(id_cols=IMD_Decile ,names_from=cluster,values_from=cluster_size_percentage)



chain_OrgSize_Cluster_Counts<-Chain_check %>%
  group_by(ParentOrgSize,cluster)%>%
  summarise(n_pharmacies=n())%>%
  group_by(ParentOrgSize)%>%
  mutate(total_chain_pharmacies_perParentOrgSize=sum(n_pharmacies),cluster_size_percentage=n_pharmacies/total_chain_pharmacies_perParentOrgSize)

chain_OrgSize_Cluster_Counts_summary<-chain_OrgSize_Cluster_Counts%>%
  pivot_wider(id_cols=ParentOrgSize ,names_from=cluster,values_from=cluster_size_percentage)

### Plotting ----

plot_pharmCluster<- function(by="IMD", ifchain=TRUE){
  if(by=="IMD"){
    
    if(ifchain==TRUE){
      extra_txt=" from the same parent organisaton"
      data<-chain_IMD_Cluster_Counts
    } else {
      extra_txt=""
      data<-IMD_Cluster_Counts
      }
    
    title <- paste0("Clustering of pharmacy",extra_txt," (by IMD)")
    data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
    data<-data%>%mutate(x=IMD_Decile, y=`cluster_size_percentage`, fill=`cluster`)
    fill_label="Pharmacy cluster within 0.5 mile"
    xlab= "IMD_Decile (2019)"
    ylab="% of pharmacies per IMD_Decile"
  }
  else{
    if(ifchain==TRUE){
      extra_txt=" from the same parent organisaton"
      data<-chain_OrgSize_Cluster_Counts
    } else{
      extra_txt=""
      data<-OrgSize_Cluster_Counts
    }
    
    title <- paste0("Clustering of pharmacy",extra_txt," (by Parent Orgnisation Size)")
    data$`ParentOrgSize`<- factor(data$`ParentOrgSize`, levels = c("Independants", "Small Multiples(2-5)", "Medium Multiples(6-35)", "Large Multiples"))
    data<-data%>%mutate(x=ParentOrgSize, y=`cluster_size_percentage`, fill=`cluster`)
    fill_label="Pharmacy clustering within 0.5 mile"
    xlab= "Parent Organisation Size"
    ylab="% of pharmacies per ParentOrgSize"
  }
  
  
  p1<- ggplot(data, 
              aes(x = x, 
                  y = y, 
                  fill = str_wrap(fill, 30), label= paste0(round(y*100,1), "%"))) +
   geom_col() + geom_text(position = position_stack(reverse = FALSE,vjust = 0.5))+
    theme_bw()+ scale_fill_brewer(palette = "Pastel1") +
    scale_y_continuous( labels = percent_format()) +
    labs(title = str_wrap(title, 90),
         fill = fill_label, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"))+
    xlab(xlab)+
    ylab(ylab) 
  
  p1
}

plot_cluster_pie<-function(ifchain=TRUE){
  
  
  if(ifchain==TRUE){
    data<-chain_Cluster_Counts
    extra_txt=" clustered with another one from the same parent organisaton"
  } else {
   data<-Pharmacy_Cluster_Counts
   extra_txt=""
  } 
  
  data<-data%>%
    mutate(cluster_size_percentage=round(cluster_size_percentage*100,1))
  
  df<-data%>%
    mutate(end = 2 * pi * cumsum(cluster_size_percentage)/sum(cluster_size_percentage),
           start = lag(end, default = 0),
           middle = 0.5 * (start + end),
           hjust = ifelse(middle > pi, 1, 0),
           vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
  
  
  
  pie1<-ggplot(df) + 
    geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                     start = start, end = end, fill = cluster)) +
    geom_text(aes(x = 1 * sin(middle), y = 1 * cos(middle), label =paste0(paste(round(cluster_size_percentage, 1), "%")),
                  hjust = hjust, vjust = vjust)) +
    coord_fixed() +
    scale_fill_brewer(palette = "Pastel1") +
    scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                       name = "", breaks = NULL, labels = NULL) +
    scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                       name = "", breaks = NULL, labels = NULL)+
    labs(fill = NULL, x = NULL,y = NULL,caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"), 
         title = paste0("% of pharmacies", extra_txt))
  
  
  pie1
}
