library(openxlsx)
library(tidyverse)
library(DBI)
library(ggplot2)
library(ggpubr)
library(stringr)
library(magrittr)
library(dplyr)
library(scales)
library(grid)
library(gridExtra)
library(textclean)
library(directlabels)
#library(xlsx)
library(readxl)
library(GGally)
library(ggExtra)
library(ggalluvial)
library(plotly)
library(odbc)
library(formattable)
library(sf)
library(tmap)   
library(ggforce) # for 'geom_arc_bar'
library(data.table)
library(ggrepel)

#### set up UDAL connection for extracts from data marts
serv  <-"udalsqlmartprod.database.windows.net"
db  <- "udal-sql-mart-qualitydataanalysis"
user  <- "jin.tong@udal.nhs.uk" #####<---------- PLEASE REPLACE

con_udal <- dbConnect(
  drv = odbc(),
  driver = "ODBC Driver 17 for SQL Server",
  server = serv,
  database = db,
  UID = user,
  authentication = "ActiveDirectoryInteractive"
)

con = con_udal

#get list of all the files 
personal <- "jin.tong" #####<---------- PLEASE REPLACE


##### GP pop data ----
pull_pop<- function(){
  sql="SELECT *
  FROM [CommunityPharmacy_Public].[z_gp-reg-pat-prac-all-age-regions]
  where [EXTRACT_DATE] = 
  (select max([EXTRACT_DATE]) from [CommunityPharmacy_Public].[z_gp-reg-pat-prac-all-age-regions])"
  result<-dbSendQuery(con,sql)
  pop<-dbFetch(result)
  dbClearResult(result)
  
  pop
  
}
pop <- pull_pop()


reg_pop <-pop %>% 
  filter(ORG_TYPE== "Comm Region", SEX == "ALL")%>%
  select(EXTRACT_DATE,Region_code = ORG_CODE, pop=`NUMBER_OF_PATIENTS`)%>%
  collect()

national_pop<- reg_pop%>%
  group_by(EXTRACT_DATE)%>%
  summarise(pop = sum(pop))%>%
  collect()

stp_pop <-pop %>% 
  filter(ORG_TYPE== "ICB", SEX == "ALL")%>%
  select(EXTRACT_DATE,STP = ORG_CODE, pop=`NUMBER_OF_PATIENTS`)%>%
  collect()

##### Ref_contractor table ----

sql <- " select *   FROM [CommunityPharmacy_Restricted].[Ref_Contractor]"
result <- dbSendQuery(con,sql)
Ref_Contractor_full <- dbFetch(result)
dbClearResult(result)

sql <- " select *   FROM[CommunityPharmacy_Public].[Ref_PharmaceuticalList][Ref_PharmaceuticalList]"
result <- dbSendQuery(con,sql)
Ref_PharmList_full <- dbFetch(result)
dbClearResult(result)

#####ICB short name
icb_short<-read_excel(paste0("C:/Users/", personal,"/OneDrive - NHS/BSA routine published/Pharmacy Opening Closure Relocations/ICB_shortname.xlsx"))


##### Pharm lists Data processing ----

icb_short<-icb_short%>%rename(`STP Code`=ICB_Code)%>%mutate(ICB_print = paste0(`STP Code`, ": ", Short_ICB_Name))


Ref_Contractor_full<-Ref_Contractor_full%>%mutate(FCode = `ContractorCode`)

pharm_list<-Ref_PharmList_full%>%
  mutate(SnapshotMonth = as.Date(SnapshotMonth)) %>%
  mutate(SnapshotMonth = if_else(SnapshotMonth == as.Date("2022-10-01"), 
                                 as.Date("2022-09-01"), 
                                 SnapshotMonth),
         Year = substr(as.character(SnapshotMonth), 1,4), 
         ##pub = as.numeric(substr(as.character(SnapshotMonth),6 ,7)),
         pub = case_when(substr(as.character(SnapshotMonth),6 ,7)=="03"~"March",
                         substr(as.character(SnapshotMonth),6 ,7)=="06"~"June",
                         substr(as.character(SnapshotMonth),6 ,7)=="09"~"September",
                         substr(as.character(SnapshotMonth),6 ,7)=="12"~"December"),
         pub =factor(pub, levels = c("March","June","September","December")),
         Year = factor(Year, levels= c("2018","2019","2020","2021","2022","2023", "2024", "2025"))) %>%
  rename(FCode = `Pharmacy ODS Code (F-Code)`, 
         postcode = `Post Code`, 
         ICB_Name = `STP Name`, 
         HWB = `Health and Wellbeing Board`)%>%
  filter(`Contract Type` != "DAC")%>%
  left_join(Ref_Contractor_full, "FCode") %>%
  mutate(HWB=toupper(HWB))%>%
  mutate(Region_Name =ifelse(is.na(Region_Name), "Region Unknown", Region_Name))


#### change of ownership data 
coo<-read.xlsx(paste0("C:/Users/", personal,"/OneDrive - NHS/BSA routine published/Pharmacy Opening Closure Relocations/Pharmacy Opening Closure Relocations  July 2025.xlsx"), sheet= "Closures, COO, R ")
open<-read.xlsx(paste0("C:/Users/", personal,"/OneDrive - NHS/BSA routine published/Pharmacy Opening Closure Relocations/Pharmacy Opening Closure Relocations  July 2025.xlsx"), sheet = "openings")

coo<- coo %>% 
  #mutate(Month = floor_date(ymd(`Effective.Date`), 'month'))%>%
  mutate(Month= lubridate::floor_date(as.Date(as.numeric(`Effective.Date`), origin = "1899-12-30"), 'month'))%>%
  rename(FCode_old=`Fcode.(Old)`,`Fcode_change`=`Fcode.change?`, FCode =`New.Fcode.(if.applicable)`)%>% 
  select(Month, Type, FCode_old, Fcode_change, FCode, Consolidation)%>%
  filter(Month<=Sys.Date())%>%
  collect()
coo$FCode <-ifelse(coo$`Fcode_change`=="NO"|is.na(coo$`Fcode_change`), coo$FCode_old, coo$FCode)


coo<-coo%>%
  left_join(Ref_Contractor_full, "FCode") %>%   filter(ContractType != "DAC") %>%
  rename(`STP Code`=STP)%>%
  left_join(icb_short, "STP Code")%>%
  rename(STP=`STP Code`)%>%
  mutate(`STP`= ifelse(`STP`=="Q62","QRV",`STP`))%>%collect()

coo$`STP`<-ifelse(is.na(coo$Short_ICB_Name), paste0(coo$`STP`, " (Old STP Code)"), coo$`STP`)

open<-open %>% 
  #mutate(Month = floor_date(ymd(`Effective.Date.(Month)`), 'month'))%>%
  mutate(Month=  lubridate::floor_date(as.Date(`Effective.Date.(Month)`, origin = "1899-12-30"), 'month'))%>%
  rename(FCode=`OCS.Code`)%>%select(Month, FCode)%>%
  filter(Month<=Sys.Date())%>%
  left_join(Ref_Contractor_full, "FCode") %>%   filter(ContractType != "DAC")%>%
  rename(`STP Code`=STP)%>%
  left_join(icb_short, "STP Code")%>%
  rename(STP=`STP Code`)%>%collect()

open$`STP`<-ifelse(is.na(open$Short_ICB_Name), paste0(open$`STP`, " (Old STP Code)"), open$`STP`)

#pharm_list_wide<-pharm_list%>%
# select(FCode, SnapshotMonth)%>%
# mutate(active = 1)%>%
# arrange(SnapshotMonth)%>%
# pivot_wider(names_from = SnapshotMonth, values_from = active)

#library(data.table)
# pharm_list_long <- melt(setDT(pharm_list_wide), id.vars = c("FCode"), variable.name = "Pub_month")

#f<-colSums(pharm_list_wide == "NULL")

### Permanent closures 

coo_1<-coo%>% rename(FCode_new=FCode)%>%select(FCode_old, Fcode_change, FCode_new)%>%rename(FCode=FCode_old)%>%
  filter(Fcode_change=="YES")%>%distinct()

pharm_list_latest<-pharm_list%>%
  group_by(FCode)%>%
  summarise(SnapshotMonth=max(SnapshotMonth))%>%
  left_join(subset(pharm_list, select=c(FCode, HWB, `STP Code`, `postcode`, `Region_Name`, `IMD_Decile`,SnapshotMonth)), c("FCode","SnapshotMonth"))

pharm_active<-pharm_list%>%
  group_by(FCode)%>%
  summarise(from=min(SnapshotMonth), until=max(SnapshotMonth))%>%
  mutate(closed = until %m+% months(3))%>%
  left_join(pharm_list_latest,"FCode")%>%
  mutate(closed=as.POSIXlt(as.Date(ifelse(closed<=max(until), closed, NA),origin = "1970-01-01"), format = '%d%b%Y'))%>%
  mutate(closed=as.Date(closed, "%Y-%m-%d"))%>%
  left_join(icb_short, "STP Code")%>%
  distinct()%>%
  left_join(coo_1,"FCode")%>%
  mutate(closed = as.Date(as.POSIXlt(as.Date(ifelse(is.na(FCode_new), closed, NA),origin = "1970-01-01"), format = '%d%b%Y'), "%Y-%m-%d"))%>%
  mutate(Region_Name =ifelse(is.na(Region_Name), "Region Unknown", Region_Name))





min_date<-min(pharm_list$SnapshotMonth) %m-% months(2)
max_date<-max(pharm_list$SnapshotMonth) %m+% months(2)
pharm_active$`STP Code`<-ifelse(is.na(pharm_active$Short_ICB_Name), paste0(pharm_active$`STP Code`, " (Old STP Code)"), pharm_active$`STP Code`)

pharm_list<-pharm_list%>% 
  #mutate(`STP Code`= ifelse(`STP Code`=="Q62","QRV",`STP Code`))%>%
  left_join(icb_short, "STP Code")%>% 
  mutate(`STP Code`= ifelse(is.na(Short_ICB_Name), paste0(`STP Code`, " (Old STP Code)"), `STP Code`))%>%
  mutate(Region_Name =ifelse(is.na(Region_Name), "Region Unknown", Region_Name))


latest_contractor<-Ref_Contractor_full%>%
  filter(is.na(Inactive),`ContractorType` == "Pharmacy")%>%
  select(`ContractorCode`
         ,`ContractorName`
         ,`ParentOrgName`=`ParentOrgName_Clean`
         ,`ParentOrgSize` 
         ,`STP`
         ,`RegionCode`
         ,`Region_Name`
         ,`ClusterSize`
         ,`ContractType`
         ,`IMD_Decile`
         ,`LSOA`
         ,`LSOA_Name`)%>%
  mutate(ParentOrgName_large = case_when( ParentOrgName=="ASDA STORES LTD" ~ "ASDA",
                                          ParentOrgName=="AVICENNA RETAIL LTD" ~ "AVICENNA",
                                          ParentOrgName=="BESTWAY NATIONAL CHEMISTS LIMITED" ~ "BESTWAY",
                                          ParentOrgName=="BOOTS UK LIMITED" ~ "BOOTS",
                                          ParentOrgName=="DAY LEWIS PLC" ~ "DAY LEWIS",
                                          ParentOrgName=="DUDLEY TAYLOR PHARMACIES LTD" ~ "DUDLEY TAYLOR",
                                          ParentOrgName=="GORGEMEAD LIMITED" ~ "GORGEMEAD",
                                          ParentOrgName=="GORGEMEAD LTD" ~ "GORGEMEAD",
                                          ParentOrgName=="HI WELDRICK LIMITED" ~ "HI WELDRICK",
                                          ParentOrgName=="JARDINES (U.K.) LIMITED" ~ "JARDINE",
                                          ParentOrgName=="JARDINE'S (UK) LTD" ~ "JARDINE",
                                          ParentOrgName=="JOHN BELL & CROYDEN LIMITED" ~ "JOHN BELL & CROYDEN",
                                          ParentOrgName=="L ROWLAND & CO (RETAIL) LTD" ~ "L ROWLAND & CO",
                                          ParentOrgName=="LINCOLN CO-OP CHEMIST LTD" ~ "LINCOLN",
                                          ParentOrgName=="LINCOLN CO-OP CHEMISTS LTD" ~ "LINCOLN",
                                          ParentOrgName=="LLOYDS PHARMACY LTD" ~ "LLOYDS",
                                          ParentOrgName=="NORCHEM HEALTHCARE LIMITED" ~ "NORCHEM",
                                          ParentOrgName=="NORCHEM HEALTHCARE LTD" ~ "NORCHEM",
                                          ParentOrgName=="PAYDENS (STEYNING) LTD" ~ "PAYDENS",
                                          ParentOrgName=="PAYDENS LIMITED" ~ "PAYDENS",
                                          ParentOrgName=="PAYDENS LTD" ~ "PAYDENS",
                                          ParentOrgName=="PAYDENS LTD (G.CURRIES)" ~ "PAYDENS",
                                          ParentOrgName=="PAYDENS PHARMACY LTD" ~ "PAYDENS",
                                          ParentOrgName=="PCT HEALTHCARE" ~ "PCT HEALTHCARE",
                                          ParentOrgName=="PCT HEALTHCARE LIMITED" ~ "PCT HEALTHCARE",
                                          ParentOrgName=="PCT HEALTHCARE LTD" ~ "PCT HEALTHCARE",
                                          ParentOrgName=="SUPERDRUG STORES PLC" ~ "SUPERDRUG",
                                          ParentOrgName=="TESCO PLC" ~ "TESCO",
                                          ParentOrgName=="TESCO STORES LIMITED" ~ "TESCO",
                                          ParentOrgName=="WAREMOSS LIMITED" ~ "WAREMOSS",
                                          ParentOrgName=="WM MORRISON SUPERMARKETS LIMITED" ~ "WM MORRISON",
                                          TRUE ~ "Independants & Small or Medium Multiples"))%>%
  rename(FCode= `ContractorCode`)%>%collect()

ICB_premise_hours<- pharm_list%>%
  group_by(SnapshotMonth, Year, pub, Region_Name, `STP`= `STP Code`, ICB_print)%>%
  summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
  left_join(stp_pop, "STP")%>%
  mutate(prem_pop = openPremises/pop*100000, hour_pop = WeeklyOpenHours/pop*1000000,ICB=`STP`)%>%
  collect()

##### plotting graphs ----

##### open premises and opening hours ----

plot_national_1 <- function(){
  title1 <- paste(  "National total registered open premises (Number of pharmacies including LPS contractors and excluding DAC contractors)")
  title2 <- paste(  "National total registered opening hours per week")
  
  data<-pharm_list %>%
    group_by(SnapshotMonth, Year, pub)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
    collect()
  
  #write.csv(data,"../SMT-pharmacy-report/reports\\premises&hours_national.csv", row.names = FALSE) 
  p1<- ggplot(data, aes(x = `pub` , y = `openPremises`,group = Year
                        , color = `Year` )) +
    geom_line()+
    geom_point()+
    theme_bw() +
    theme(legend.direction ="vertical",legend.position = "right")+
    #theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    theme(axis.text.x=element_blank())+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::breaks_pretty())+
    geom_dl(aes(label = Year), method = list(dl.trans(x = x + 0.2),
                                             "last.points", cex = 0.8,fontface='bold'))+
    labs(title = str_wrap(title1, 140),
         y = "Open Premises \nNo. of pharmacies")
  
  p2<- ggplot(data, aes(x = `pub` , y = `WeeklyOpenHours`,group= Year
                        , color = `Year` )) +
    geom_line()+
    geom_point()+
    theme_bw() +
    theme(legend.direction ="vertical",legend.position = "right")+
    geom_dl(aes(label = Year), method = list(dl.trans(x = x + 0.2),
                                             "last.points", cex = 0.8,fontface='bold'))+
    # theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks()) +
    labs(title = str_wrap(title2, 120), y = "Total registered opening\n hours per week", x ="Pharmaceutical List Publication Month")
  
  #arrange two graphs on the same page, align x axis
  ggarrange(p1+xlab(NULL),p2,
            ncol = 1, nrow = 2,align = "v",
            common.legend = T, legend = "right")
  
} 

plot_national_2 <- function(){
  title1 <- paste(  "National total registered open premises (Number of pharmacies including LPS contractors and excluding DAC contractors)")
  title2 <- paste(  "National total registered opening hours per week")
  
  subtitle1<-"* Labels in red are changes in national total number of open pharmacies compared to previous publication of Consolidated Pharmaceutical List"
  subtitle2<-"** Labels in blue are changes in national total registered hours compared to previous publication of Consolidated Pharmaceutical List"
  
  data<-pharm_list %>%
    group_by(SnapshotMonth, Year, pub)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))
  
  DT1<-data%>%ungroup()%>%select(SnapshotMonth, openPremises)
  DT1<-DT1%>%mutate(Diff_prem=c(0,diff(DT1$openPremises)))
  
  
  DT2<-data%>%ungroup()%>%select(SnapshotMonth, WeeklyOpenHours )
  DT2<-DT2%>% mutate(Diff_hour=c(0,diff(DT2$WeeklyOpenHours)))%>%
    right_join(DT1, "SnapshotMonth")%>%
    select(`SnapshotMonth`,`Diff_prem`,`Diff_hour`)
  
  data<-data%>%
    left_join(DT2, "SnapshotMonth")
  
  p1<- ggplot(data, aes(x = `SnapshotMonth` , y = `openPremises`)) +
    geom_line()+
    geom_point()+
    geom_text(data=subset(data,`SnapshotMonth`>min(data$SnapshotMonth)), aes(y= `openPremises` , label = Diff_prem), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="red")+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    #theme_minimal()+ #this produces graph with no boarder line
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(min(data$`openPremises`)-100, max(data$`openPremises`)+100)) +
    labs(title = str_wrap(title1, 140),subtitle = subtitle1, y = "Open Premises \nNo. of pharmacies")+
    theme(plot.subtitle=element_text(size=11, hjust=0.5, face="italic", color="red"))
  
  p2<- ggplot(data, aes(x = `SnapshotMonth` , y = `WeeklyOpenHours`)) +
    geom_line()+
    geom_point()+
    theme_bw() +
    geom_text(data=subset(data,`SnapshotMonth`>min(data$SnapshotMonth)), aes(y= `WeeklyOpenHours` , label = round(Diff_hour,0)), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="blue")+
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(),limits = c(min(data$`WeeklyOpenHours`)-50000, max(data$`WeeklyOpenHours`)+50000)) +
    labs(title = str_wrap(title2, 140), subtitle = subtitle2, y = "Total registered opening\n hours per week", x ="Pharmaceutical List Publication Month")+
    theme(plot.subtitle=element_text(size=11, hjust=0.5, face="italic", color="blue"))
  
  #arrange two graphs on the same page, align x axis
  ggarrange(p1+xlab(NULL),p2,
            ncol = 1, nrow = 2,align = "v")
  
} 

plot_regional <- function(plot="prem",pop="Y"){
  data<-pharm_list %>%
    group_by(SnapshotMonth,`Region_code`=`RegionCode`, Region_Name)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
    filter(!is.na(Region_Name))%>%
    left_join(reg_pop, "Region_code")%>%
    mutate(prem_pop = openPremises/pop*100000, hour_pop = `WeeklyOpenHours`/pop*1000000)%>%
    collect()

  #write.csv(data,"../SMT-pharmacy-report/reports\\premises&hours_region.csv", row.names = FALSE)
  if(plot=="prem"& pop =="N"){
  title <- paste(  "Total registered open premises (Number of pharmacies including LPS contractors and excluding DAC contractors) at regional level")
  y="Open Premises \nNo. of pharmacies"
  data<-data%>%rename(y_var = `openPremises`)%>%collect()
  }
  else if(plot== "hour" & pop =="N")
  {  title <- paste(  "Total registered opening hours per week at regional level")
    y = "Total registered opening\n hours per week"
    data<-data%>%rename(y_var = `WeeklyOpenHours`)%>%collect()
  }else if(plot== "prem"& pop == "Y")
  { 
    title <- paste(  "Total registered open premises per 100k population (Number of pharmacies including LPS contractors and excluding DAC contractors)")
  y="Open Premises \nNo. of pharmacies"
  data<-data%>%rename(y_var = `prem_pop`)%>%filter(!is.na(y_var))%>%collect()
 }
  else if(plot== "hour" & pop =="Y"){
    title <- paste(  "Total registered opening hours per week per 100k population")
    y = "Total registered opening\n hours per week"
    data<-data%>%rename(y_var = `hour_pop`)%>%filter(!is.na(y_var))%>%collect()
  }
   
  data$`Region_Name`<- factor(data$`Region_Name`, levels = c("East Of England", "London", "Midlands","North East And Yorkshire", "North West","South East", "South West", "Region Unknown"))
  
  p1<- ggplot(data, aes(x = `SnapshotMonth` , y = y_var, color=`Region_Name`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme_bw() +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(min(data$`y_var`)-10, max(data$`y_var`)+10)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
    labs(title = str_wrap(title, 140), y = y)
  p1
} 


plot_IMD <- function(){
  title1 <- paste(  "Total registered open premises (Number of pharmacies including LPS contractors and excluding DAC contractors) breakdown by IMD (2019)")
  title2 <- paste(  "Total registered opening hours per week breakdown by IMD Decile (IMD2019)")
  
  data<-pharm_list %>%
    group_by(SnapshotMonth, IMD_Decile)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
    filter(!is.na(IMD_Decile))
  
  data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
  #write.csv(data,"../SMT-pharmacy-report/reports\\premises&hoursbyIMD.csv", row.names = FALSE)
  p1<- ggplot(data, aes(x = `SnapshotMonth` , y = `openPremises`, group=`IMD_Decile`,color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(min(data$`openPremises`)-100, max(data$`openPremises`)+100)) +
    labs(title = str_wrap(title1, 120), y = "Open Premises \nNo. of pharmacies")
  
  p2<- ggplot(data, aes(x = `SnapshotMonth` , y = `WeeklyOpenHours`, group=`IMD_Decile`,color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(),limits = c(min(data$`WeeklyOpenHours`)-500, max(data$`WeeklyOpenHours`)+500)) +
    labs(title = str_wrap(title2, 120), y = "Total registered opening\n hours per week", x ="Pharmaceutical List Publication Month")
  
  #arrange two graphs on the same page, align x axis
  ggarrange(p1+xlab(NULL),p2,
            ncol = 1, nrow = 2,align = "v",
            common.legend = T, legend = "right")
  
} 

plot_hwb_prem <- function(region= "London"){
  title1 <- paste0(  "Total registered open premises at Health and Wellbeing Board level in ",region)
  
  data<- pharm_list%>%
    group_by(SnapshotMonth, Year, pub, Region_Name, STP, HWB)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
    filter(Region_Name==region, `SnapshotMonth`>"2022-03-01")%>%
    collect()
  
  min_date<-min(data$SnapshotMonth)-60
  max_date<-max(data$SnapshotMonth)+60
  
  p1<- ggplot(data, aes(x = `SnapshotMonth` , y = `openPremises`, group=HWB, color=HWB)) +
    geom_line()+
    geom_point()+
    theme_bw() +
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks()) +
    labs(title = str_wrap(title1, 90), y = "Open Premises \nNo. of pharmacies")+
    geom_dl(aes(label = HWB), method = list(dl.trans(x = x + 0.2),
                                            "last.points", cex = 0.5,fontface='bold'))+
    facet_wrap(~STP, ncol = 3, scale="free_x")+xlab("")+ theme(legend.position = "none")
  
  p1
  
} 

plot_hwb_hours <- function(region= "London"){
  title2 <- paste(  "Total registered opening hours per week at Health and Wellbeing Board level in ",region)
  
  data<- pharm_list%>%
    group_by(SnapshotMonth, Year, pub, Region_Name, STP, HWB)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
    filter(Region_Name==region, `SnapshotMonth`>"2022-03-01")%>%
    collect()
  
  min_date<-min(data$SnapshotMonth)-60
  max_date<-max(data$SnapshotMonth)+60
  
  
  p2<- ggplot(data, aes(x = `SnapshotMonth` , y = `WeeklyOpenHours`,group=HWB, color=HWB)) +
    geom_line()+
    geom_point()+
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks()) +
    labs(title = str_wrap(title2, 120), y = "Total registered opening\n hours per week", x ="Pharmaceutical List Publication Month")+
    geom_dl(aes(label = HWB), method = list(dl.trans(x = x + 0.2),
                                            "last.points", cex = 0.5,fontface='bold'))+
    facet_wrap(~STP, ncol = 3, scale="free_x")+xlab("")+ theme(legend.position = "none")
  
  p2
  
  
} 

plot_ICB <- function(region= "London",plot = "prem", pop="N"){

  data<- ICB_premise_hours%>%
    filter(Region_Name==region,`ICB`!="0", !is.na(`ICB`))
  
  min_date<-min(data$SnapshotMonth)-30
  max_date<-max(data$SnapshotMonth)+30
  
  if(plot=="prem" & pop=="N"){
    title = paste0("Total registered open premises at ICB level in ", region)
    y = "Open Premises \nNo. of pharmacies"
    data<-data%>%rename(y_var = `openPremises`)%>%collect()
    data$ICB_print<-ifelse(is.na(data$ICB_print), data$ICB, data$ICB_print)
  } else if(plot=="hour" & pop=="N"){
    title = paste0("Total registered opening hours per week at ICB level in ", region)
    y = "Total registered opening\n hours per week"
    data<-data%>%rename(y_var = `WeeklyOpenHours`)%>%collect()
    data$ICB_print<-ifelse(is.na(data$ICB_print), data$ICB, data$ICB_print)
  } else if(plot=="prem" & pop=="Y"){
    title = paste0("Total registered open premises per 100k population at ICB level in ", region)
    y = "Open Premises \nNo. of pharmacies"
    data<-data%>%rename(y_var = `prem_pop`)%>%filter(!is.na(ICB_print))%>%collect()
  }else if(plot=="hour" & pop=="Y"){
    title = paste0("Total registered opening hours per week per 100k population at ICB level in ", region)
    y = "Total registered opening\n hours per week"
    data<-data%>%rename(y_var = `hour_pop`)%>%filter(!is.na(ICB_print))%>%collect()
  }
  
  p1<- ggplot(data, aes(x = `SnapshotMonth` , y = y_var,color=`ICB_print`)) +
    geom_line()+
    geom_point()+
    theme_bw() +
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5) )+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks()) +
    facet_wrap(~`ICB_print`, ncol = 3
               #, scale="free_x"
    )+xlab("")+
     labs(title = str_wrap(title, 120), y = y, x ="Pharmaceutical List Publication Month")
 # +geom_dl(aes(label = `STP Code`), method = list(dl.trans(x = x + 0.2),
                                                 #  "last.points", cex = 0.8,fontface='bold'))
  
  p1
  
} 

plot_ICB_hour <- function(region= "London"){
  title1 <- paste0(  "Total registered opening hours per week at ICB level in ", region)
  
  data<- ICB_premise_hours%>%
    filter(Region_Name==region,`SnapshotMonth`>"2021-03-01",`ICB`!="0", !is.na(`ICB`))
  
  min_date<-min(data$SnapshotMonth)-60
  max_date<-max(data$SnapshotMonth)+60
  
  p1<- ggplot(data, aes(x = `SnapshotMonth` , y = `WeeklyOpenHours`, color=`ICB`)) +
    geom_line()+
    geom_point()+
    theme_bw() +
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks()) +
    facet_wrap(~`ICB`, ncol = 3
               #, scale="free_x"
    )+xlab("")+
    labs(title = str_wrap(title1, 90), y =  "Total registered opening\n hours per week", x ="Pharmaceutical List Publication Month")
  #+geom_dl(aes(label = `STP Code`), method = list(dl.trans(x = x + 0.2),
                                                  # "last.points", cex = 0.8,fontface='bold'))
  
  p1
  
} 

##### permanent closures ----

plot_closure_national<-function(){
  data<-pharm_active%>%
    group_by(closed)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed))%>%
    mutate(National = "National")
  
  #write.csv(data,"../SMT-pharmacy-report/reports\\PermClosures_national.csv", row.names = FALSE) 
  
  title1 <- paste(  "Total number of permanent closures at national level (per published pharm list)")
  
  p1<- ggplot() +
    geom_line(data, mapping = aes(x = `closed` , y = `noClosure`,size = National ), colour = "black") +
    geom_point()+
    ggrepel::geom_label_repel(data = data,
                              mapping = aes(x = `closed` , y = `noClosure`,
                                            label = `noClosure`),
                              size = 3.5,
                              label.size = NA,
                              box.padding = unit(0.5, "lines")) +
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y")+ 
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`noClosure`)+100)) +
    labs(title = str_wrap(title1, 90), y = "No. of pharmacies permanently closed", x="Pharmaceutical List Publication Month")
  
  p1
}

plot_closure_IMD<-function(){
  
  data<-pharm_active%>%
    group_by(closed, IMD_Decile)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed))
  
  title1 <- paste(  "Total number of permanent closures breakdown by IMD Decile (IMD2019)")
  
  
  data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
  #write.csv(data,"../SMT-pharmacy-report/reports\\PermClosures_IMD.csv", row.names = FALSE) 
  
  p1<- ggplot(data, aes(x = `closed` , y = `noClosure`, color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme_bw() +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`noClosure`)+10)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~IMD_Decile, ncol = 5)+xlab("")+ 
    labs(title = str_wrap(title1, 90), y = "No. of pharmacies permanently closed", x="Pharmaceutical List Publication Month")
  
  p1
}

plot_closure_regional_all<-function(rate = "Y"){
 
  total<-pharm_list %>%
    group_by(until = SnapshotMonth, Region_Name)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(Region_Name))
  
  data2<-pharm_active%>%
    group_by(closed, Region_Name)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed), !is.na(Region_Name))%>%
    mutate(until= closed%m-% months(3))%>%
    left_join(total, c("until", "Region_Name"))%>%
    mutate(closure_rate = noClosure/total*100)
  
  data2$`Region_Name`<- factor(data2$`Region_Name`, levels = c("East Of England", "London", "Midlands","North East And Yorkshire", "North West","South East", "South West", "Region Unknown"))
  
  if(rate=="N"){
  title1 <- paste(  "Total number of permanent closures at regional level (per published pharm list)")
  
  p1<- ggplot() +
    geom_line(data2, mapping =aes(x = `closed` , y = `noClosure`, colour = `Region_Name`)) +
    geom_point()+
    geom_text(data=data2, aes(x = `closed` ,y= `noClosure` , label = noClosure), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
   # ggrepel::geom_label_repel(data = data2,
                            #  mapping = aes(x = `closed` , y = `noClosure`,
                                        #    label = `noClosure`),
                            #  size = 3.5,
                             # label.size = NA,
                             # box.padding = unit(0.5, "lines")) +
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data2$`noClosure`)+10)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
     labs(title = str_wrap(title1, 120), y = "No. of pharmacies permanently closed", x="Pharmaceutical List Publication Month")
  }
  else if(rate == "Y")
  {
    title1 <- paste(  "Permanent closure rates (%) at regional level")
    subtitle <- paste("Permanent closure rate (%) = total number of Permanent closures / Total number of open premises in the previous quarter")
    data2<- subset(data2, Region_Name!="Region Unknown")
    
    p1<- ggplot() +
      geom_line(data2, mapping =aes(x = `closed` , y = `closure_rate`, colour = `Region_Name`)) +
      geom_point()+
      #geom_text(data=data2, aes(x = `closed` ,y= `closure_rate` , label = paste0(round(`closure_rate`,0), "%")), 
                #position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
      scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
      theme_bw() +
      scale_size_manual(values = 1) +
      scale_y_continuous(label = scales::comma,
                         breaks = scales::pretty_breaks(), limits = c(0, max(data2$`closure_rate`)+0.5)) +
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
      facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
      labs(title = str_wrap(title1, 120),subtitle = subtitle, y = "Percentage of pharmacies permanently closed", x="Pharmaceutical List Publication Month")
  }
  p1
}

plot_closure_regional<-function(region="London", rate = "N"){
  total<-pharm_list %>%
    group_by(until = SnapshotMonth, Region_Name)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(Region_Name))
  
  total2<-pharm_list %>%
    group_by(until = SnapshotMonth, STP)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(STP))
  
  data<-pharm_active%>%
    filter(Region_Name== region)%>%
    group_by(closed, Region_Name)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed), !is.na(Region_Name))%>%
    mutate(until= closed%m-% months(3))%>%
    left_join(total, c("until", "Region_Name"))%>%
    mutate(closure_rate = noClosure/total*100)
  
  data2<-pharm_active%>%
    filter(Region_Name== region)%>%
    group_by(closed,`STP`= `STP Code`, `ICB_print`)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed), !is.na(`STP`), `STP` != "NA (Old STP Code)")%>%
    mutate(until= closed%m-% months(3))%>%
    left_join(total2, c("until", "STP"))%>%
    mutate(closure_rate = noClosure/total*100)%>%
    mutate(ICB=`STP`)%>%collect()
  
  if(rate == "N")
  { title1 <- paste0(  "Total number of permanent closures at ICB level in ", region)
  
  data2$ICB_print<-ifelse(is.na(data2$ICB_print), data2$ICB, data2$ICB_print)
  
  p1<- ggplot() +
   # geom_line(data, mapping = aes(x = `closed` , y = `noClosure`,size = Region_Name ), colour = "black") +
    geom_line(data2, mapping =aes(x = `closed` , y = `noClosure`, colour = `ICB_print`), linewidth=0.9) +
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    geom_text(data=data2, aes(x = `closed` ,y= `noClosure` , label = noClosure), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
    # ggrepel::geom_label_repel(data = data2,
    #  mapping = aes(x = `closed` , y = `noClosure`,
    #    label = `noClosure`),
    #  size = 3.5,
    # label.size = NA,
    # box.padding = unit(0.5, "lines")) +
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data2$`noClosure`)+2)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~ICB_print, ncol = 4)+xlab("")+ 
    labs(title = str_wrap(title1, 90), y = "No. of pharmacies permanently closed", x="Pharmaceutical List Publication Month")
  }
  else if(rate == "Y")
  {
    data2<-data2%>%filter(!is.na(`closure_rate`), !is.na(ICB_print))
    
    title1 <- paste0(  "Permanent closure rates (%) at ICB level in ", region)
    subtitle <- paste("Permanent closure rate (%) = total number of Permanent closures / Total number of open premises in the previous quarter")
    
    p1<- ggplot() +
    #  geom_line(data, mapping = aes(x = `closed` , y = `closure_rate`,size = Region_Name ), colour = "black") +
      geom_line(data2, mapping =aes(x = `closed` , y = `closure_rate`, colour = `ICB_print`), linewidth=0.9) +
      geom_point()+
      scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
      theme_bw() +

      #geom_text(data=data2, aes(x = `closed` ,y= `closure_rate` , label = paste0(round(`closure_rate`,1), "%")),  
               #position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
      # ggrepel::geom_label_repel(data = data2,
      #  mapping = aes(x = `closed` , y = `noClosure`,
      #    label = `noClosure`),
      #  size = 3.5,
      # label.size = NA,
      # box.padding = unit(0.5, "lines")) +
      scale_size_manual(values = 1) +
      scale_y_continuous(label = scales::comma,
                         breaks = scales::pretty_breaks(), limits = c(0, max(data2$`closure_rate`)+2)) +
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
      facet_wrap(~ICB_print, ncol = 4)+xlab("")+ 
      labs(title = str_wrap(title1, 90),subtitle = subtitle, y = "Percentage of pharmacies permanently closed", x="Pharmaceutical List Publication Month")
    }
  p1
}

plot_closure_ICB<-function(region="London"){
  data<-pharm_active%>%
    filter(Region_Name== region)%>%
    group_by(closed, `STP Code`)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed), !is.na(`STP Code`))
  
  data2<-pharm_active%>%
    filter(Region_Name== region)%>%
    group_by(closed, `STP Code`, HWB)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed), !is.na(`STP Code`))
  
  title1 <- paste0(  "Total number of permanent closures at HWB level in ", region)
  
  p1<- ggplot() +
    geom_line(data, mapping = aes(x = `closed` , y = `noClosure`, group = `STP Code`, colour = `STP Code`), linewidth=2) +
    geom_line(data2, mapping =aes(x = `closed` , y = `noClosure`, group = `HWB`, colour = `HWB`), linewidth=0.5) +
    geom_point()+
    ggrepel::geom_label_repel(data = data,
                              mapping = aes(x = `closed` , y = `noClosure`,
                                            label = `noClosure`),
                              size = 3.5,
                              label.size = NA,
                              box.padding = unit(0.5, "lines")) +
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y")+ 
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`noClosure`)+10)) +
    labs(title = str_wrap(title1, 90), y = "No. of pharmacies permanently closed", x="Pharmaceutical List Publication Month")+
    facet_wrap(~`STP Code`, ncol = 3
               #, scale="free_x"
    )+xlab("")+ theme(legend.position = "none")
  
  p1
}

#####change of ownership -----

plot_coo_national<-function(){
  data<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    group_by(Month)%>%
    summarise(no_coo=n_distinct(FCode))%>%
    filter(no_coo>0)
  
  #write.csv(data,"../SMT-pharmacy-report/reports\\COO_national.csv", row.names = FALSE) 
  
  title1 <- paste(  "Total number of change of ownerships at national level since 2019")
  
  p1<- ggplot(data, aes(x = `Month` , y = `no_coo`)) +
    geom_line()+
    geom_point()+
    geom_text(data=data, aes(y= `no_coo` , label = no_coo), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="red")+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y")+ 
    #theme_minimal()+ #this produces graph with no boarder line
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`no_coo`)+10)) +
    labs(title = str_wrap(title1, 90), y = "No. of pharmacies which changed ownership", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  
  p1
}

plot_coo_IMD<-function(){
  
  data<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    group_by(Month, IMD_Decile)%>%
    summarise(no_coo=n_distinct(FCode))
  #write.csv(data,"../SMT-pharmacy-report/reports\\COO_IMD.csv", row.names = FALSE) 
  title1 <- paste(  "Total number of changed ownerships breakdown by IMD Decile (IMD2019)")
  
  
  data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
  
  p1<- ggplot(data, aes(x = `Month` , y = `no_coo`, group=`IMD_Decile`,color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`no_coo`)+1)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~IMD_Decile, ncol = 5)+xlab("")+ 
     labs(title = str_wrap(title1, 90), y = "No. of pharmacies which changed ownership", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  
  p1
}

plot_coo_regional_all<-function(rate = "N"){
  total<-pharm_list %>%
    group_by(until = SnapshotMonth, Region_Name)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(Region_Name))
  
   data2<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    group_by(Month, Region_Name)%>%
    summarise(no_coo=n_distinct(FCode))%>%
    filter(!is.na(Region_Name),no_coo>0, !is.na(no_coo))%>%
     mutate(until= Month%m-% months(3))%>%
     left_join(total, c("until", "Region_Name"))%>%
     mutate(coo_rate = no_coo/total*1000)
  
   if(rate == "N"){
  title2 <- paste(  "Total number of change of ownerships at regional level  since 2019")
  
  data2$`Region_Name`<- factor(data2$`Region_Name`, levels = c("East Of England", "London", "Midlands","North East And Yorkshire", "North West","South East", "South West", "Region Unknown"))
  
  p2<- ggplot() +
    geom_line(data2, mapping =aes(x = `Month` , y = `no_coo`, colour = `Region_Name`)) +
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
   # geom_text(data=data2, aes(x = `Month` , y = `no_coo` , label = `no_coo`), 
       #       position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5), legend.position = "right"
          ,legend.title = element_blank ())+
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data2$`no_coo`)+2)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
    labs(title = str_wrap(title2, 120), y = "No. of pharmacies which changed ownership", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
   }
   else if(rate == "Y")
   {
     title2 <- paste(  "Change of ownerships per 1000 pharmacies at regional level  since 2019")
     subtitle <- paste("No. of COO per 1000 pharmacies = total number of changes of ownership / Total number of open premises in the previous quarter * 1000")
     
     data2$`Region_Name`<- factor(data2$`Region_Name`, levels = c("East Of England", "London", "Midlands","North East And Yorkshire", "North West","South East", "South West", "Region Unknown"))
     
     p2<- ggplot() +
       scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
       geom_line(data2, mapping =aes(x = `Month` , y = `no_coo`, colour = `Region_Name`), linewidth=0.9) +
       geom_point()+
       #geom_text(data=data2, aes(x = `Month` , y = `no_coo` , label = round(`coo_rate`,0)), 
             #position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
       theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5), legend.position = "right"
             ,legend.title = element_blank ())+
       scale_size_manual(values = 1) +
       scale_y_continuous(label = scales::comma,
                          breaks = scales::pretty_breaks(), limits = c(0, max(data2$`coo_rate`)+2)) +
       theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
       facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
       labs(title = str_wrap(title2, 120),subtitle = subtitle,  y = "No. of COO per 1000 pharmacies", x="Effective Month",
            caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
   }
  p2
}

plot_coo_regional<-function(region="London", rate = "N"){
  total2<-pharm_list %>%
    group_by(until = SnapshotMonth, STP)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(STP))
  
   data<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    filter(Region_Name== region)%>%
    group_by(Month, Region_Name)%>%
    summarise(no_coo=n_distinct(FCode))
  
  data2<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    filter(Region_Name== region)%>%
    group_by(Month, `STP`,`ICB_print`)%>%
    summarise(no_coo=n_distinct(FCode))%>%
    filter(!is.na(`STP`), `STP` != "NA (Old STP Code)", !is.na(no_coo))%>%
    mutate(until= Month%m-% months(3))%>%
    left_join(total2, c("until", "STP"))%>%
    mutate(coo_rate = no_coo/total*100)%>%
    mutate(ICB=`STP`)%>%collect()
  
  if(rate == "N"){
  title1 <- paste0(  "Total number of changed ownerships at ICB level in ", region)
  data2$ICB_print<-ifelse(is.na(data2$ICB_print), data2$ICB, data2$ICB_print)
  
  p1<- ggplot() +
    #geom_line(data, mapping = aes(x = `Month` , y = `no_coo`,size = Region_Name ), colour = "black") +
    geom_line(data2, mapping =aes(x = `Month` , y = `no_coo`, colour = `ICB_print`), linewidth=0.9) +
    geom_point()+
   # geom_text(data=data2, aes(x = `Month` , y = `no_coo` , label = `no_coo`), 
         #     position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data2$`no_coo`)+1)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~ICB, ncol = 4)+xlab("")+ 
     labs(title = str_wrap(title1, 90), y = "No. of pharmacies which changed ownership", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  }
  else if(rate == "Y"){
   data2<-data2%>%filter(!is.na(coo_rate), !is.na(ICB_print))
   
     title1 <- paste0(  "Total number of changed ownership per 1000 pharmacies at ICB level in ", region)
    subtitle <- paste("No. of COO per 1000 pharmacies = total number of changes of ownership / Total number of open premises in the previous quarter * 1000")
    
    p1<- ggplot() +
      #geom_line(data, mapping = aes(x = `Month` , y = `no_coo`,size = Region_Name ), colour = "black") +
      geom_line(data2, mapping =aes(x = `Month` , y = `coo_rate`, colour = `ICB_print`), linewidth=0.9) +
      geom_point()+
      # geom_text(data=data2, aes(x = `Month` , y = `no_coo` , label = `no_coo`), 
      #     position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
      scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
      theme_bw() +
      scale_size_manual(values = 1) +
      scale_y_continuous(label = scales::comma,
                         breaks = scales::pretty_breaks(), limits = c(0, max(data2$`coo_rate`)+1)) +
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
      facet_wrap(~ICB_print, ncol = 4)+xlab("")+ 
      labs(title = str_wrap(title1, 90), subtitle=subtitle,  y = "No. of COO per 1000 pharmacies", x="Effective Month",
           caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  }
  p1
}

#####New openings-----

plot_open_national<-function(){
  data<-open%>%
    group_by(Month)%>%
    summarise(no_open=n_distinct(FCode))
  
  title1 <- paste(  "Total number of newly opened pharmacies at national level since 2019")
  
  p1<- ggplot(data, aes(x = `Month` , y = `no_open`)) +
    geom_line()+
    geom_point()+
    geom_text(data=data, aes(y= `no_open` , label = no_open), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="red")+
    scale_x_date(date_breaks = "3 month", date_labels = "%b-%y")+ 
    #theme_minimal()+ #this produces graph with no boarder line
    theme_bw() +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`no_open`)+1)) +
    labs(title = str_wrap(title1, 90), y = "No. of new pharmacies", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  
  p1
}


plot_open_IMD<-function(){
  
  data<-open%>%
    group_by(Month, IMD_Decile)%>%
    summarise(no_open=n_distinct(FCode))
  
  title1 <- paste(  "Total number of newly opened pharmacies breakdown by IMD Decile (IMD2019)")
  
  
  data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
  
  p1<- ggplot(data, aes(x = `Month` , y = `no_open`, group=`IMD_Decile`,color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data$`no_open`)+1)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~IMD_Decile, ncol = 5)+xlab("")+ 
    labs(title = str_wrap(title1, 90), y = "No. of new pharmacies", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  
  p1
}

plot_open_regional_all<-function(rate = "N"){
  total<-pharm_list %>%
    group_by(until = SnapshotMonth, Region_Name)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(Region_Name))
  
  data2<-open%>%
    group_by(Month, Region_Name)%>%
    summarise(no_open=n_distinct(FCode))%>%
    filter(!is.na(Region_Name),no_open>0, !is.na(no_open))%>%
    mutate(until= Month)%>%
    left_join(total, c("until", "Region_Name"))%>%
    mutate(open_rate = no_open/total*1000)
  
  if(rate == "N"){
  title2 <- paste(  "Total number of newly opened pharmacies at regional level  since 2019")
  
  data2$`Region_Name`<- factor(data2$`Region_Name`, levels = c("East Of England", "London", "Midlands","North East And Yorkshire", "North West","South East", "South West", "Region Unknown"))
  
  p2<- ggplot() +
    geom_line(data2, mapping =aes(x = `Month` , y = `no_open`, group = `Region_Name`, colour = `Region_Name`)) +
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    geom_point()+
    geom_text(data=data2, aes(x = `Month` , y = `no_open`, label = `no_open`), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5), legend.position = "right"
          ,legend.title = element_blank ())+
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data2$`no_open`)+1)) +
    facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
    labs(title = str_wrap(title2, 1200), y = "No. of new pharmacies", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  }
  else if(rate == "Y"){
    title2 <- paste(  "Total number of newly opened pharmacies at regional level  since 2019")
    subtitle <- paste("No. of new openings per 1000 pharmacies = total number of newly opened pharmacies/ Total number of open premises in the same quarter * 1000")
    
    data2$`Region_Name`<- factor(data2$`Region_Name`, levels = c("East Of England", "London", "Midlands","North East And Yorkshire", "North West","South East", "South West", "Region Unknown"))
    
    p2<- ggplot() +
      geom_line(data2, mapping =aes(x = `Month` , y = `open_rate`, colour = `Region_Name`)) +
      scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
      theme_bw() +
      geom_point()+
      geom_text(data=data2, aes(x = `Month` , y = `open_rate`, label = round(`open_rate`,0)), 
                position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5), legend.position = "right"
            ,legend.title = element_blank ())+
      scale_size_manual(values = 1) +
      scale_y_continuous(label = scales::comma,
                         breaks = scales::pretty_breaks(), limits = c(0, max(data2$`open_rate`)+1)) +
      facet_wrap(~Region_Name, ncol = 4)+xlab("")+ 
      labs(title = str_wrap(title2, 120), subtitle = subtitle, y = "No. of new opening per 1k pharmacies", x="Effective Month",
           caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  }
  p2
}

plot_open_regional<-function(region="London", rate = "N"){
  total2<-pharm_list %>%
    group_by(until = SnapshotMonth, STP)%>%
    summarise(total = n_distinct(FCode))%>%
    filter(!is.na(STP))
  
  data<-open%>%
    filter(Region_Name== region)%>%
    group_by(Month, Region_Name)%>%
    summarise(no_open=n_distinct(FCode))
  
  data2<-open%>%
    filter(Region_Name== region)%>%
    group_by(Month, `STP`, `ICB_print`)%>%
    summarise(no_open=n_distinct(FCode))%>%
    filter(!is.na(`STP`), `STP` != "NA (Old STP Code)", !is.na(no_open))%>%
    mutate(until= Month)%>%
    left_join(total2, c("until", "STP"))%>%
    mutate(open_rate = no_open/total*100)%>%
    mutate(ICB=`STP`)%>%collect()
  
  if (rate == "N"){
  
  title1 <- paste0(  "Total number of newly opened pharmacies at ICB level in ", region)
  
  data2$ICB_print<-ifelse(is.na(data2$ICB_print), data2$ICB, data2$ICB_print)
  
  p1<- ggplot() +
   # geom_line(data, mapping = aes(x = `Month` , y = `no_open`,size = Region_Name ), colour = "black") +
    geom_line(data2, mapping =aes(x = `Month` , y = `no_open`, colour = `ICB_print`), linewidth=1.2) +
    geom_point()+
    geom_text(data=data2, aes(x = `Month` , y = `no_open` , label = `no_open`), 
              position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
    theme_bw() +
    scale_size_manual(values = 1) +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(0, max(data2$`no_open`)+1)) +
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~ICB_print, ncol = 4)+xlab("")+ 
    labs(title = str_wrap(title1, 120), y = "No. of new pharmacies", x="Effective Month",
         caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
  }
  else if (rate == "Y"){
    
    data2<-data2%>%filter(!is.na(open_rate), !is.na(ICB_print))
    
    title1 <- paste0(  "Total number of new openings per 1000 pharmacies at ICB level in ", region)
    subtitle <- paste("No. of new openings per 1000 pharmacies = total number of newly opened pharmacies/ Total number of open premises in the same quarter * 1000")
    
    p1<- ggplot() +
      # geom_line(data, mapping = aes(x = `Month` , y = `no_open`,size = Region_Name ), colour = "black") +
      geom_line(data2, mapping =aes(x = `Month` , y = `open_rate`, colour = `ICB_print`), linewidth=1.2) +
      geom_point()+
      #geom_text(data=data2, aes(x = `Month` , y = `open_rate` , label = `open_rate`), 
               # position= position_dodge(width=1.5), hjust=1, vjust=-.5, color="black")+
      scale_x_date(date_breaks = "6 month", date_labels = "%b-%y")+ 
      theme_bw() +
      scale_size_manual(values = 1) +
      scale_y_continuous(label = scales::comma,
                         breaks = scales::pretty_breaks(), limits = c(0, max(data2$`open_rate`)+1)) +
      theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5)) + 
      facet_wrap(~ICB_print, ncol = 4)+xlab("")+ 
      labs(title = str_wrap(title1, 120), subtitle = subtitle, y = "No. of new openings per 1k pharmacies", x="Effective Month",
           caption =  "*Data source: Pharmacy Opening Closure Relocations monthly data provided by BSA")
    
  }
  p1
}


##### ICB level mapping ----

prep_map_data<-function(){
  data1<- pharm_list%>%
    group_by(SnapshotMonth, `STP Code`)%>%
    summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
    mutate(ICB=`STP Code`,`Month`=SnapshotMonth)%>%
    ungroup()%>%
    select(`Month`,ICB, openPremises, WeeklyOpenHours)%>%
    collect()
  
  
  data2<-pharm_active%>%
    group_by(closed, `STP Code`)%>%
    summarise(noClosure=n_distinct(FCode))%>%
    filter(!is.na(closed), !is.na(`STP Code`))%>%
    mutate(ICB=`STP Code`, `Month`= closed)%>%
    ungroup()%>%
    select(Month, ICB, noClosure)%>%
    collect()
  
  data3<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    group_by(Month, `STP`)%>%
    summarise(no_coo=n_distinct(FCode))%>%
    filter(!is.na(`STP`))%>%
    mutate(ICB=`STP`)%>%
    ungroup()%>%
    select(Month, ICB, no_coo)%>%collect()
  
  data4<-open%>%
    group_by(Month, `STP`)%>%
    summarise(no_open=n_distinct(FCode))%>%
    filter(!is.na(`STP`))%>%
    mutate(ICB=`STP`)%>%
    ungroup()%>%
    select(Month, ICB, no_open)%>%collect()
  
  map_data<-data1%>%left_join(data2, c("Month","ICB"))%>%left_join(data3, c("Month","ICB"))%>%left_join(data4, c("Month","ICB"))
  
  
  m=max(data1$Month)
  
  data_a<- map_data%>%filter(Month==m, ICB!= "NA (Old STP Code)")
  data_b<- map_data%>%filter(Month==as.Date(m,"%Y-%m-%d" )%m-% months(3), ICB!= "NA (Old STP Code)")
  
  map_data<-data_a %>%full_join(data_b, "ICB")%>%
    mutate(prem_rd= (`openPremises.y` - `openPremises.x`)/`openPremises.y`*100, 
           hour_rd= (`WeeklyOpenHours.y`- `WeeklyOpenHours.x`)/`WeeklyOpenHours.y`*100) ## present reduction % as positive figures
  
  ICB_boundaries <- sf::st_read(paste0("C:/Users/", personal,"/OneDrive - NHS/PHARM-2022_23-003 Unplanned closures/Monthly data and reporting/Data files for monthly pack/ICB_boundaries.gpkg"))
  
  map_data <- dplyr::left_join(map_data, ICB_Boundaries, by = c("ICB" = "icb_code"))
  
  map_data<- map_data%>%filter(!is.na(icb_name)) 
  
  # Transform data into sf
  map_data <- sf::st_as_sf(map_data)
  #map_data <- st_sf (map_data)
  
  
  map_data}

map_data<-prep_map_data()

#function to plot maps
plot_ICB_map <- function(interactive_mode = TRUE, measure= "coo"){
  
  m=format(max(map_data$`Month.x`),'%B %Y')
  n=format(max(map_data$`Month.x`)%m-% months(3),'%B %Y')
  
  #toggle between interactive and static output
  if(interactive_mode == TRUE){
    
    tmap_options(check.and.fix = TRUE)
    tmap::tmap_mode("view")
    
  }else{
    tmap_options(check.and.fix = TRUE)
    tmap::tmap_mode("plot")
    
  }
  
  if(measure == "coo"){
    
    cvd_colname <- "no_coo.x"
    map_title <- paste0("Number of change of ownership at ICB level between ",n, " and ", m)
    
    
  }else if(measure == "open" ){
    
    cvd_colname <- "no_open.x"
    map_title <- paste0("Number of newly opened pharmacies at ICB Level between ",n, " and ", m)
    
  }
  else if(measure == "closure" ){
    
    cvd_colname <- "noClosure.x"
    map_title <- paste0("Number of permanent closures at ICB Level between ",n, " and ", m)
    
  }
  else if(measure == "hours" ){
    
    cvd_colname <- "hour_rd"
    map_title <- paste0("Reduced opening hours as % of total open hours at ICB Level between ",n, " and ", m)
    
  }
  else if(measure == "premises" ){
    
    cvd_colname <- "prem_rd"
    map_title <- paste0("Reduction of registered premises as % of total open premises at ICB Level between ",n, " and ", m)
    
  }
  
  map <- tm_shape(map_data) +
    tm_polygons(col = cvd_colname,
                n = 5,
                style = "quantile",
                id = "icb_name",
                title = "",
                palette = "Greens",
                contrast = 1, alpha = 1,
                borders.col = "black") +
    tm_scale_bar() +
    tm_compass(size = 3, position = c("0.85", "0.85")) 
  
  #each output uses a different title argument
  if(interactive_mode == TRUE){
    
    map <- map +
      tm_layout(title = map_title,
                main.title.position = "center",
                main.title.size = 1.05,
                legend.title.size = 0.85,
                legend.format = list(fun = function(x) formatC(x, digits = 0, big.mark = " ", format = "f")))
    
  }else{
    
    map <- map +
      tm_layout(title = str_wrap(map_title, 80),
                main.title.position = "center",
                main.title.size = 1.05,
                legend.title.size = 0.85,
                legend.format = list(fun = function(x) formatC(x, digits = 0, big.mark = " ", format = "f")))
    
  }
  
  map
}


### Data tables for downloading ----
get_premises_table <- function(){
  
  
  data <- ICB_premise_hours %>%
    group_by(SnapshotMonth, Region_Name, ICB)
  
  
  data <- data %>%
    rename(`Reporting Month` = SnapshotMonth, 
           `Region Name` = Region_Name,
           `ICB Code` = ICB,
           `Number of open premises` = openPremises,
           `Population`= pop,
           `Premises per 100k population`= prem_pop)%>%
    select(`Reporting Month`, `Region Name`, `ICB Code`, `Number of open premises`, `Population`, `Premises per 100k population`)
  
  data
  
}

get_hours_table <- function(){
  
  
  data <- ICB_premise_hours %>%
    group_by(SnapshotMonth, Region_Name, ICB)
  
  
  data <- data %>%
    rename(`Reporting Month` = SnapshotMonth, 
           `Region Name` = Region_Name,
           `ICB Code` = ICB,
           `Weekly opening hours` = WeeklyOpenHours,
           `Population`= pop,
           `Weekly hours per 100k population`= hour_pop)%>%
    select(`Reporting Month`, `Region Name`, `ICB Code`, `Weekly opening hours`, `Population`, `Weekly hours per 100k population`)
  
  data
  
}

get_closure_table <- function(){
  
  
  data<-pharm_active%>%
    group_by(closed, Region_Name, `STP Code`)%>%
    summarise(`Number of Permanent closures`=n_distinct(FCode))%>%
    rename(`ICB Code`=`STP Code`, `Month of closure`=`closed`, `Region Name` = Region_Name)%>%
    select(`Month of closure`,`Region Name`, `ICB Code`, `Number of Permanent closures`)%>%
    group_by(`Month of closure`,`Region Name`, `ICB Code`)
  
  
  data
  
}

get_coo_table <- function(){
  
  
  data<-coo%>%
    filter(Type == "CHANGE OF OWNERSHIP")%>%
    group_by(Month, `Region_Name`, `STP`)%>%
    summarise(`Number of changes of ownership`=n_distinct(FCode))%>%
    rename(`ICB Code`=`STP`, `Month of ownership change`=`Month`, `Region Name` = Region_Name)%>%
    select(`Month of ownership change`,`Region Name`, `ICB Code`, `Number of changes of ownership`)%>%
    group_by(`Month of ownership change`,`Region Name`, `ICB Code`)
  
  
  data
  
}

get_open_table <- function(){
  
  
  data<-open%>%
    group_by(Month, `Region_Name`, `STP`)%>%
    summarise(`Number of new pharmacies`=n_distinct(FCode))%>%
    rename(`ICB Code`=`STP`, `Effective Month`=`Month`, `Region Name` = Region_Name)%>%
    select(`Effective Month`,`Region Name`, `ICB Code`, `Number of new pharmacies`)%>%
    group_by(`Effective Month`,`Region Name`, `ICB Code`)
  
  
  data
  
}


#### Market share by chains ----
#color<-function(){
# The palette with black:
#Palette <- c("#E69F00", "#009E73", "#F0E442", 
#   "#0072B2", "#D55E00", "#56B4E9", 
#  "#CC79A7", "#999999","#FFCCFF", 
#  "#00CC00", "#FF00FF","#9E9E9E",
#  "#8968CD", "#9ACD32","#000000",
#  "#DF536B", "#F5C710","#61D04F",
#  "#CD0BBD", "#2297E6","#28E2E5")}

plot_pie1<-function(level="National",type="premises"){
  
  data<-pharm_list%>%
    filter(SnapshotMonth == max(SnapshotMonth))%>%
    select(FCode,`Weekly Total`)%>%
    right_join(latest_contractor, "FCode")
  
  
  if(level=="National"){
    data1<-data%>%
      group_by(ParentOrgSize)%>%
      summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
      mutate(premises_pc=round(openPremises/sum(openPremises)*100,1), hours_pc=round(WeeklyOpenHours/sum(WeeklyOpenHours)*100, 1))%>%
      mutate(ParentOrgSize = ifelse(is.na(ParentOrgSize), "ParentOrg Unknown", ParentOrgSize))
    
    data1$`ParentOrgSize`<- factor(data1$`ParentOrgSize`, levels = c("Independants","Small Multiples(2-5)", "Medium Multiples(6-35)","Large Multiples", "ParentOrg Unknown"))
    
  }
  else {
    data1<-data%>%
      group_by(Region_Name, ParentOrgSize)%>%
      summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
      mutate(premises_pc=round(openPremises/sum(openPremises)*100,1), hours_pc=round(WeeklyOpenHours/sum(WeeklyOpenHours)*100, 1))%>%
      filter(Region_Name == level)
    
    data1$`ParentOrgSize`<- factor(data1$`ParentOrgSize`, levels = c("Independants","Small Multiples(2-5)", "Medium Multiples(6-35)","Large Multiples", "ParentOrg Unknown"))
    
  } 
  
  
  if(type=="premises"){
    
    df<-data1 %>%
      mutate(end = 2 * pi * cumsum(premises_pc)/sum(premises_pc),
             start = lag(end, default = 0),
             middle = 0.5 * (start + end),
             hjust = ifelse(middle > pi, 1, 0),
             vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
    
    
    
    pie1<-ggplot(df) + 
      geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                       start = start, end = end, fill = ParentOrgSize)) +
      geom_text_repel(aes(x = 1 * sin(middle), y = 1 * cos(middle), label = str_wrap(paste0(ParentOrgSize," - ", paste(round(premises_pc, 1), "%")),35),
                    hjust = hjust, vjust = vjust)) +
      coord_fixed() +
      scale_fill_brewer(palette = "Pastel1") +
      scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL) +
      scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL)+
      labs(fill = NULL, x = NULL,y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
           title = paste0("% of total premises (", level, ")"))
    
    # pie1<- ggplot(data1, aes(x="", y=premises_pc, fill=ParentOrgSize))+
    # geom_bar(width = 1, stat = "identity") +
    #coord_polar("y", start=0) +
    # theme_minimal()+
    #  theme(
    #  axis.title = element_blank(), 
    #   axis.ticks = element_blank(), 
    #  axis.text = element_blank(),
    #  panel.background =element_blank(),
    #  panel.border=element_blank(),
    #   plot.background =element_blank(), 
    #  panel.grid = element_blank(),
    #  plot.title=element_text(size=14, face="bold"))+
    # labs(fill = NULL,
    #  x = NULL,
    #  y = NULL,
    #   title = "Market share breakdown by parent organisation size: % of total premises")+
    #  geom_text(aes(label = paste(round(premises_pc, 1), "%")),
    #   position = position_stack(vjust = 0.5)) +
    #  scale_fill_manual(values = color())
  }
  else{
    
    df<-data1 %>%
      mutate(end = 2 * pi * cumsum(hours_pc)/sum(hours_pc),
             start = lag(end, default = 0),
             middle = 0.5 * (start + end),
             hjust = ifelse(middle > pi, 1, 0),
             vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
    
    
    
    pie1<-ggplot(df) + 
      geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                       start = start, end = end, fill = ParentOrgSize)) +
      geom_text(aes(x = 1 * sin(middle), y = 1 * cos(middle), label = str_wrap(paste0(ParentOrgSize," - ", paste(round(premises_pc, 1), "%")),35),
                    hjust = hjust, vjust = vjust)) +
      coord_fixed() +
      scale_fill_brewer(palette = "Pastel1") +
      scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL) +
      scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL)+
      labs(fill = NULL, x = NULL, y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
           title = paste0("% of weekly total opening hours (", level, ")"))
    
    
  }
  
  pie1
}

plot_pie2<-function(level="National",type="premises"){
  
  data<-pharm_list%>%
    filter(SnapshotMonth == max(SnapshotMonth))%>%
    select(FCode,`Weekly Total`)%>%
    right_join(latest_contractor, "FCode")
  
  if(level=="National"){
    data1<-data%>%
      group_by(ParentOrgName_large)%>%
      summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
      mutate(premises_pc=round(openPremises/sum(openPremises)*100,1), hours_pc=round(WeeklyOpenHours/sum(WeeklyOpenHours)*100, 1))
    
    data1$rank_prem<- NA
    data1$rank_hour<- NA
    data1$rank_prem[order(data1$premises_pc)] <- 1:nrow(data1)
    data1$rank_hour[order(data1$hours_pc)] <- 1:nrow(data1)
    
  }
  else {
    data1<-data%>%
      group_by(Region_Name, ParentOrgName_large)%>%
      summarise(openPremises = n_distinct(FCode), WeeklyOpenHours=sum(`Weekly Total`, na.rm=T))%>%
      mutate(premises_pc=round(openPremises/sum(openPremises)*100,1), hours_pc=round(WeeklyOpenHours/sum(WeeklyOpenHours)*100, 1))%>%
      filter(Region_Name == level)
    
    data1$rank_prem<- NA
    data1$rank_hour<- NA
    data1$rank_prem[order(data1$premises_pc)] <- 1:nrow(data1)
    data1$rank_hour[order(data1$hours_pc)] <- 1:nrow(data1)
  } 
  
  
  if(type=="premises"){
    
    data1<-data1%>%
      mutate(ParentOrgName_large2=case_when(rank_prem<max(rank_prem)-6 ~ "Other large multiples",
                                            rank_prem>=max(rank_prem)-6 ~ ParentOrgName_large))
    
    
    data1<-data1%>%
      group_by(ParentOrgName_large2)%>%
      summarise(openPremises = sum(`openPremises`, na.rm=T))%>%
      mutate(premises_pc=round(openPremises/sum(openPremises)*100,1))
    
    df<-data1 %>%
      mutate(end = 2 * pi * cumsum(premises_pc)/sum(premises_pc),
             start = lag(end, default = 0),
             middle = 0.5 * (start + end),
             hjust = ifelse(middle > pi, 1, 0),
             vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
    
    
    
    pie1<-ggplot(df) + 
      geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                       start = start, end = end, fill = ParentOrgName_large2)) +
      geom_text_repel(aes(x = 1 * sin(middle), y = 1.1 * cos(middle), label = str_wrap(paste0(ParentOrgName_large2," - ", paste(round(premises_pc, 1), "%")),30),
                    hjust = hjust, vjust = vjust)) +
      coord_fixed() +
      scale_fill_brewer(palette = "Pastel1") +
      scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL) +
      scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL)+
      labs(fill = NULL, x = NULL,y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"), title = paste0("% of total premises (", level, ")"))
    
  }
  else{
    
    data1<-data1%>%
      mutate(ParentOrgName_large2=case_when(rank_hour<max(rank_prem)-6 ~ "Other large multiples",
                                            rank_hour>=max(rank_prem)-6 ~ ParentOrgName_large))
    
    data1<-data1%>%
      group_by(ParentOrgName_large2)%>%
      summarise(WeeklyOpenHours=sum(`WeeklyOpenHours`, na.rm=T))%>%
      mutate(hours_pc=round(WeeklyOpenHours/sum(WeeklyOpenHours)*100, 1))
    
    df<-data1 %>%
      mutate(end = 2 * pi * cumsum(hours_pc)/sum(hours_pc),
             start = lag(end, default = 0),
             middle = 0.5 * (start + end),
             hjust = ifelse(middle > pi, 1, 0),
             vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
    
    
    
    pie1<-ggplot(df) + 
      geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                       start = start, end = end, fill = ParentOrgName_large2)) +
      geom_text(aes(x = 1 * sin(middle), y = 1.1 * cos(middle), label = str_wrap(paste0(ParentOrgName_large2," - ", paste(round(hours_pc, 1), "%")),30),
                    hjust = hjust, vjust = vjust)) +
      coord_fixed() +
      scale_fill_brewer(palette = "Pastel1") +
      scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL) +
      scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL)+
      labs(fill = NULL, x = NULL, y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
           title = paste0(" % of weekly total opening hours (", level, ")"))
    
  }
  
  pie1
}

plot_pie3<-function(level="National", since= "2023-04-01", breakdown= TRUE){
  
  data<-pharm_active%>%
    filter(!is.na(closed))%>%
    select(FCode, closed)%>%
    right_join(latest_contractor, "FCode")%>%
    filter(closed >= since)
  
  if(level=="National"){
    
    data1<-data%>%
      group_by(ParentOrgName_large)%>%
      summarise(noClosure=n_distinct(FCode))%>%
      mutate(closure_pc=round(noClosure/sum(noClosure)*100,1))
    
    
    data1$rank_closure<- NA
    data1$rank_closure[order(data1$closure_pc)] <- 1:nrow(data1)
    
    data2<-data%>%
      group_by(ParentOrgSize)%>%
      summarise(noClosure=n_distinct(FCode))%>%
      mutate(closure_pc=round(noClosure/sum(noClosure)*100,1))
    
  }
  else {
    data1<-data%>%
      group_by(Region_Name, ParentOrgName_large)%>%
      summarise(noClosure=n_distinct(FCode))%>%
      mutate(closure_pc=round(noClosure/sum(noClosure)*100,1))%>%
      filter(Region_Name == level)
    
    
    data1$rank_closure<- NA
    data1$rank_closure[order(data1$closure_pc)] <- 1:nrow(data1)
    
    data2<-data%>%
      group_by(Region_Name, ParentOrgSize)%>%
      summarise(noClosure=n_distinct(FCode))%>%
      mutate(closure_pc=round(noClosure/sum(noClosure)*100,1))%>%
      filter(Region_Name == level)
  } 
  
  if(breakdown== TRUE){
    
    data1<-data1%>%
      mutate(ParentOrgName_large2=case_when(rank_closure<max(rank_closure)-6 ~ "Other large multiples",
                                            rank_closure>=max(rank_closure)-6 ~ ParentOrgName_large))
    
    
    data1<-data1%>%
      group_by(ParentOrgName_large2)%>%
      summarise(noClosure = sum(`noClosure`, na.rm=T))%>%
      mutate(closure_pc=round(noClosure/sum(noClosure)*100,1))
    
    df<-data1 %>%
      mutate(end = 2 * pi * cumsum(closure_pc)/sum(closure_pc),
             start = lag(end, default = 0),
             middle = 0.5 * (start + end),
             hjust = ifelse(middle > pi, 1, 0),
             vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
    
    
    
    pie1<-ggplot(df) + 
      geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                       start = start, end = end, fill = ParentOrgName_large2)) +
      geom_text_repel(aes(x = 1 * sin(middle), y = 1.1 * cos(middle), label = str_wrap(paste0(ParentOrgName_large2," - ", paste(round(closure_pc, 1), "%")),30),
                    hjust = hjust, vjust = vjust)) +
      coord_fixed() +
      scale_fill_brewer(palette = "Pastel1") +
      scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL) +
      scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL)+
      labs(fill = NULL, x = NULL,y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
           title = paste0("% of total Permanent closures (", level, ") since ", since))
    
  }
  else{
    
    df<-data2 %>%
      mutate(end = 2 * pi * cumsum(closure_pc)/sum(closure_pc),
             start = lag(end, default = 0),
             middle = 0.5 * (start + end),
             hjust = ifelse(middle > pi, 1, 0),
             vjust = ifelse(middle < pi/2 | middle > 3 * pi/2, 0, 1))
    
    
    
    pie1<-ggplot(df) + 
      geom_arc_bar(aes(x0 = 0, y0 = 0, r0 = 0, r = 1,
                       start = start, end = end, fill = ParentOrgSize)) +
      geom_text(aes(x = 1 * sin(middle), y = 1.1 * cos(middle), label = str_wrap(paste0(ParentOrgSize," - ", paste(round(closure_pc, 1), "%")),30),
                    hjust = hjust, vjust = vjust)) +
      coord_fixed() +
      scale_fill_brewer(palette = "Pastel1") +
      scale_x_continuous(limits = c(-2, 1.9),  # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL) +
      scale_y_continuous(limits = c(-1.3, 1.3),    # Adjust so labels are not cut off
                         name = "", breaks = NULL, labels = NULL)+
      labs(fill = NULL, x = NULL,y = NULL, caption =  paste0("*Data source: Pharmaceutical List ",as.character(format(max(pharm_active$until),'%B %Y')), " publication"),
           title = paste0("% of total Permanent closures (", level, ") since ", since))
    
  }
  
  pie1
  
  
}


##### Pharmacy income ----

#color<-function(){
# The palette with black:
#Palette <- c("#E69F00", "#009E73", "#F0E442", 
#   "#0072B2", "#D55E00", "#56B4E9", 
#  "#CC79A7", "#999999","#FFCCFF", 
#  "#00CC00", "#FF00FF","#9E9E9E",
#  "#8968CD", "#9ACD32","#000000",
#  "#DF536B", "#F5C710","#61D04F",
#  "#CD0BBD", "#2297E6","#28E2E5")}


sql="select[FY_Qs], [Contractor Code], [type], sum([Figure]) as Income
from 
(select [Month], [Contractor Code], [Figure],
case when [Measure] in ('Other Dispensing Activity Related Payments',
'Local enhanced services + local approved payments',
'Infrastructure Payments',
'NHS Smoking Cessation Service Pilot Income',
'Tier 2 Initiation of Oral Contraception Pilot Income',
'General Practice Digital Minor Illness Service Pilot Income',
'Other Local enhanced services + local approved payments',
'UEC to CPCS Pilot Income',
'Medicine Use Review and Prescription Intervention Service (MUR) Income',
'Tier 1 Ongoing supply of Oral Contraception Pilot Income',
'Covid Test Distribution Service Income',
'Hypertension Case Finding Pilot Income',
'Urgent Medicine Supply Advanced Service (NUMSAS) Income',
'Pharmacy Access Scheme',
'Other Miscellaneous',
'Methadone Payment',
'Pharmacy Quality Scheme') then  'OtherIncome'
when [Measure] in (
'Stoma Customisation (STOMA) Income',
'New Medicine Service (NMS) Income',
'Hypertension Case-finding Service Income',
'Tier 1 Contraception Service Consultation Income',
'Appliance Use Reviews (AUR) Income',
'CPCS Setup Income',
'Hepatitis C Antibody Testing Service Income',
'Tier 1 Contraception Service Set Up fee',
'CPCS GP Referral Pathway Engagement Income',
'Smoking Cessation Service Consultations Income',
'Discharge Medicine Service Income',
'Hypertension Case-finding Service Setup Fee',
'Smoking Cessation Service Set Up fee',
'Community Pharmacist Consultation Service (CPCS) Income') then  'CPCFIncome'
when [Measure] = 'Seasonal Influenza Vaccination Advances Service (FLU) Income' then 'CovidIncome'
when [Measure] = 'Covid Vaccination Service Income' then 'FluIncome'
when [Measure] = 'Dispensing Activity Fee' then 'DispensingFee'
else '' end as [type],
	  case when [Month] between '2018-04-01' and '2018-06-01' then 'FY2018/19 Q1'
	  when [Month] between '2018-07-01' and '2018-09-01' then 'FY2018/19 Q2'
	   when [Month] between '2018-10-01' and '2018-12-01' then 'FY2018/19 Q3'
	    when [Month] between '2019-01-01' and '2019-03-01' then 'FY2018/19 Q4'
	 when [Month] between '2019-04-01' and '2019-06-01' then 'FY2019/20 Q1'
	  when [Month] between '2019-07-01' and '2019-09-01' then 'FY2019/20 Q2'
	   when [Month] between '2019-10-01' and '2019-12-01' then 'FY2019/20 Q3'
	    when [Month] between '2020-01-01' and '2020-03-01' then 'FY2019/20 Q4'
	 when [Month] between '2020-04-01' and '2020-06-01' then 'FY2020/21 Q1'
	  when [Month] between '2020-07-01' and '2020-09-01' then 'FY2020/21 Q2'
	  	when [Month] between '2020-10-01' and '2020-12-01' then 'FY2020/21 Q3'
		 when [Month] between '2021-01-01' and '2021-03-01' then 'FY2020/21 Q4'
	 when [Month] between '2021-04-01' and '2021-06-01' then 'FY2021/22 Q1'
	  when [Month] between '2021-07-01' and '2021-09-01' then 'FY2021/22 Q2'
	   when [Month] between '2021-10-01' and '2021-12-01' then 'FY2021/22 Q3'
	    when [Month] between '2022-01-01' and '2022-03-01' then 'FY2021/22 Q4'
	 when [Month] between '2022-04-01' and '2022-06-01' then 'FY2022/23 Q1'
	  when [Month] between '2022-07-01' and '2022-09-01' then 'FY2022/23 Q2'
		when [Month] between '2022-10-01' and '2022-12-01' then 'FY2022/23 Q3'
		 when [Month] between '2023-01-01' and '2023-03-01' then 'FY2022/23 Q4'
	 when [Month] between '2023-04-01' and '2023-06-01' then 'FY2023/24 Q1'
	  when [Month] between '2023-07-01' and '2023-09-01' then 'FY2023/24 Q2'
		when [Month] between '2023-10-01' and '2023-12-01' then 'FY2023/24 Q3'
		  when [Month] between '2024-01-01' and '2024-03-01' then 'FY2023/24 Q4'
	when [Month] between '2024-04-01' and '2024-06-01' then 'FY2024/25 Q1'
	  when [Month] between '2024-07-01' and '2024-09-01' then 'FY2024/25 Q2'
	  	when [Month] between '2024-10-01' and '2024-12-01' then 'FY2024/25 Q3'
	  when [Month] between '2025-01-01' and '2025-03-01' then 'FY2024/25 Q4'
	  	when [Month] between '2025-04-01' and '2025-06-01' then 'FY2025/26 Q1'
	  when [Month] between '2025-07-01' and '2025-09-01' then 'FY2025/26 Q2'
	  	  	when [Month] between '2025-10-01' and '2025-12-01' then 'FY2025/26 Q3'
	  when [Month] between '2026-01-01' and '2026-03-01' then 'FY2025/26 Q4'
	 else '' end as [FY_Qs]
 FROM [NHSE_Sandbox_DispensingReporting].[dbo].[Pharmacy_Dashboard_Master]
 where [Month]>='2018-04-01' 
	) a
 group by a.[FY_Qs],a.[type], a.[Contractor Code] 
 having [type] != ''
"


result <- dbSendQuery(con,sql)
Pharm_income<- dbFetch(result)
dbClearResult(result)

pharm_income_2<-Pharm_income%>%
  group_by(`Contractor Code`, `FY_Qs`)%>%
  summarise(Income = sum(Income, na.rm=T))%>%
  mutate(type = "NHS_Income")


Pharm_income <-rbind(Pharm_income, pharm_income_2)

Pharm_income <-Pharm_income%>%
  filter(Income>0) %>%
  rename(`FCode`=`Contractor Code`)%>%
  left_join(latest_contractor,"FCode")%>%
  distinct()

plot_income_national<-function(income = "NHS_Income"){
  
  t<-Pharm_income %>%
    filter(type == income)%>%
    group_by(FY_Qs)%>%
    summarise(total=sum(Income, na.rm=T))%>%
    filter(FY_Qs!="")
  
  t2 <- Pharm_income %>%
    filter(type == income)%>%
    group_by(FY_Qs)%>%
    summarise(Minimum= quantile(Income, probs = c(0)) ,
              First_Quartile= quantile(Income, probs = c(0.25)) ,
              Second_Quartile= quantile(Income, probs = c(0.5)) ,
              Third_Quartile= quantile(Income, probs = c(0.75)),
              Maximum = quantile(Income, probs = c(1)) )%>%
    filter(FY_Qs!="")
  
  t2<-melt(setDT(t2), id.vars = c("FY_Qs"), variable.name = "Pharmacy_Income")
  t2$`Pharmacy_Income`<- factor(  t2$`Pharmacy_Income`, levels = c("Minimum", "First_Quartile", "Second_Quartile","Third_Quartile", "Maximum"))
  t2a<-t2%>%filter(`Pharmacy_Income` %in% c ("Minimum","Maximum"))
  t2b<-t2%>%filter(`Pharmacy_Income` %in% c ("First_Quartile", "Second_Quartile","Third_Quartile"))
  
  if(income == "NHS_Income"){
    title2 <- paste("Quarterly total NHS income per pharmacy - National view")
    title <- paste("Quarterly total NHS income at national level")}
  else if(income == "CPCFIncome") { 
    title2 <- paste("Quarterly total CPCF clinical service income per pharmacy - National view")
    title <- paste("Quarterly total CPCF clinical service income at national level")}
  else if(income == "CovidIncome") { 
    title2 <- paste("Quarterly total COVID vaccination service income per pharmacy - National view")
    title <- paste("Quarterly total COVID vaccination service income at national level")}
  else if(income == "FluIncome") { 
    title2 <- paste("Quarterly total Flu vaccination service income per pharmacy - National view")
    title <- paste("Quarterly total Flu vaccination service income at national level")}
  else {
    title2 <- paste("Quarterly total dispensing income per pharmacy - National view")
    title <- paste("Quarterly total dispensing income at national level")
  }
  
  
  p1 <- ggplot(t, aes(x = `FY_Qs` , y = `total`,group = 1)) +
    geom_line(color="steelblue")+
    geom_point()+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks())+
    labs(title = str_wrap(title, 90))+
    theme(legend.position = "none")+
    ylab("Total Income (GBP)") +
    theme_bw()+ scale_colour_manual()+
    theme(axis.text.x=element_text(angle=20,hjust=1,vjust=1))
  
  p2 <- ggplot(t2a, aes(x = `FY_Qs` , y = `value`, group =`Pharmacy_Income`,color = `Pharmacy_Income`)) +
    geom_line()+
    theme_bw()+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks())+
    labs(title = str_wrap(title2, 90))+
    theme(legend.position = "right",legend.title = element_blank ())+
    ylab("Total Income (GBP) \nper Pharmacy") +
    theme(axis.text.x=element_blank())
  
  p3 <- ggplot(t2b, aes(x = `FY_Qs` , y = `value`, group =`Pharmacy_Income`,color = `Pharmacy_Income`)) +
    geom_line()+
    theme_bw()+
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks())+
    labs(title = str_wrap(title2, 90))+
    theme(legend.position = "right",legend.title = element_blank ())+
    ylab("Total Income (GBP) \nper pharmacy") +
    theme(axis.text.x=element_text(angle=20,hjust=1,vjust=1))
  
  
  
  p <- ggarrange(p1+xlab(NULL),p2+xlab(NULL),p3+xlab(NULL),
                 ncol = 1, nrow = 3,align = "v")
  
  p
}

plot_income_regional<-function(income = "NHS_Income", region= "London"){
  
  
  data <-Pharm_income %>%filter(type == income, Region_Name== region)
  
  
  if(income == "NHS_Income"){
    title2 <- paste0("Quarterly total NHS income per pharmacy in ", region)}
  else if(income == "CPCFIncome") { 
    title2 <- paste0("Quarterly total CPCF clinical service income per pharmacy in ", region)}
  else if(income == "CovidIncome") { 
    title2 <- paste0("Quarterly total COVID vaccination service income per pharmacy in ", region)}
  
  else if(income == "FluIncome") { 
    title2 <- paste0("Quarterly total Flu vaccination service income per pharmacy in ", region)}
  
  else {
    title2 <- paste0("Quarterly total dispensing income per pharmacy in ", region)
    
  }
  
  
  p2<- ggplot(data, aes(x =`FY_Qs`, y = `Income`,group = `FY_Qs`)) +
    geom_boxplot(outlier.colour = "steelblue",
                 outlier.size = 2) +
    stat_summary(fun = mean,
                 geom = "point",
                 size = 2,
                 color = "red") +
    theme_classic()+    
    scale_y_continuous(trans='log10',label = scales::comma,
                       breaks = scales::pretty_breaks())+
    theme(axis.text.x=element_text(angle=45,hjust=0.5,vjust=0.5))+
    ylab("Quarterly income (GBP) per pharmacy")+labs(title = str_wrap(title2, 90))
  
  
  
  p2
}
plot_income_regional_2<-function(Q="FY2022/23 Q2",region= "London"){
  
  
  data <-Pharm_income %>%filter(Region_Name== region, FY_Qs %in% c("FY2022/23 Q2","FY2022/23 Q1","FY2021/22 Q4", "FY2021/22 Q3"))
  data$type<-factor(data$type, c("NHS_Income","DispensingFee","CPCFIncome","CovidIncome","FluIncome","OtherIncome"))
  
  data<-data %>% group_by(type, FY_Qs)%>%mutate(sd = sd(Income, na.rm=T))%>%
    filter(FY_Qs!="")
  
  p2 <- ggplot(data, aes(x= FY_Qs, y= Income, fill= type, Color= type))+
    geom_bar(stat = "identity", position = "dodge", alpha=1)+
    geom_errorbar(aes(ymin= Income - sd, ymax= Income + sd),
                  position = position_dodge(0.9),width=0.25,   alpha= 0.6)+
    scale_fill_brewer(palette = "Greens")+
    #geom_text(aes(label=Income),position=position_dodge(width=0.9), vjust=-0.25, hjust= -0.1, size= 3)+
    #geom_text(aes(label= Tukey), position= position_dodge(0.9), size=3, vjust=-0.8, hjust= -0.5, color= "gray25")+
    theme_bw()+
    theme(legend.position = c(0.2, 0.9),legend.direction = "horizontal")+
    theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank())+
    # labs(fill="Income (GBP)")+
    theme(axis.text.x = element_text(angle = 25, size =9, vjust = 1, hjust=1))+
    scale_y_continuous(trans='log10',label = scales::comma,
                       breaks = scales::pretty_breaks())+
    # scale_x_discrete(labels= c( "Site 1\n(Hibiscus tillaceus)","Site 2 \n(Ceiba pentandra)","Site 3 \n(Clitoria fairchildiana)","Site 4 \n(Pachira aquatica)"))+
    #theme(legend.position = c(0.85, 0.7))+
    labs(y= "Quarterly Income\n(Type of income)", title = "Quarterly pharmacy Income by type", subtitle = "In the last 4 quarters")
  
  
  
  
  p2
}



##### Pharmacy activities ----
sql<- "
select  [FY_Qs],
		[Contractor],
		[Measure],
		sum(Figure) as Figure
		from
(SELECT	t.[Month],
        t.[FY_Qs],
		t.[Contractor],
		t.[Measure],
		t.[Figure]
from
(select a.*, b.[Seasonal Influenza Vaccination Advances Service (FLU) Activity], d.[Stop Smoking Service Activity], e.[Discharge Medicine Service Activity], c.[Items Dispensed],
case when a.[Month] between '2018-04-01' and '2018-06-01' then '2018-06-01'
	  when a.[Month] between '2018-07-01' and '2018-09-01' then '2018-09-01'
	   when a.[Month] between '2018-10-01' and '2018-12-01' then '2018-12-01'
	    when a.[Month] between '2019-01-01' and '2019-03-01' then '2019-03-01'
	 when a.[Month] between '2019-04-01' and '2019-06-01' then '2019-06-01'
	  when a.[Month] between '2019-07-01' and '2019-09-01' then '2019-09-01'
	   when a.[Month] between '2019-10-01' and '2019-12-01' then '2019-12-01'
	    when a.[Month] between '2020-01-01' and '2020-03-01' then '2020-03-01'
	 when a.[Month] between '2020-04-01' and '2020-06-01' then '2020-06-01'
	  when a.[Month] between '2020-07-01' and '2020-09-01' then '2020-09-01'
	  	when a.[Month] between '2020-10-01' and '2020-12-01' then '2020-12-01'
		 when a.[Month] between '2021-01-01' and '2021-03-01' then '2021-03-01'
	 when a.[Month] between '2021-04-01' and '2021-06-01' then '2021-06-01'
	  when a.[Month] between '2021-07-01' and '2021-09-01' then '2021-09-01'
	   when a.[Month] between '2021-10-01' and '2021-12-01' then '2021-12-01'
	    when a.[Month] between '2022-01-01' and '2022-03-01' then '2022-03-01'
	 when a.[Month] between '2022-04-01' and '2022-06-01' then '2022-06-01'
	  when a.[Month] between '2022-07-01' and '2022-09-01' then '2022-09-01'
		when a.[Month] between '2022-10-01' and '2022-12-01' then '2022-12-01'
		 when a.[Month] between '2023-01-01' and '2023-03-01' then '2023-03-01'
	 when a.[Month] between '2023-04-01' and '2023-06-01' then '2023-06-01'
	  when a.[Month] between '2023-07-01' and '2023-09-01' then '2023-09-01'
		when a.[Month] between '2023-10-01' and '2023-12-01' then '2023-12-01'
		  when a.[Month] between '2024-01-01' and '2024-03-01' then '2024-03-01'
	when a.[Month] between '2024-04-01' and '2024-06-01' then '2024-06-01'
	  when a.[Month] between '2024-07-01' and '2024-09-01' then '2024-09-01'
	  	  	when a.[Month] between '2024-10-01' and '2024-12-01' then '2024-12-01'
	  when a.[Month] between '2025-01-01' and '2025-03-01' then '2025-03-01'
	  	when a.[Month] between '2025-04-01' and '2025-06-01' then '2025-06-01'
	  when a.[Month] between '2025-07-01' and '2025-09-01' then '2025-09-01'
	  	  	when a.[Month] between '2025-10-01' and '2025-12-01' then '2025-12-01'
	  when a.[Month] between '2026-01-01' and '2026-03-01' then '2026-03-01'
	 else '' end as [FY_Qs]
from
(SELECT  [Month],[Contractor],
       cast(isnull([MED USE REVIEWS],0) as int) [Medicine Use Review and Prescription Intervention Service (MUR) Activity],
		cast(isnull([NMS NUM],0) as int) [New Medicine Service (NMS) Activity],
		cast(ISNULL([AUR HOME NUM],0) + ISNULL([AUR PREM NUM],0) as int) [Appliance Use Reviews (AUR) Activity],
		cast(isnull([STOMA CUST FEE],0)/4.32 as int) [Stoma Customisation (STOMA) Activity],
		cast(isnull([HEP C SRVCE],0)/36 as int) [Hepatitis C Antibody Testing Service Activity],
		cast(isnull([COVID_VACC],0)/12.58 as int) [Covid Vaccination Service Activity],
		cast((ISNULL([CPCS FEES],0) + ISNULL([CPCS FEES ADJ],0))/14 as int) [Community Pharmacist Consultation Service (CPCS) Activity],
		cast((ISNULL([PF_CP_CONSULTATION_FEE],0)+ISNULL([PF_CP_CONSULTATION_FEE_ADJ],0))/15 as int) [Pharmacy First Clinical Pathways Consultation Activity],
      cast((ISNULL([PF_MI_&_UMS_FEE] ,0)+ ISNULL([PF_MI_&_UMS_FEE_ADJ] ,0))/15 as int) [Pharmacy First MI & UMS Consultation Activity],
	 cast((isnull([CONTRACEPTION_CONSULTATIONS],0)+ isnull([CONTRACEPTION_CONSULTATION_ADJ],0))/18 as int) [Oral Contraception Consultation Activity]
  FROM [NHSE_Sandbox_DispensingReporting].[dbo].[MIS_Pharmacy]) a
full join
  (SELECT [MonthCommencing] [Month], RTRIM(LTRIM([Dispenser Code])) [Contractor Code],
	cast([Quantity]  as int) as [Seasonal Influenza Vaccination Advances Service (FLU) Activity]
FROM [NHSE_Sandbox_DispensingReporting].[dbo].[AdvancedFluReport] ) b
on a.[Contractor]=b.[Contractor Code] and a.[Month]=b.[Month] 
full join
(SELECT	[Date] [Month],
				RTRIM(LTRIM([Contractor Code])) [Contractor Code],
				cast([Number of Items] as int) [Items Dispensed]
		FROM [NHSE_Sandbox_DispensingReporting].[dbo].[Pharm_and_DAC]
		WHERE [Contractor Type] NOT LIKE  'Appliance' ) c
on  a.[Contractor]=c.[Contractor Code] and a.[Month]=c.[Month] 
full join
(SELECT
      [Pharmacy Code]
      ,[Month(claim)]
      ,cast([Number of consultations]  as int) as [Stop Smoking Service Activity]
  FROM [NHSE_Sandbox_DispensingReporting].[dbo].[SCS_activity]) d
  on a.[Month] =d.[Month(claim)] and a.[Contractor]=d.[Pharmacy Code]
full join
(SELECT [Pharmacy Code]
      ,  DATEADD(Month,-1,[EXTRACT_Month])as [Month]
	  , Cast(count(*) as int) as [Discharge Medicine Service Activity]
  FROM [NHSE_Sandbox_DispensingReporting].[dbo].[DMScombined]
  group by [Pharmacy Code],DATEADD(Month,-1,[EXTRACT_Month])) e
  on a.[Month]=e.[Month] and a.[Contractor]=e.[Pharmacy Code] 
  ) f
  UNPIVOT (
	Figure
	for Measure IN ([Items Dispensed], 	
	[New Medicine Service (NMS) Activity],
	[Discharge Medicine Service Activity], 
	[Stop Smoking Service Activity], 
	[Pharmacy First Clinical Pathways Consultation Activity],
    [Pharmacy First MI & UMS Consultation Activity],
	[Oral Contraception Consultation Activity],
	[Appliance Use Reviews (AUR) Activity],
	[Stoma Customisation (STOMA) Activity],
	[Community Pharmacist Consultation Service (CPCS) Activity],
	[Medicine Use Review and Prescription Intervention Service (MUR) Activity],
	[Hepatitis C Antibody Testing Service Activity],
	[Covid Vaccination Service Activity],
	[Seasonal Influenza Vaccination Advances Service (FLU) Activity])
	) t
	) m
	group by  [FY_Qs],[Contractor], [Measure]"

result<-dbSendQuery(con,sql)
Activity<-dbFetch(result)
dbClearResult(result)


Activity<-Activity%>%
  mutate(FY_Qs=as.Date(FY_Qs))%>%
  left_join(Ref_Contractor_full[c("ContractorCode","IMD_Decile")], by = c("Contractor" = "ContractorCode"))


plot_activity_IMD<-function(measure="Items Dispensed"){
  
  data<-Activity%>%
    filter(`Measure`==measure)%>%
    group_by(FY_Qs, IMD_Decile)%>%
    summarise(Figure=sum(Figure, na.rm=T))%>%
    filter(!is.na(FY_Qs))%>%
    filter(!is.na(IMD_Decile))

  data[data == 0] <- NA
  
  title1 <- paste0(  "Total number of ", measure, " by pharmacies in each IMD Decile (IMD2019)")
  
  
  data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
  
  p1<- ggplot(data, aes(x = `FY_Qs` , y = `Figure`, color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme_bw() +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(min(data$`Figure`)-10, max(data$`Figure`)+10)) +
    theme(axis.text.x=element_text(angle=90,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~IMD_Decile, ncol = 5)+xlab("")+ 
    labs(title = str_wrap(title1, 90), y = paste0("Total ", measure), x="BSA claiming month")
  
  p1
}

plot_activity_IMD_avg<-function(measure="Items Dispensed"){
  
  data<-Activity%>%
    filter(`Measure`==measure)%>%
    group_by(FY_Qs, IMD_Decile)%>%
    summarise(Figure=mean(Figure, na.rm=T))%>%
    filter(!is.na(FY_Qs))%>%
    filter(!is.na(IMD_Decile))
  
 data[data == 0] <- NA
  
  
  title1 <- paste0(  "Average ", measure, " per pharmacy in each IMD Decile (IMD2019)")
  
  
  data$`IMD_Decile`<- factor( data$`IMD_Decile`, levels = c("1", "2", "3","4", "5","6", "7", "8", "9", "10"))
  
  p1<- ggplot(data, aes(x = `FY_Qs` , y = `Figure`, color=`IMD_Decile`)) +
    geom_line()+
    geom_point()+
    scale_x_date(date_breaks = "6 month", date_labels = "%b-%y",limits = c(min_date, max_date))+ 
    theme_bw() +
    scale_y_continuous(label = scales::comma,
                       breaks = scales::pretty_breaks(), limits = c(min(data$`Figure`)-10, max(data$`Figure`)+10)) +
    theme(axis.text.x=element_text(angle=90,hjust=0.5,vjust=0.5)) + 
    facet_wrap(~IMD_Decile, ncol = 5)+xlab("")+ 
    labs(title = str_wrap(title1, 90), y = "Average per pharmacy", x="BSA claiming month")
  
  p1
}
