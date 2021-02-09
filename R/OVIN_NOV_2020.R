## file last revised 29th October 2020
##### merging OVIN and walkability data ####

library(rJava)
library(xlsx) #READ xlsx files
library(dplyr) # making dataset
library(sjlabelled) #labelling data
library(table1) #making table 1

## read in ovin 2017 PC6 (outcome) data
ovin_full <- read.csv("INSERTDIRECTORY/OVIN_2017_PC6.csv", header = T, sep = ";", dec = ".", quote = "\"", stringsAsFactors = F, na.strings = "#NULL!")

## read in walkability 2018 (exposure) data
walk16 <- read.csv("INSERTDIRECTORY/walk_pc6_oct20.csv", header = T, sep = ";", dec = ".")

## removing people that DO NOT move on the sample date at all 
ovin_full <- ovin_full[!(ovin_full$Weggeweest==0),] #change from 115161 -> 107631 obs (from PC4 dataset) 

## I don't know what home PC6 are called in your OVIN dataset, so please insert them here 
ovin_full$homePC6 <- #inserthomePC variable here
ovin_full$homePC4 <- as.numeric(gsub("([0-9]+).*$", "\\1", ovin_full$homePC6)) #also create PC4 to merge with SES data

## removing trips made to/from outside the NL or with missing PC data
ovin_full <- ovin_full[!is.na(ovin_full$homePC6), ] #people who dont have homePC6 values (I doubt there will be but just in case)

ovin_full <- ovin_full[!is.na(ovin_full$VertPC), ] #trips with missing departure/ arrival PC (mostly because I don't have homePC in the PC4 dataset here, but can be skipped here? not sure if it makes a lot of difference)
ovin_full <- ovin_full[!is.na(ovin_full$AankPC), ]
ovin_full <- ovin_full[!(ovin_full$AankPC==0),]      #crossing borders
ovin_full <- ovin_full[!(ovin_full$AankPC=="0000"),] #crossing borders
ovin_full <- ovin_full[!(ovin_full$VertPC==0),]      #crossing borders
ovin_full <- ovin_full[!(ovin_full$VertPC=="0000"),] #crossing borders

## remove participants younger than 18 and older than 65 
ovin_full <- filter(ovin_full, Leeftijd > 17 & Leeftijd < 66) ## 62653 observations left (from pc4 dataset)
## new variable for whether OP also uses bike on sample date
ovin_full$bike_use <- ifelse(ovin_full$KRvm == 6, 1, 0) 

# calculate person-level outcome data
ovin_mean <- ovin_full %>% 
  select(OPID, Geslacht, Leeftijd, Opleiding, MaatsPart, Herkomst, homePC4, homePC6,
         HHSam, HHGestInk, HHAuto, Mode,  Sted, Weekdag, Week, Vertrekp, VerplNr, Doel,    
         ReisduurOP, AfstandOP, Vertrekp, VertPC, AankPC, KMotiefV, KRvm, AfstR, RReisduur, bike_use)  %>%
  group_by(OPID) %>% 
  #mutate(homePC6B      = ifelse(Vertrekp == 1, VertPC[VerplNr=1], ifelse((Vertrekp != 1 & Doel == 1), AankPC, NA))) %>% ## compose homePC6 in case of missing data
  
  ## transform purpose into discretionary vs non-discretionary, 1 is non-discretionary aka work- and study-related purposes. Groceries, shopping and leisure fall under discretionary purposes.
  mutate(motive      = ifelse((KMotiefV == 1 | KMotiefV == 2 | KMotiefV == 3 | KMotiefV == 5), 1, 0)) %>% 
  
  mutate(dist_nondis = sum(AfstR[motive == 1 & KRvm == 7 & (VertPC == homePC6 | AankPC == homePC6)], na.rm=T)) %>%  ## distance walking PER DAY for non-discretionary purposes
  mutate(dist_dis    = sum(AfstR[motive == 0 & KRvm == 7 & (VertPC == homePC6 | AankPC == homePC6)], na.rm=T)) %>%    ## distance walking PER DAY for discretionary purposes
  
  mutate(time_nondis = sum(RReisduur[motive == 1 & KRvm == 7 & (VertPC == homePC6 | AankPC == homePC6)], na.rm=T)) %>%   ## time spent walking PER DAY for non-discretionary purposes
  mutate(time_dis    = sum(RReisduur[motive == 0 & KRvm == 7 & (VertPC == homePC6 | AankPC == homePC6)], na.rm=T)) %>% ## time spent walking PER DAY for discretionary purposes
  
  mutate(total_timewalk = time_nondis + time_dis) %>%  ## total time spent walking per day, in minutes
  mutate(total_distwalk = dist_nondis + dist_dis)   %>%    ## total distance walked per day, in hectometers
  
  mutate(total_timebike = sum(RReisduur[KRvm == 6 & (VertPC == homePC6 | AankPC == homePC6)], na.rm=T)) %>%  ## time spent biking in minutes
  mutate(total_distbike = sum(AfstR[KRvm == 6 & (VertPC == homePC6 | AankPC == homePC6)], na.rm=T))  %>%  ## distance biked in hectometers
  
  mutate(total_timeactive = (total_timebike + total_timewalk))  %>%  #total time biking + walking
  mutate(total_distactive = (total_distbike + total_distwalk))  %>%  #total distance biked + walked
  
  set_labels(Geslacht, labels = c("Male" = 1, "Female"= 2)) %>%
  set_labels(Herkomst, labels = c("White Dutch" = 1, "Other Western"= 2, "Non-western" = 3)) %>%
  set_labels(Mode, labels = c("CAWI"= 1, "CATI"=2, "CAPI" = 3)) %>%
  
  mutate(Urban = case_when(Sted == 1 ~ 1, 
                           Sted %in% c(2,3) ~ 2,
                           Sted %in% c(4,5) ~ 3)) %>%
  set_labels(Urban, labels = c("<500 address/km2" = 1, "500 - 1500" = 2, "<1500 address/km2" = 3)) %>%
  mutate(Season = case_when(Week %in% (12:24) ~ 1,
                            Week %in% (25:39) ~2, #"summer"
                            Week %in% (40:51) ~3, #"fall" 
                            TRUE ~ 4)) %>% #"winter" 
  set_labels(Season, labels = c("Spring" = 1, "Summer" = 2, "Autumn" = 3, "Winter" = 4)) %>%
  mutate(Agegroup = case_when(Leeftijd %in% c(18:35) ~ 1, #young 
                              Leeftijd %in% c(36:49) ~ 2, #middle
                              Leeftijd %in% c(50:65) ~ 3, #older
                              TRUE ~ NA_real_))%>% #missing
  set_labels(Agegroup, labels = c("18 - 35" = 1, "36 - 49" = 2, "50 - 65" = 3)) %>%
  mutate(Edu = case_when(Opleiding %in% c(0,1) ~ 1, #low
                         Opleiding %in% c(2,3) ~ 2, #Medium
                         Opleiding %in% c(4) ~ 3,   #high
                         Opleiding %in% c(5,6,7) ~ NA_real_)) %>% #missing or others
  set_labels(Edu, labels = c("Low" = 1, "Medium" = 2, "High" = 3)) %>%
  mutate(Work = case_when(MaatsPart %in% c(1) ~ 1, #work part time
                          MaatsPart %in% c(2) ~ 2, #work full time
                          MaatsPart %in% c(4) ~ 3, #student
                          MaatsPart %in% c(3, 5, 6, 8) ~ 4, #not working
                          MaatsPart %in% c(9, 10) ~ NA_real_)) %>% #missing, unknown or others
  set_labels(Season, labels = c("Work part-time" = 1, "Work fulltime" = 2, "Student" = 3, "Not working" = 4)) %>%
  mutate(HHAuto = case_when(HHAuto == 0 ~ 0, # no car in household
                            HHAuto == 1 ~ 1,
                            HHAuto %in% c(2,3,4,5,6,7,8,9) ~ 2,
                            HHAuto == 10 ~ NA_real_)) %>%
  set_labels(HHAuto, labels = c("No car in household" = 0, "One car in household" = 1, "Two or more cars" = 2)) %>%
  mutate(Income = case_when(HHGestInk %in% c(1,2) ~ 1, #low
                            HHGestInk %in% c(3) ~ 2, #Medium
                            HHGestInk %in% c(4,5,6) ~ 3,   #high
                            HHGestInk %in% c(7) ~ NA_real_)) %>% #missing
  set_labels(Income, labels = c("Low" = 1, "Medium" = 2, "High" = 3)) %>%
  mutate(HHSam = case_when(HHSam == 1 ~ 1, # Household composition 
                           HHSam == 2 ~ 2,
                           HHSam %in% c(3,4) ~ 3,
                           HHSam %in% c(6,7) ~ 4,
                           HHSam %in% c(5,8) ~ 5)) %>%
  set_labels(HHSam, labels = c("Single-person household" = 1, "Couple without children" = 2,
                               "Couple with children" = 3, "Single parent with children" = 4,
                               "Other compositions" = 5))  %>%
  mutate(Weekdag = case_when(Weekdag %in% c(1,7) ~ 0, #weekend
                             Weekdag %in% c(2:6) ~ 1)) %>% #weekday
  set_labels(Weekdag, labels = c("Weekend" = 0, "Weekday" = 1)) 

#now only selecting 1 observation per participant 
ovin_personal <- distinct(ovin_mean, OPID, .keep_all = T) 

#merging with walkability data
ovin_merged <- merge(ovin_personal, Walk16, by.x = "HomePC6", by.y = "PC6", all.x = TRUE, all.y = FALSE) #merging exposure and outcome data

#or manual merging of interested walkability indices in case the merge above is not successful/ too messy
#ovin_personal$SES <- walk16$SES[match(as.numeric(ovin_personal$homePC4), as.numeric(walk16$PC4))] 
#ovin_personal$walk18_pc6_nbh <- walk16$walk18_pc6_nbh[match(as.numeric(ovin_personal$homePC6), as.numeric(walk16$PC6))]
#ovin_personal$walk18_pc6_150 <- walk16$walk18_pc6_150[match(as.numeric(ovin_personal$homePC6), as.numeric(walk16$PC6))]
#ovin_personal$walk18_pc6_500 <- walk16$walk18_pc6_500[match(as.numeric(ovin_personal$homePC6), as.numeric(walk16$PC6))]
#ovin_personal$walk18_pc6_1000 <- walk16$walk18_pc6_1000[match(as.numeric(ovin_personal$homePC6), as.numeric(walk16$PC6))]
#ovin_merged <- ovin_personal

### making of Table 1: population characteristics by quintiles of neighborhood walkability ####

#make quintiles of walkability for table 1
ovin_merged <- within(ovin_personal, HomeWalkq <- 
                        as.integer(cut(walk18_pc6_nbh, quantile(walk18_pc6_nbh, probs = seq(0, 1, 0.2), include.lowest=TRUE, na.rm=T)))) 

# (somehow the dyplr labelling wont work so I have to rename them here)
ovin_merged$Geslacht <- factor(ovin_personal$Geslacht, levels=c(1,2), labels=c("Male", "Female"))
ovin_merged$Agegroup <- factor(ovin_personal$Agegroup, levels=c(1,2,3), labels= c("18 - 35", "36 - 49", "50 - 65"))
ovin_merged$Herkomst <- factor(ovin_personal$Herkomst, levels=c(1,2,3), labels=c("White Dutch", "Other Western", "Non-western"))
ovin_merged$Edu      <- factor(ovin_personal$Edu, levels = c(1,2,3), labels=c("Low", "Medium", "High"))
ovin_merged$Work     <- factor(ovin_personal$Work, levels= c(1,2,3,4), labels=c("Work part-time", "Work fulltime", "Student", "Not working"))  
ovin_merged$Income   <- factor(ovin_personal$Income, levels=c(1,2,3), labels=c("Low", "Medium", "High"))
ovin_merged$HHAuto   <- factor(ovin_personal$HHAuto, levels=c(0,1,2), labels=c("No car in household", "One car in household", "Two or more cars"))
ovin_merged$HHSam    <- factor(ovin_personal$HHSam, levels=c(1,2,3,4,5), labels=c("Single-person household", "Couple without children",
                                                                                  "Couple with children", "Single parent with children", "Other compositions"))
ovin_merged$Season   <- factor(ovin_personal$Season, levels=c(1,2,3,4), labels=c("Spring", "Summer", "Autumn", "Winter"))
ovin_merged$Urban    <- factor(ovin_personal$Urban, levels =c(1,2,3), labels=c("<500 addresses/km2", "500 - 1500", ">1500"))
ovin_merged$Weekdag  <- factor(ovin_personal$Weekdag, levels = c(0,1), labels=c("Weekend", "Weekday"))
ovin_merged$Mode     <- factor(ovin_personal$Mode, levels = c(1,2,3), labels=c("Internet", "Telephone", "Face-to-face"))
ovin_merged$bike_use <- factor(ovin_personal$bike_use, levels = c(0,1), labels=c("No","Yes"))

label(ovin_merged$Geslacht) <- "Sex"
label(ovin_merged$Agegroup) <- "Age group"
label(ovin_merged$Herkomst) <- "Background"
label(ovin_merged$Edu) <- "Highest education obtained"
label(ovin_merged$Work) <- "Working status"
label(ovin_merged$Income) <- "Standardized household income"
label(ovin_merged$HHAuto) <- "Household cars"
label(ovin_merged$HHSam) <- "Household situation"
label(ovin_merged$Season) <- "Season"
label(ovin_merged$Urban) <- "Urbanization degree"
label(ovin_merged$total_timewalk) <- "Time spent walking, minutes"
label(ovin_merged$total_distwalk) <- "Distance walked, x100 meters"
label(ovin_merged$SES) <- "Socioeconomic status score neighborhood"
label(ovin_merged$Weekdag) <- "Day of the week"
label(ovin_mergedl$Mode) <- "Response type"
label(ovin_merged$bike_use) <- "Participants also bike on the same day"
label(ovin_merged$walk18_pc6_nbh) <- "Neighborhood walkability around the home"
label(ovin_merged$total_timebike) <- "Time spent biking, minutes"
label(ovin_merged$total_distbike) <- "Distance biked, x100 meters"
label(ovin_merged$total_timeactive) <- "Time spent biking & walking, minutes"
label(ovin_merged$total_distactive) <- "Distance biked & walked, x100 meters"

## optional: write table
#write.table(ovin_merged, file="INSERTDIRECTORY/ovin_walkability_PC6.csv", sep = ";", dec = ".")

#### TOBIT MODELLING ####

library(survival) 
library(censReg) #tobit modelling

# read in data again if needed
#ovin_merged <- read.csv("INSERTDIRECTORY/ovin_walkability_PC6.csv", header = T, sep = ";", dec = ".")

ovin_merged <- na.omit(ovin_merged, cols=c("homePC6")) #complete case analysis for walking

#Table1: only complete cases wrt home PC6
table1(~ as.factor(Geslacht) + as.factor(Agegroup) + as.factor(Herkomst) + as.factor(Edu) + as.factor(Work) + as.factor(Income) + as.factor(HHAuto) + as.factor(HHSam) + as.factor(Season) + as.factor(Urban) + as.factor(Weekdag) + as.factor(Mode) + as.factor(bike_use) 
       + SES + walk18_pc6_nbh + total_timewalk + total_distwalk + total_timebike + total_distbike + total_timeactive + total_distactive | factor(HomeWalkq), data = ovin_merged, label=T)

#manual tobit modelling, mostly for checking if looping works
model_1_cens <- censReg(formula = total_timewalk ~ walk18_pc6_nbh, data= ovin_merged)
summary(model_1_cens)
model_2_cens <- censReg(formula = total_timewalk ~ walk18_pc6_150 + Agegroup + Geslacht + Herkomst + Edu + Work + Income + HHAuto + HHSam + SES + Season + Weekdag + Mode, data= ovin_merged) 
summary(model_2_cens)
print(summary(model_2_cens), digits=2)
model_3_cens <- censReg(formula = total_timewalk ~ walk18_pc6_500 + Agegroup + Geslacht + Herkomst + Edu + Work + Income + HHAuto + HHSam + SES + Season + Weekdag + Mode + bike_use, data= ovin_merged)
summary(model_3_cens)

#creating loops for models 1 2 and 3
dep_vars <- c("total_timewalk", "total_distwalk", "time_dis", "time_nondis", "dist_dis", "dist_nondis") #"total_timebike", "total_distbike", "total_timeactive", "total_distactive"
ind_vars <- c("walk18_pc6_nbh", "walk18_pc6_150", "walk18_pc6_500", "walk18_pc6_1000") #can be extended later to analyze individual components

#model 1: only walkability and outcome
tobit_model1 <- lapply(ind_vars, function(h){
  f <- h
  unlist(lapply(dep_vars, function(g){
    k <- formula(paste(g, f, sep ="~"))
    m <- censReg(formula = k, data = ovin_merged) 
    estimate <- round(m$estimate[2], 2)
    lower <- round(confint(m)[2,1],2)
    upper <- round(confint(m)[2,2],2)
    (sprintf("%s (%s, %s)", estimate, lower, upper))
  }))})
output_model1 <- do.call(rbind, tobit_model1)
colnames(output_model1) <- dep_vars 
rownames(output_model1) <- ind_vars
output_model1 #print out models 1 for all walkability indices and all walking outcomes

#model 2: adjusted for extra confounders
#tobit_model2 <- lapply(ind_vars, function(h){
#  f <- sprintf("%s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s", h, "Agegroup", "Geslacht" , "Herkomst",  "Edu",  "Work", "Income", "HHAuto", "HHSam", "SES", "Season", "Weekdag", "Mode") #Model 2
#  unlist(lapply(dep_vars, function(g){
#    k <- formula(paste(g, f, sep ="~"))
#    m <- censReg(formula = k, data = ovin_merged) 
#    estimate <- round(m$estimate[2], 2)
#    lower <- round(confint(m)[2,1],2)
#    upper <- round(confint(m)[2,2],2)
#    (sprintf("%s (%s, %s)", estimate, lower, upper))
#  }))})
#output_model2 <- do.call(rbind, tobit_model2)
#colnames(output_model2) <- dep_vars 
#rownames(output_model2) <- ind_vars
#output_model2 #print out models 2 for all walkability indices and all walking outcomes

#model 3: fully adjusted models
tobit_model3 <- lapply(ind_vars, function(h){
  f <- sprintf("%s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s + %s", h, "Agegroup", "Geslacht" , "Herkomst",  "Edu",  "Work", "Income", "HHAuto", "HHSam", "SES", "Season", "Weekdag", "Mode", "bike_use")  #Model 3
  unlist(lapply(dep_vars, function(g){
    k <- formula(paste(g, f, sep ="~"))
    m <- censReg(formula = k, data = ovin_merged) #updating the data
    estimate <- round(m$estimate[2], 2)
    lower <- round(confint(m)[2,1],2)
    upper <- round(confint(m)[2,2],2)
    (sprintf("%s (%s, %s)", estimate, lower, upper))
  }))})
output_model3 <- do.call(rbind, tobit_model3)
colnames(output_model3) <- dep_vars 
rownames(output_model3) <- ind_vars
output_model3 #print out models 3 for all walkability indices and all walking outcomes