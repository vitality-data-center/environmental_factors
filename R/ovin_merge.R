install.packages("dplyr")    # alternative installation of the %>%
install.packages("sjlabelled")
library(magrittr) # need to run every time you start R and want to use %>%
library(dplyr)    # alternative, this also loads %>%
library(sjlabelled)
ovin.2015 <- sjlabelled::read_spss("J:/nl_data/ovin_2015/OViN2015_Databestand.sav", verbose = FALSE)%>%
select(OPID, VertPC, VerplID, MotiefV, KHvm, Reisduur, AfstV, Rit, RitNr,  RitID, AfstR, Rvm, KRvm, RReisduur, Geslacht, KLeeft, Opleiding, HHSam, HHGestInk, HHAuto, Rijbewijs, Herkomst) %>%
  mutate(year=2015)

cat("ovin 2015 number = ", nrow(ovin.2015))


ovin.2016 <- sjlabelled::read_spss("J:/nl_data/ovin_2016/OViN2016_Databestand.sav", verbose = FALSE)%>%
select(OPID, VertPC, VerplID, MotiefV, KHvm, Reisduur, AfstV, Rit, RitNr,  RitID, AfstR, Rvm, KRvm, RReisduur, Geslacht, KLeeft, Opleiding, HHSam, HHGestInk, HHAuto, Rijbewijs, Herkomst) %>%
mutate(year=2016)

cat("ovin 2016 number = ", nrow(ovin.2016))

#print(ovin.2015[ovin.2015[, "OPID"] == 15102036 & ovin.2015[, "VertPC"] == '0000',])
ovin_merged <- rbind(ovin.2015, ovin.2016)%>%
mutate(Herkomst = case_when(Herkomst == 1 ~ 1, # Ethnicity
                            Herkomst %in% c(2,3) ~ 2,
                            Herkomst == 4 ~ NA_real_)) %>%
set_labels(Herkomst, labels = c("Dutch" = 1, "Non-Dutch" = 2)) %>%
filter(!is.na(Herkomst)) %>%
mutate(HHSam = case_when(HHSam == 1 ~ 1, # Household composition 
                         HHSam == 2 ~ 2,
                         HHSam %in% c(3,4) ~ 3,
                         HHSam %in% c(6,7) ~ 4,
                         HHSam %in% c(5,8) ~ 5)) %>%
set_labels(HHSam, labels = c("Single-person household" = 1, "Couple without children" = 2,
                              "Couple with children" = 3, "Single parent with children" = 4,
                              "Other compositions" = 5)) %>%
mutate(KLeeft = case_when(KLeeft %in% c(1,2,3,4,5) ~ 1, # Age groups 
                          KLeeft %in% c(6,7) ~ 2,
                          KLeeft %in% c(8,9) ~ 3,
                          KLeeft %in% c(10,11) ~ 4,
                          KLeeft %in% c(12,13) ~ 5,
                          KLeeft %in% c(14,15) ~ 6,
                          KLeeft %in% c(16,17) ~ 7,
                          KLeeft == 18 ~ 8)) %>%
set_labels(KLeeft, labels = c("0-19" = 1, "20-29" = 2, "30-39" = 3, "40-49" = 4, "50-59" = 5,
                                "60-69" = 6, "70-79" = 7, "80 and older" = 8)) %>%
mutate(Opleiding = case_when(Opleiding %in% c(0,1,2) ~ 1, # Education
                              Opleiding == 3 ~ 2,
                              Opleiding == 4 ~ 3,
                              Opleiding %in% c(5,6,7) ~ 4)) %>%
set_labels(Opleiding, labels = c("Low" = 1, "Medium" = 2, "High" = 3, "Other" = 4)) %>%
mutate(HHGestInk = case_when(HHGestInk %in% c(1,2) ~ 1, # Income Groups
                             HHGestInk %in% c(3,4) ~ 2,
                             HHGestInk %in% c(5,6) ~ 3,
                             HHGestInk == 7 ~ NA_real_)) %>%
set_labels(HHGestInk, labels = c("< €20.000" = 1, "€20.000-40.000" = 2, "> €40.000" = 3)) %>%
filter(!is.na(HHGestInk)) %>%
mutate(HHAuto = case_when(HHAuto == 0 ~ 0, # Car ownership
                            HHAuto == 1 ~ 1,
                            HHAuto %in% c(2,3,4,5,6,7,8,9) ~ 2,
                            HHAuto == 10 ~ NA_real_)) %>%
set_labels(HHAuto, labels = c("No car" = 0, "1 car" = 1, "2 or more cars" = 2)) %>%
filter(!is.na(HHAuto))%>%
filter(VertPC != '0') %>%
filter(VertPC != '0000')%>%
filter(MotiefV == 9 | MotiefV == 10 | MotiefV == 11)%>%
filter(KHvm == 7 &  KRvm == 7)


ovin_by_opid <- ovin_merged %>% 
group_by(OPID, year) %>%
summarise(VertPC  = paste(VertPC , collapse = ","), KLeeft = first(KLeeft), Geslacht = first(Geslacht), HHGestInk = first(HHGestInk), Opleiding = first(Opleiding), HHSam = first(HHSam), Herkomst = first(Herkomst), HHAuto=last(HHAuto), RReisduur = sum(RReisduur))%>%
filter(RReisduur<=400)  # remove outlier

#print(head(ovin_by_opid, 10))
# remove duplicate values
ovin_by_opid$VertPC  <-  sapply(strsplit(as.character(ovin_by_opid$VertPC), split=","),
                    function(x) paste(unique(x), collapse = ', ')) 
ovin_by_opid <- ovin_by_opid  %>% 
filter(sapply(strsplit(ovin_by_opid$VertPC, " "), length) == 1 ) %>%
rename(pc4 = VertPC) %>%
group_by(pc4) %>%
filter(n()>=4)%>% # only inlcude pc4 that has more than 4 obs
ungroup()
# mutate(n.ride = sapply(strsplit(ovin_by_opid$VertPC, " "), length)) %>% # How many vertPC 
# filter(n.ride == 1)# Only vertPC which appear once

#print(mode(ovin_by_opid$VertPC))
print(head(ovin_by_opid, 10))
write.csv(ovin_by_opid,'J:\\vdc_workspace\\vdc\\output\\ovin_final.csv', row.names = FALSE)
