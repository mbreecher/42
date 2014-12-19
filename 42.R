#the answer to life, the universe and everything
library(plyr)
library(reshape2)

setwd("C:/R/workspace/shared")
source("import_functions.r")
source("transformations.r")

#////////////////////////////////
# Billable Hours by service level
#////////////////////////////////

collapsed_time <- collapsed_time()






#////////////////////////////////
# scheduled services by month
#////////////////////////////////

services <- import_services()
services <- services[services$filing.estimate >= "2013-06-30",]
services$monthyear <- format(services$filing.estimate, format = "%y-%m")

scheduled_by_month <- aggregate(Services.ID ~ monthyear + Service.Type + Form.Type, data = services, FUN = length)
scheduled_by_month$Service.Name <- paste(scheduled_by_month$Form.Type, scheduled_by_month$Service.Type, sep = " ")
approved_groups <- c("10-K Standard Import","10-Q Standard Import","10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Basic Maintentance","10-Q Basic Maintentance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward")
scheduled_by_month[!scheduled_by_month$Service.Name %in% approved_groups,]$Service.Name <- "Other Services"
scheduled_by_month <- scheduled_by_month[order(scheduled_by_month$monthyear),]
#cast wide to setup rbind
scheduled_services <- dcast(scheduled_by_month, Service.Name ~ monthyear, sum, value.var = "Services.ID")

model_time_ps <- dcast(agg_time_model, service_type + form ~ relative_week, sum, value.var = "psm_time")