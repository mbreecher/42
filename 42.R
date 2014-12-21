#the answer to life, the universe and everything
library(plyr)
library(reshape2)

setwd("C:/R/workspace/shared")
source("import_functions.r")
source("transformations.r")

#////////////////////////////////
# Billable Hours by xbrl status
#////////////////////////////////

setwd("C:/R/workspace/shared")
source("monthly_time.R")

collapsed_weekly <- timelog_with_status()
billable <- aggregate(Hours ~ monthyear +  xbrl_status + Billable + form_type, data = collapsed_weekly, FUN = sum)
billable <- billable[billable$Billable %in% 1,]
billable <- billable[order(billable$monthyear),] #sort
agg_billable <- aggregate(Hours ~ monthyear + xbrl_status + form_type, data = billable, FUN = sum) #aggregate by type

#cast wide to prepare for rbind
billable_hours <- dcast(agg_billable, xbrl_status + form_type ~ monthyear, sum, value.var = "Hours")

#////////////////////////////////
# Flat Fee Hours by service level
#////////////////////////////////

# setwd("C:/R/workspace/shared")
# source("monthly_time.R")
# 
# collapsed_weekly <- timelog_with_status()
project_time <- aggregate(Hours ~ monthyear +  Service.Type + Billable + Form.Type, data = collapsed_weekly, FUN = sum)
project_time <- project_time[project_time$Billable %in% 0,]
project_time <- project_time[order(project_time$monthyear),] #sort
agg_project <- aggregate(Hours ~ monthyear + Service.Type + Form.Type, data = project_time, FUN = sum) #aggregate by type

#cast wide to prepare for rbind
project_hours <- dcast(agg_project, Service.Type + Form.Type ~ monthyear, sum, value.var = "Hours")

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
#cast wide to prepare for rbind
scheduled_services <- dcast(scheduled_by_month, Service.Name ~ monthyear, sum, value.var = "Services.ID")