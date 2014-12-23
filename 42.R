#the answer to life, the universe and everything
#dependencies:
#timelog_for_r
#subcloud_for_r
#role_dates
#accounts_with_year_end
library(plyr)
library(reshape2)
library(xlsx)

setwd("C:/R/workspace/shared")
source("import_functions.r")
source("transformations.r")

#////////////////////////////////
# Billable Hours by xbrl status
#////////////////////////////////

setwd("C:/R/workspace/shared")
source("monthly_time.R")

collapsed_monthly <- timelog_with_status()
billable <- aggregate(Hours ~ monthyear +  xbrl_status + Billable + form_type, data = collapsed_monthly, FUN = sum)
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
agg_project$header <- paste(agg_project$Form.Type, agg_project$Service.Type, sep = " ")
groups <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import","10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward")
agg_project[!(agg_project$header %in% groups),]$header <- "Other Services"

#cast wide to prepare for rbind
project_hours <- dcast(agg_project, header ~ monthyear, sum, value.var = "Hours")
project_hours <- project_hours[match(c(groups, "Other Services"),project_hours$header),]
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

#////////////////////////////////
# Full Time Employees - count
#////////////////////////////////
all_time <- aggregate(Hours ~ monthyear +  role + User , data = collapsed_monthly, FUN = sum)
all_time[all_time$User %in% "Jane Cavanaugh" & all_time$monthyear %in% c("14-09", "14-10", "14-11"),]$role <- "PSS"
#all_time[all_time$User %in% "Alissa Clausen",]$role <- "PSS"
count_by_role <- aggregate(Hours ~ monthyear + User + role , data = all_time, FUN = sum)
count_by_role <- ddply(count_by_role, .(monthyear, role), summarise, count = length(unique(User)))
count_by_role <- dcast(count_by_role, role ~ monthyear, sum, value.var = "count")

#////////////////////////////////
# Total client time by role
#////////////////////////////////
time_by_role <- aggregate(Hours ~ monthyear +  role , data = all_time, FUN = sum)
time_by_role <- time_by_role[order(time_by_role$monthyear),]
time_by_role <- dcast(time_by_role, role ~ monthyear, sum, value.var = "Hours") 
time_by_role <- time_by_role[time_by_role$role %in% c("PSM", "PSS", "Sr PSM"),]

#////////////////////////////////
# Total client time by role
#////////////////////////////////

#rbind results
#test <- rbind.fill(billable_hours, project_hours, scheduled_services, count_by_role, time_by_role)

#****************** write results to file
setwd("C:/R/workspace/42/output")
write.xlsx(x = billable_hours, file = "42_data.xlsx",sheetName = "billable_hours", row.names = FALSE)
write.xlsx(x = project_hours, file = "42_data.xlsx",sheetName = "project_hours", row.names = FALSE, append = TRUE)
write.xlsx(x = scheduled_services, file = "42_data.xlsx",sheetName = "scheduled_services", row.names = FALSE, append = TRUE)
write.xlsx(x = count_by_role, file = "42_data.xlsx",sheetName = "count_by_role", row.names = FALSE, append = TRUE)
write.xlsx(x = time_by_role, file = "42_data.xlsx",sheetName = "time_by_role", row.names = FALSE, append = TRUE)

