#the answer to life, the universe and everything
#dependencies:
#timelog_for_R.csv 
#services_for_ps_history_R.csv
#ps_start_dates.csv
#opportunities_for_R.csv
#accounts_with_year_end.csv
#contracts_for_pshistory.csv
#hierarchy.csv
start = proc.time() #expect ~16 minutes
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
setwd("C:/R/workspace/42")
source("helpers.R")

ptm <- proc.time()
timelog_with_status <- timelog_with_status() #~12 minutes
proc.time() - ptm
timelog_with_status <- timelog_with_status[order(timelog_with_status$Date),]
agg_billable <- aggregate(Hours ~ monthyear +  xbrl_status + form_type, 
                          data = timelog_with_status[timelog_with_status$Billable %in% 1,], FUN = sum)

#cast wide to prepare for rbind
billable_hours <- dcast(agg_billable, xbrl_status + form_type ~ monthyear, sum, value.var = "Hours")
names(billable_hours) <- monthyear_to_written(names(billable_hours))
row.names(billable_hours) <- paste(billable_hours$xbrl_status, billable_hours$form_type, sep = " - ")
name_order <- c("DIY - Q","DIY - K","Basic - Q","Basic - K","Full Service - Q","Full Service - K")
billable_hours <- billable_hours[match(name_order, row.names(billable_hours)),]
billable_hours <- billable_hours[,-c(1,2)]

#////////////////////////////////
# Flat Fee Hours by service level
#////////////////////////////////

project_time <- aggregate(Hours ~ monthyear +  Service.Type + Form.Type, 
                          data = timelog_with_status[timelog_with_status$Billable %in% 0 & !is.na(timelog_with_status$Hours),], FUN = sum)
project_time$header <- paste(project_time$Form.Type, project_time$Service.Type, sep = " ")
groups <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import","10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","Q-K Full Service Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward")
project_time[!(project_time$header %in% groups),]$header <- "Other Services"

#cast wide to prepare for rbind
project_hours <- dcast(project_time, header ~ monthyear, sum, value.var = "Hours")
project_hours <- project_hours[match(c(groups, "Other Services"),project_hours$header),]
names(project_hours) <- monthyear_to_written(names(project_hours))
row.names(project_hours) <- c(groups, "Other Services") #use rownames for service names rather than column
project_hours <- project_hours[,-1] #remove name column
project_hours[is.na(project_hours)] <- 0

#////////////////////////////////
# scheduled services by month
#////////////////////////////////

hierarchy <- import_hierarchy() #for some reason, not taking when imported within function
services <- import_services(output = 'expanded')

services <- services[services$filing.estimate > "2013-06-30",]
services$monthyear <- format(services$filing.estimate, format = "%y-%m")
services <- services[order(services$filing.estimate),]

scheduled_by_month <- aggregate(Services.ID ~ monthyear + Service.Type + Form.Type, data = services, FUN = length)
scheduled_by_month <- scheduled_by_month[!scheduled_by_month$Service.Type %in% c("Migration"),] #remove migrations
scheduled_by_month[scheduled_by_month$Service.Type %in% c("Full Service Roll Forward") & scheduled_by_month$Form.Type %in% c("Q-K", "K-K"),]$Form.Type <- "10-K"
scheduled_by_month$Service.Name <- paste(scheduled_by_month$Form.Type, scheduled_by_month$Service.Type, sep = " ")
name_order <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import", "10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward")
scheduled_by_month[!scheduled_by_month$Service.Name %in% name_order,]$Service.Name <- "Other Services"

#cast wide to prepare for rbind
scheduled_services <- dcast(scheduled_by_month, Service.Name ~ monthyear, sum, value.var = "Services.ID")
names(scheduled_services) <- monthyear_to_written(names(scheduled_services))
scheduled_services <- scheduled_services[match(c(name_order,"Other Services") , scheduled_services$Service.Name),]
row.names(scheduled_services) <- c(name_order, "Other Services") #use rownames for service names rather than column
scheduled_services <- scheduled_services[,-1] #remove name column
scheduled_services[is.na(scheduled_services)] <- 0


#////////////////////////////////
# Full Time Employees - count
#////////////////////////////////
all_time <- aggregate(Hours ~ monthyear +  role + User , data = timelog_with_status, FUN = sum)
all_time[all_time$User %in% "Jane Cavanaugh" & all_time$monthyear %in% c("14-09", "14-10", "14-11"),]$role <- "PSS"
#all_time[all_time$User %in% "Alissa Clausen",]$role <- "PSS"
count_by_role <- aggregate(Hours ~ monthyear + User + role , data = all_time, FUN = sum)
count_by_role <- ddply(count_by_role, .(monthyear, role), summarise, count = length(unique(User)))
count_by_role <- dcast(count_by_role, role ~ monthyear, sum, value.var = "count")
count_by_role <- count_by_role[count_by_role$role %in% c("PSM", "PSS", "Sr PSM"),]
names(count_by_role) <- monthyear_to_written(names(count_by_role))

#////////////////////////////////
# Total client time by role
#////////////////////////////////
time_by_role <- aggregate(Hours ~ monthyear +  role , data = all_time, FUN = sum)
time_by_role <- dcast(time_by_role, role ~ monthyear, sum, value.var = "Hours") 
time_by_role <- time_by_role[time_by_role$role %in% c("PSM", "PSS", "Sr PSM"),]
names(time_by_role) <- monthyear_to_written(names(time_by_role))

#////////////////////////////////
# Total # of XBRL registrants
# ps history number
#////////////////////////////////
#use application log to identify filings

app_data <- import_app_filing_data()
app_data <- app_data[!is.na(app_data$Fact.Cnt),]
app_data <- app_data[app_data$Form.Type %in% c("10-Q", "10-K", "10-K/A", "10-Q/A"),] #limit to form 10
app_data$software <- "WebFilings"
app_data_uniques <- unique(app_data[names(app_data) %in% c("Registrant.CIK", "monthyear", "software")])
app_data_wide <- dcast(app_data_uniques, software ~ monthyear, length, value.var = "Registrant.CIK")
names(app_data_wide) <- monthyear_to_written(names(app_data_wide))
app_data_wide

#////////////////////////////////
# xbrl customers
#////////////////////////////////
customers <- unique(app_data[names(app_data) %in% c("Company.Name", "monthyear", "software")])
customers_wide <- dcast(customers, software ~ monthyear, length, value.var = "Company.Name")
names(customers_wide) <- monthyear_to_written(names(customers_wide))
customers_wide

customer_counts <- rbind(app_data_wide, customers_wide)
row.names(customer_counts) <- c("Registrants", "Customers")
customer_counts <- customer_counts[,-1]

#////////////////////////////////
# filing counts
#////////////////////////////////

# filings during the month... maybe by type
filings_wide <- dcast(app_data, Form.Type ~ monthyear, length, value.var = "Accession.Number")
row.names(filings_wide) <- filings_wide$Form.Type
filings_wide <- filings_wide[,-1]
names(filings_wide) <- monthyear_to_written(names(filings_wide))
# number of true diys that filed by month

#////////////////////////////////
# combine filing and customer count info for filing and customer data tab
#////////////////////////////////
space <- rep("", dim(customer_counts)[1])
filing_and_customer <- rbind(customer_counts, space, filings_wide)
row.names(filing_and_customer) <- c(row.names(customer_counts), "", row.names(filings_wide))

#////////////////////////////////
# Net discounted sales price
#////////////////////////////////
collapsed_opps <- collapsed_opportunities() # ~2.75 minutes
collapsed_opps <- collapsed_opps[order(collapsed_opps$filing.estimate),]

#make list price sales price if list price == 0 or na
collapsed_opps[collapsed_opps$List.Price %in% 0 | is.na(collapsed_opps$List.Price),]$List.Price <- collapsed_opps[collapsed_opps$List.Price %in% 0  | is.na(collapsed_opps$List.Price),]$Sales.Price
#make list price sales price if sales price > list
collapsed_opps[collapsed_opps$List.Price < collapsed_opps$Sales.Price,]$List.Price <- collapsed_opps[collapsed_opps$List.Price < collapsed_opps$Sales.Price,]$Sales.Price

#set discount percentages by item
collapsed_opps$discount <- 1 #instantiate field with full discount
collapsed_opps[!collapsed_opps$Sales.Price %in% 0,]$discount <- 1 - (collapsed_opps[!collapsed_opps$Sales.Price %in% 0,]$Sales.Price / collapsed_opps[!collapsed_opps$Sales.Price %in% 0,]$List.Price)

sales_info <- aggregate(Sales.Price ~ monthyear + Service.Type + Form.Type, data = collapsed_opps, FUN = sum)
sales_info$header <- paste(sales_info$Form.Type, sales_info$Service.Type, sep = " ")
groups <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import","10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward")
sales_info[!(sales_info$header %in% groups),]$header <- "Other Services"

#cast wide to prepare for rbind
sales_info_wide <- dcast(sales_info, header ~ monthyear, sum, value.var = "Sales.Price")
sales_info_wide <- sales_info_wide[match(c(groups, "Other Services"),sales_info_wide$header),]
row.names(sales_info_wide) <- c(groups, "Other Services") #use rownames for service names rather than column
names(sales_info_wide) <- monthyear_to_written(names(sales_info_wide))
sales_info_wide <- sales_info_wide[,-1] #remove name column
sales_info_wide[is.na(sales_info_wide)] <- 0

#////////////////////////////////
# Discounted by range
#////////////////////////////////
collapsed_opps$discount.bucket <- "100% Discount" #instantiate with full discount
collapsed_opps[collapsed_opps$discount >= 0 & collapsed_opps$discount <= .01,]$discount.bucket <- "0-1% Discount"
collapsed_opps[collapsed_opps$discount > .01 & collapsed_opps$discount <= .2,]$discount.bucket <- "1.1%-20% Discount"
collapsed_opps[collapsed_opps$discount > .2 & collapsed_opps$discount <= .5,]$discount.bucket <- "20.1%-50% Discount"
collapsed_opps[collapsed_opps$discount > .5 & collapsed_opps$discount <= .75,]$discount.bucket <- "50.1%-75% Discount"
collapsed_opps[collapsed_opps$discount > .75 & collapsed_opps$discount < 1,]$discount.bucket <- "75.1%-99% Discount"

# QA check
# for(each in unique(collapsed_opps$discount.bucket)){
#   print(each)
#   print(min(collapsed_opps[collapsed_opps$discount.bucket %in% each,]$discount))
#   print(max(collapsed_opps[collapsed_opps$discount.bucket %in% each,]$discount))
# }

discount_groups <- aggregate(Services.ID ~ monthyear + discount.bucket, data = collapsed_opps, FUN = length)

#cast wide to prepare for rbind
discount_groups_wide <- dcast(discount_groups, discount.bucket ~ monthyear, sum, value.var = "Services.ID")
names(discount_groups_wide) <- monthyear_to_written(names(discount_groups_wide))
name_order <- c("100% Discount","75.1%-99% Discount","50.1%-75% Discount","20.1%-50% Discount","1.1%-20% Discount","0-1% Discount")
discount_groups_wide <- discount_groups_wide[match(name_order, discount_groups_wide$discount.bucket),]

#////////////////////////////////
# Discounted 100% by type
#////////////////////////////////

full_discount <- aggregate(Services.ID ~ monthyear + Service.Type, data = collapsed_opps[collapsed_opps$discount %in% 1,], FUN = length)

#cast wide to prepare for rbind
full_discount_wide <- dcast(full_discount, Service.Type ~ monthyear, sum, value.var = "Services.ID")
names(full_discount_wide) <- monthyear_to_written(names(full_discount_wide))

#////////////////////////////////
# Discounted 20% - 99% by type
#////////////////////////////////

discount_20_to_99 <- aggregate(Services.ID ~ monthyear + Service.Type, data = collapsed_opps[collapsed_opps$discount >.2 &  collapsed_opps$discount < 1,], FUN = length)

#cast wide to prepare for rbind
discount_20_to_99_wide <- dcast(discount_20_to_99, Service.Type ~ monthyear, sum, value.var = "Services.ID")
names(discount_20_to_99_wide) <- monthyear_to_written(names(discount_20_to_99_wide))

#////////////////////////////////
# Goodwill Hours used by month
#////////////////////////////////
wide_goodwill_used <- dcast(timelog_with_status[timelog_with_status$Service %in% c("Goodwill Hours"),],Service ~ monthyear, sum, value.var = "Hours" )
names(wide_goodwill_used) <- monthyear_to_written(names(wide_goodwill_used))

#////////////////////////////////
# Goodwill Balance
#////////////////////////////////
unique_customers <- unique(services[!is.na(services$Goodwill.Hours.Available),names(services) %in% c("Account.Name","Goodwill.Hours.Available" )])
goodwill_balance <- data.frame(date = Sys.Date(), goodwill_balance = sum(unique_customers$Goodwill.Hours.Available))

#****************** write results to file
setwd("C:/R/workspace/42/output")
write.xlsx(x = billable_hours, file = "42_data.xlsx",sheetName = "billable_hours", row.names = TRUE)
write.xlsx(x = project_hours, file = "42_data.xlsx",sheetName = "project_hours", row.names = TRUE, append = TRUE)
write.xlsx(x = scheduled_services, file = "42_data.xlsx",sheetName = "scheduled_services", row.names = TRUE, append = TRUE)
write.xlsx(x = count_by_role, file = "42_data.xlsx",sheetName = "count_by_role", row.names = FALSE, append = TRUE)
write.xlsx(x = time_by_role, file = "42_data.xlsx",sheetName = "time_by_role", row.names = FALSE, append = TRUE)
write.xlsx(x = filing_and_customer, file = "42_data.xlsx",sheetName = "customers_and_filings", row.names = TRUE, append = TRUE)
write.xlsx(x = sales_info_wide, file = "42_data.xlsx",sheetName = "net_sales", row.names = TRUE, append = TRUE)
write.xlsx(x = discount_groups_wide, file = "42_data.xlsx",sheetName = "services by discount", row.names = FALSE, append = TRUE)
write.xlsx(x = full_discount_wide, file = "42_data.xlsx",sheetName = "Full discount", row.names = FALSE, append = TRUE)
write.xlsx(x = discount_20_to_99_wide, file = "42_data.xlsx",sheetName = "20-99 discount", row.names = FALSE, append = TRUE)
write.xlsx(x = wide_goodwill_used, file = "42_data.xlsx",sheetName = "Goodwill Hours Used", row.names = FALSE, append = TRUE)
write.xlsx(x = goodwill_balance, file = "42_data.xlsx",sheetName = "Goodwill Balance", row.names = FALSE, append = TRUE)

proc.time() - start

#rbind results
#test <- rbind.fill(billable_hours, project_hours, scheduled_services, count_by_role, time_by_role)