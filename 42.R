#the answer to life, the universe and everything
#dependencies:
#time_entry_detail_report__complete_report.csv
#services_for_ps_history_R.csv
#ps_start_dates.csv
#opportunities_for_R.csv
#accounts_with_year_end.csv
#contracts_for_pshistory.csv
#hierarchy.csv
#churned_customers.csv
start = proc.time() #expect ~45 minutes
library(plyr)
library(reshape2)
library(xlsx)

setwd("C:/R/workspace/shared")
source("import_functions.r")
source("transformations.r")
source("helpers.R")

#////////////////////////////////
# Billable Hours by xbrl status
#////////////////////////////////

setwd("C:/R/workspace/shared")
source("time_by_interval.R")
setwd("C:/R/workspace/42")
source("helpers.R")

#all billable time

#initial timelog import with processing. Store info and bypass when no new info available
setwd("c:/r/workspace/source")
sf_timelog_data_age <- file.info('timelog_for_R.csv')$mtime
oa_timelog_data_age <- file.info('time_entry_detail_report__complete_report.csv')$mtime
setwd("c:/r/workspace/42/datastore")

if(file.info("timelog_with_status.Rda")$mtime > sf_timelog_data_age &
     file.info("timelog_with_status.Rda")$mtime > oa_timelog_data_age){
  print("no change in timelog data, loading historical info")
  print(paste("timelog_with_status.Rda", "last updated", round(difftime(Sys.time(), file.info("timelog_with_status.Rda")$mtime, units = "days"), digits = 1), "days ago", sep = " "))
  timelog_with_status_df <- readRDS(file = "timelog_with_status.Rda")
}else{
  ptm <- proc.time()
  print("updated timelog data available, processing...")
  timelog_with_status_df <- timelog_with_status(include_cs = T) #~12 minutes
  print(proc.time() - ptm)
  
  #manual processing of roles
  timelog_with_status_df[timelog_with_status_df$User %in% "Jane Cavanaugh" & timelog_with_status_df$monthyear %in% c("14-09", "14-10", "14-11"),]$role <- "PSS"
  timelog_with_status_df[timelog_with_status_df$User.Title %in% "Professional Services Intern",]$role <- "Intern"
  timelog_with_status_df[timelog_with_status_df$role %in% c("PSS", "Intern"),]$is_psm <- 1
  
  if(length(timelog_with_status_df[timelog_with_status_df$role %in% c("Sr. PSM"),]$role) < 0){
    timelog_with_status_df[timelog_with_status_df$role %in% c("Sr. PSM"),]$role <- "Sr PSM"
  }
  if(length(timelog_with_status_df[timelog_with_status_df$role %in% c("PSM Team Manager"),]$role) < 0){
    timelog_with_status_df[timelog_with_status_df$role %in% c("PSM Team Manager"),]$role <- "TM"  
  }
  
  
  timelog_with_status_df <- timelog_with_status_df[order(timelog_with_status_df$Date),]
  setwd("c:/r/workspace/42/datastore")
  saveRDS(timelog_with_status_df, file = "timelog_with_status.Rda")
  print(paste("timelog_with_status.Rda", "last updated", round(difftime(Sys.time(), file.info("timelog_with_status.Rda")$mtime, units = "days"), digits = 1), "days ago", sep = " "))
}

#peel off and remove non-ps time
non_ps_time <- timelog_with_status_df[timelog_with_status_df$is_psm %in% 0 | is.na(timelog_with_status_df$is_psm),] #grab 0s and NAs
agg_non_ps <- aggregate(Hours ~ monthyear + Service.Type, data = non_ps_time[non_ps_time$Billable %in% 1,], FUN = sum)
agg_non_ps[agg_non_ps$Service.Type %in% "",]$Service.Type <- "Other"

agg_billable <- aggregate(Hours ~ monthyear +  xbrl_status + form_type, 
                          data = timelog_with_status_df[timelog_with_status_df$Billable %in% 1 &
                                                          timelog_with_status_df$is_psm %in% 1,], FUN = sum)

billable_hours <- dcast(agg_billable, xbrl_status + form_type ~ monthyear, sum, value.var = "Hours")
names(billable_hours) <- monthyear_to_written(names(billable_hours))
row.names(billable_hours) <- paste(billable_hours$xbrl_status, billable_hours$form_type, sep = " - ")
name_order <- c("DIY - Q","DIY - K","Basic - Q","Basic - K","Full Service - Q","Full Service - K")
billable_hours <- billable_hours[match(name_order, row.names(billable_hours)),]
billable_hours <- billable_hours[,-c(1,2)]

cs_hours_wide <- dcast(agg_non_ps, Service.Type ~ monthyear, sum, value.var = "Hours")
row.names(cs_hours_wide) <- cs_hours_wide$Service.Type
cs_hours_wide <- cs_hours_wide[,!names(cs_hours_wide) %in% "Service.Type"]
names(cs_hours_wide) <- monthyear_to_written(names(cs_hours_wide))

#report goodwill hours separately
agg_goodwill <- aggregate(Hours ~ monthyear +  xbrl_status + form_type, 
                          data = timelog_with_status_df[timelog_with_status_df$Billable %in% 1 &
                                                          timelog_with_status_df$is_psm %in% 1 & 
                                                          timelog_with_status_df$Service %in% "Goodwill Hours",], 
                          FUN = sum)
goodwill_hours <- dcast(agg_goodwill, xbrl_status + form_type ~ monthyear, sum, value.var = "Hours")
names(goodwill_hours) <- monthyear_to_written(names(goodwill_hours))
row.names(goodwill_hours) <- paste(goodwill_hours$xbrl_status, "-", goodwill_hours$form_type, "(goodwill)", sep = " ")
name_order <- c("DIY - Q (goodwill)","DIY - K (goodwill)","Basic - Q (goodwill)","Basic - K (goodwill)","Full Service - Q (goodwill)","Full Service - K (goodwill)")
goodwill_hours <- goodwill_hours[match(name_order, row.names(goodwill_hours)),]
goodwill_hours <- goodwill_hours[,-c(1,2)]

#create spacer row and bind billable and goodwill 
space <- data.frame(matrix(c(rep.int("",length(billable_hours))),nrow=1,ncol=length(billable_hours)))
names(space) <- names(billable_hours); row.names(space) <- "goodwill portion only"
cs_space <- space; row.names(cs_space) <- "cs time (not included above)"

for(name in names(billable_hours)[!names(billable_hours) %in% names(cs_hours_wide)]){
  dummy_df <- c()
  for(row in row.names(cs_hours_wide)){
    dummy_df <- c(dummy_df, 0)
  }
  dummy_df <- data.frame(matrix(dummy_df))
  row.names(dummy_df) <- row.names(cs_hours_wide)
  names(dummy_df) <- name
  
  cs_hours_wide <- cbind(cs_hours_wide, dummy_df)
}

# add column to goodwill hours df if none used
for(name in names(space)[!names(space)%in% names(goodwill_hours)]){
  goodwill_hours[,name] <- NA
}
billable_and_goodwill <- rbind(billable_hours, space, goodwill_hours, cs_space, cs_hours_wide)
billable_and_goodwill[is.na(billable_and_goodwill)] <- 0

#////////////////////////////////
# Flat Fee Hours by service level
#////////////////////////////////

project_time <- aggregate(Hours ~ monthyear +  Service.Type + Form.Type, 
                          data = timelog_with_status_df[timelog_with_status_df$Billable %in% 0 &
                                                          timelog_with_status_df$is_psm %in% 1 &
                                                          !is.na(timelog_with_status_df$Hours),], FUN = sum)
project_time[project_time$Service.Type %in% "Migration",]$Form.Type <- "TM"
project_time$header <- paste(project_time$Form.Type, project_time$Service.Type, sep = " ")
groups <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import","10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","Q-K Full Service Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward", "TM Migration")
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

services <- services[services$filing.estimate > "2013-06-30" & services$CS.PS %in% c("PS", "CS/PS"),]
services$monthyear <- format(services$filing.estimate, format = "%y-%m")
services <- services[order(services$filing.estimate),]

scheduled_by_month <- aggregate(Services.ID ~ monthyear + Service.Type + Form.Type, data = services, FUN = length)
#scheduled_by_month <- scheduled_by_month[!scheduled_by_month$Service.Type %in% c("Migration"),] #remove migrations
scheduled_by_month[scheduled_by_month$Service.Type %in% c("Full Service Roll Forward") & scheduled_by_month$Form.Type %in% c("Q-K", "K-K"),]$Form.Type <- "10-K"
scheduled_by_month[scheduled_by_month$Service.Type %in% c("Migration"),]$Form.Type <- "TM"
scheduled_by_month$Service.Name <- paste(scheduled_by_month$Form.Type, scheduled_by_month$Service.Type, sep = " ")
name_order <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import", "10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward", "TM Migration")
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
all_time <- aggregate(Hours ~ monthyear +  role + User , 
                      data = timelog_with_status_df[timelog_with_status_df$is_psm %in% 1,], FUN = sum)
count_by_role <- aggregate(Hours ~ monthyear + User + role , data = all_time, FUN = sum)
count_by_role <- ddply(count_by_role, .(monthyear, role), summarise, count = length(unique(User)))
count_by_role <- dcast(count_by_role, role ~ monthyear, sum, value.var = "count")
#count_by_role <- count_by_role[count_by_role$role %in% c("PSM", "PSS", "Sr PSM"),]
names(count_by_role) <- monthyear_to_written(names(count_by_role))

#////////////////////////////////
# Total client time by role
#////////////////////////////////
time_by_role <- aggregate(Hours ~ monthyear +  role , data = all_time, FUN = sum)
time_by_role <- dcast(time_by_role, role ~ monthyear, sum, value.var = "Hours") 
#time_by_role <- time_by_role[time_by_role$role %in% c("PSM", "PSS", "Sr PSM"),]
names(time_by_role) <- monthyear_to_written(names(time_by_role))

#////////////////////////////////
# Total # of XBRL registrants
# ps history number
#////////////////////////////////
#use application log to identify filings

#conditionally bypass this block when stored data is newer than the source
setwd("c:/r/workspace/source")
filings_data_age <- file.info('app_filings.Rda')$mtime
setwd("c:/r/workspace/42/datastore")

if(file.info("filing_and_customer.Rda")$mtime > filings_data_age){
  print("no change in filing data, loading historical info")
  filing_and_customer <- readRDS(file = "filing_and_customer.Rda")
}else{
  print("newer filing data available, importing and processing...")
  app_data <- load_app_filing_data()
  app_data <- app_data[!is.na(app_data$Fact.Cnt),]
  app_data <- app_data[app_data$Form.Type %in% c("10-Q", "10-K", "10-K/A", "10-Q/A"),] #limit to form 10
  app_data$software <- "WebFilings"
  app_data_uniques <- unique(app_data[names(app_data) %in% c("Registrant.CIK", "monthyear", "software")])
  app_data_wide <- dcast(app_data_uniques, software ~ monthyear, length, value.var = "Registrant.CIK")
  names(app_data_wide) <- monthyear_to_written(names(app_data_wide))
  app_data_wide
  
  # filing_and_customer: xbrl customers
  customers <- unique(app_data[names(app_data) %in% c("Company.Name", "monthyear", "software")])
  customers_wide <- dcast(customers, software ~ monthyear, length, value.var = "Company.Name")
  names(customers_wide) <- monthyear_to_written(names(customers_wide))
  customers_wide
  
  customer_counts <- rbind(app_data_wide, customers_wide)
  row.names(customer_counts) <- c("Registrants", "Customers")
  customer_counts <- customer_counts[,-1]
  
  # filing_and_customer: filing counts
  filings_wide <- dcast(app_data, Form.Type ~ monthyear, length, value.var = "Accession.Number")
  row.names(filings_wide) <- filings_wide$Form.Type
  filings_wide <- filings_wide[,-1]
  names(filings_wide) <- monthyear_to_written(names(filings_wide))
  
  # xbrl status by monthly filers ~ 18.5 minutes
  ptm <- proc.time()

  app_data_cust_status <- ddply(app_data, .var = c("Company.Name", "monthyear"), .fun = function(x){
    status = NA
    time_cust <- timelog_with_status_df[timelog_with_status_df$Account.Name %in% x$Company.Name &
                                          timelog_with_status_df$is_psm %in% 1 &
                                          timelog_with_status_df$Date <= max(x$Filing.Date) &
                                          timelog_with_status_df$Date >= min(x$Filing.Date) - 45,]
    if(dim(time_cust)[1] == 0){
      status = "Inactive DIY"
    }else if(TRUE %in% (time_cust$Service.Type %in% c("Roll Forward","Standard Import","Detail Tagging","Full Service Roll Forward","Full Service Standard Import"))){
      status = "Full Service"
    }else if(TRUE %in% (time_cust$Service.Type %in% c("Maintenance Package", "Maintenance"))){
      status = "Basic"
    }else if(TRUE %in% (time_cust$Service.Type %in% c("Reserve Hours", "Other"))){
      status = "DIY w/ hours"
    }else{
      status = "Inactive DIY"
    }
    data.frame(customer_status = status)
  }) # 4.5 minutes
  proc.time() - ptm
  app_data_reg_status <- ddply(app_data, .var = c("Registrant.CIK", "monthyear"), .fun = function(x){
    status = NA
    
    time_reg <- timelog_with_status_df[timelog_with_status_df$CIK %in% x$Registrant.CIK &
                                         timelog_with_status_df$is_psm %in% 1 &
                                          timelog_with_status_df$Date <= max(x$Filing.Date) &
                                          timelog_with_status_df$Date >= min(x$Filing.Date) - 45,]
  
    if(dim(time_reg)[1] == 0){
      status = "Inactive DIY "
    }else if(TRUE %in% (time_reg$Service.Type %in% c("Roll Forward","Standard Import","Detail Tagging","Full Service Roll Forward","Full Service Standard Import"))){
      status = "Full Service "
    }else if(TRUE %in% (time_reg$Service.Type %in% c("Maintenance Package", "Maintenance"))){
      status = "Basic "
    }else if(TRUE %in% (time_reg$Service.Type %in% c("Reserve Hours", "Other"))){
      status = "DIY w/ hours "
    }else{
      status = "Inactive DIY "
    }
    data.frame(registrant_status = status)
  })
  
  proc.time() - ptm
  cust_uniques <- unique(app_data_cust_status[names(app_data_cust_status) %in% c("Company.Name", "monthyear", "customer_status")])
  reg_uniques <- unique(app_data_reg_status[names(app_data_reg_status) %in% c("Registrant.CIK", "monthyear", "registrant_status")])
  customer_by_status <- dcast(cust_uniques, customer_status ~ monthyear, length, value.var = "Company.Name")
  names(customer_by_status) <- monthyear_to_written(names(customer_by_status))
  row.names(customer_by_status) <- customer_by_status$customer_status
  customer_by_status <- customer_by_status[,-1]
  customer_by_status <- customer_by_status[match(c("Full Service", "Basic", "DIY w/ hours", "Inactive DIY"), rownames(customer_by_status)),]
  registrants_by_status <- dcast(reg_uniques, registrant_status ~ monthyear, length, value.var = "Registrant.CIK")
  names(registrants_by_status) <- monthyear_to_written(names(registrants_by_status))
  row.names(registrants_by_status) <- registrants_by_status$registrant_status
  registrants_by_status <- registrants_by_status[,-1]
  registrants_by_status <- registrants_by_status[match(c("Full Service ", "Basic ", "DIY w/ hours ", "Inactive DIY "), rownames(registrants_by_status)),]
  
  # combine filing and customer count info for filing and customer data tab
  space <- rep("", dim(customer_counts)[1])
  filing_and_customer <- rbind(customer_counts, space, filings_wide,space, registrants_by_status,space, customer_by_status)
  row.names(filing_and_customer) <- c(row.names(customer_counts), "by form", row.names(filings_wide), "registrants", row.names(registrants_by_status), "wDesk accounts", row.names(customer_by_status))

  setwd("c:/r/workspace/42/datastore")
  saveRDS(filing_and_customer, file = "filing_and_customer.Rda")
}

#////////////////////////////////
# Net discounted sales price
#////////////////////////////////
collapsed_opps <- collapsed_opportunities() # ~2.75 minutes
collapsed_opps <- collapsed_opps[collapsed_opps$filing.estimate > "2013-06-30",]
collapsed_opps <- collapsed_opps[order(collapsed_opps$filing.estimate),]

#make list price sales price if list price == 0 or na
collapsed_opps[collapsed_opps$List.Price %in% 0 | is.na(collapsed_opps$List.Price),]$List.Price <- collapsed_opps[collapsed_opps$List.Price %in% 0  | is.na(collapsed_opps$List.Price),]$Sales.Price
#make list price sales price if sales price > list
collapsed_opps[collapsed_opps$List.Price < collapsed_opps$Sales.Price,]$List.Price <- collapsed_opps[collapsed_opps$List.Price < collapsed_opps$Sales.Price,]$Sales.Price

#set discount percentages by item
collapsed_opps$discount <- 1 #instantiate field with full discount
collapsed_opps[!collapsed_opps$Sales.Price %in% 0,]$discount <- 1 - (collapsed_opps[!collapsed_opps$Sales.Price %in% 0,]$Sales.Price / collapsed_opps[!collapsed_opps$Sales.Price %in% 0,]$List.Price)

# split projects into historical and future
  #historical (completed) projects
  sales_info_history <- aggregate(Sales.Price ~ monthyear + Service.Type + Form.Type, 
                      data = collapsed_opps[collapsed_opps$monthyear <= format(Sys.Date(), format = "%y-%m") & collapsed_opps$Status %in% "Completed",], FUN = sum)
  sales_info_history$type <- "history"
  
  #future (active or not started) projects
  sales_info_predicted <- aggregate(Sales.Price ~ monthyear + Service.Type + Form.Type, 
                      data = collapsed_opps[collapsed_opps$monthyear >= format(Sys.Date(), format = "%y-%m") & !collapsed_opps$Status %in% "Completed",], FUN = sum)
  sales_info_predicted$type <- "predicted"

# now combine time
sales_info <- rbind(sales_info_history, sales_info_predicted)
sales_info[sales_info$Service.Type %in% "Migration",]$Form.Type <- "TM"
sales_info[sales_info$Service.Type %in% "Full Service Roll Forward" & 
             sales_info$Form.Type %in% "Q-K",]$Form.Type <- "10-K"
sales_info$header <- paste(sales_info$Form.Type, sales_info$Service.Type, sep = " ")
groups <- c("10-K Detail Tagging","10-Q Detail Tagging","10-K Full Review","10-Q Full Review","10-K Standard Import","10-Q Standard Import","10-K Full Service Standard Import","10-Q Full Service Standard Import","10-K Maintenance","10-Q Maintenance","K-K Roll Forward","Q-K Roll Forward","Q-Q Roll Forward","K-Q Roll Forward","10-K Full Service Roll Forward","10-Q Full Service Roll Forward", "TM Migration")
sales_info[!(sales_info$header %in% groups),]$header <- "Other Services"
sales_info$monthyear_amended <- paste(sales_info$monthyear, sales_info$type, sep = "\n")

#cast wide to prepare for rbind
sales_info_wide <- dcast(sales_info, header ~ monthyear_amended, sum, value.var = "Sales.Price")
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

# #////////////////////////////////
# # Goodwill Hours used by month
# #////////////////////////////////
# wide_goodwill_used <- dcast(timelog_with_status_df[timelog_with_status_df$Service %in% c("Goodwill Hours") &
#                                                      timelog_with_status_df$is_psm %in% 1,],Service ~ monthyear, sum, value.var = "Hours" )
# names(wide_goodwill_used) <- monthyear_to_written(names(wide_goodwill_used))
# 
# #////////////////////////////////
# # Goodwill Balance
# #////////////////////////////////
# unique_customers <- unique(services[!is.na(services$Goodwill.Hours.Available),names(services) %in% c("Account.Name","Goodwill.Hours.Available" )])
# goodwill_balance <- data.frame(date = Sys.Date(), goodwill_balance = sum(unique_customers$Goodwill.Hours.Available))

#////////////////////////////////
# Goodwill Hours used by month
#////////////////////////////////
setwd("C:/R/workspace/source")
churned <- read.csv("churned_customers.csv", header = T , stringsAsFactors=F)
print(paste("churned_customers.csv", "last updated", round(difftime(Sys.time(), file.info("churned_customers.csv")$mtime, units = "days"), digits = 1), "days ago", sep = " "))
churned$Churned.Effective.Date <- as.Date(churned$Churned.Effective.Date, format = "%m/%d/%Y")
churned$Date.Churned.Marked <- as.Date(churned$Date.Churned.Marked, format = "%m/%d/%Y")
churned$quarter <- paste(format(churned$Churned.Effective.Date, format = "%Y"),
                         "-Q", ceiling(as.numeric(format(churned$Churned.Effective.Date, format = "%m"))/3),
                         sep = "")
churned[is.na(churned$Churned.Effective.Date),]$quarter <- paste(format(churned[is.na(churned$Churned.Effective.Date),]$Date.Churned.Marked, format = "%Y"),
                                                  "-Q", ceiling(as.numeric(format(churned[is.na(churned$Churned.Effective.Date),]$Date.Churned.Marked, format = "%m"))/3),
                                                  sep = "")
churned[churned$quarter %in% "NA-QNA",]$quarter <- NA
churned[grepl("Acquired", churned$Churned.Detail, ignore.case = T),]$Churned.Detail <- "Acquired"
churned[grepl("Went Private", churned$Churned.Detail, ignore.case = T),]$Churned.Detail <- "Went Private"
churned[churned$Churned.Detail %in% c("Delinquent/Bankrupt", "Delayed IPO"),]$Churned.Detail <- "Other"
churned[churned$Churned.Detail %in% c("Software Issue", "Wdesk Issues"),]$Churned.Detail <- "Wdesk Issues"

churned_result <- dcast(churned, Churned.Detail ~ quarter, length, value.var = "Account.ID")

#////////////////////////////////
# Averages by service and form type
#////////////////////////////////
service_averages <- collapsed_time()
previous_filing_period <- paste(as.numeric(format((Sys.Date() - 90), "%Y")), ceiling(as.numeric(format((Sys.Date() - 90), "%m"))/3), sep = "")
one_year_ago_period <- paste(as.numeric(format((Sys.Date() - 365), "%Y")), ceiling(as.numeric(format((Sys.Date() - 365), "%m"))/3), sep = "")

#limit to completed quarters
service_averages <- service_averages[service_averages$filingPeriod <= previous_filing_period,] 
service_averages_by_quarter <- ddply(service_averages[!is.na(service_averages$Hours) &
                                                        !service_averages$Service.Type %in% c("Reserve Hours", "Rush Charges", "Migration"),], 
                                     .var = c("Service.Type", "Form.Type", "reportingPeriod"), .fun = function(x){
                                       data.frame(mean = mean(x$Hours),
                                                  median = median(x$Hours),
                                                  n = length(x$Hours))  
                                     })
service_averages_past_year <- ddply(service_averages[!is.na(service_averages$Hours) &
                                                       service_averages$filingPeriod %in% sequence_yearquarters(one_year_ago_period, previous_filing_period, 1) &
                                                        !service_averages$Service.Type %in% c("Reserve Hours", "Rush Charges", "Migration"),], 
                                     .var = c("Service.Type", "Form.Type"), .fun = function(x){
                                       data.frame(annual_mean_all = mean(x$Hours),
                                                  annual_median_all = median(x$Hours),
                                                  annual_n_all = length(x$Hours),
                                                  annual_mean_psm = mean(x$PSM.Hours),
                                                  annual_median_psm = median(x$PSM.Hours),
                                                  annual_n_psm = length(x$PSM.Hours))  
                                     })
service_averages_by_quarter_long <- melt(service_averages_by_quarter, id=c("Service.Type", "Form.Type", "reportingPeriod"))
service_averages_by_quarter_long$header <- paste(service_averages_by_quarter_long$reportingPeriod, service_averages_by_quarter_long$variable, sep = "\n")
service_averages_by_quarter_long$reportingPeriod <- NULL
service_averages_past_year_long <- melt(service_averages_past_year, id=c("Service.Type", "Form.Type"))
service_averages_past_year_long$header <- service_averages_past_year_long$variable
service_averages_by_quarter_long <- rbind(service_averages_by_quarter_long, service_averages_past_year_long)

service_averages_by_quarter_wide <- dcast(service_averages_by_quarter_long, Service.Type + Form.Type ~ header, sum, value.var = c("value"))
service_averages_by_quarter_wide <- service_averages_by_quarter_wide[,names(service_averages_by_quarter_wide)[rev(order(names(service_averages_by_quarter_wide)))]]

#****************** write results to file
setwd("C:/R/workspace/42/output")
write.xlsx(x = billable_and_goodwill, file = "42_data.xlsx",sheetName = "billable_hours", row.names = TRUE)
write.xlsx(x = project_hours, file = "42_data.xlsx",sheetName = "project_hours", row.names = TRUE, append = TRUE)
write.xlsx(x = scheduled_services, file = "42_data.xlsx",sheetName = "scheduled_services", row.names = TRUE, append = TRUE)
write.xlsx(x = count_by_role, file = "42_data.xlsx",sheetName = "count_by_role", row.names = FALSE, append = TRUE)
write.xlsx(x = time_by_role, file = "42_data.xlsx",sheetName = "time_by_role", row.names = FALSE, append = TRUE)
write.xlsx(x = filing_and_customer, file = "42_data.xlsx",sheetName = "customers_and_filings", row.names = TRUE, append = TRUE)
write.xlsx(x = sales_info_wide, file = "42_data.xlsx",sheetName = "net_sales", row.names = TRUE, append = TRUE)
write.xlsx(x = discount_groups_wide, file = "42_data.xlsx",sheetName = "services by discount", row.names = FALSE, append = TRUE)
write.xlsx(x = full_discount_wide, file = "42_data.xlsx",sheetName = "Full discount", row.names = FALSE, append = TRUE)
write.xlsx(x = discount_20_to_99_wide, file = "42_data.xlsx",sheetName = "20-99 discount", row.names = FALSE, append = TRUE)
write.xlsx(x = churned_result, file = "42_data.xlsx",sheetName = "churned", row.names = FALSE, append = TRUE)
write.xlsx(x = service_averages_by_quarter_wide, file = "42_data.xlsx",sheetName = "service averages", row.names = FALSE, append = TRUE)
# write.xlsx(x = wide_goodwill_used, file = "42_data.xlsx",sheetName = "Goodwill Hours Used", row.names = FALSE, append = TRUE)
# write.xlsx(x = goodwill_balance, file = "42_data.xlsx",sheetName = "Goodwill Balance", row.names = FALSE, append = TRUE)

proc.time() - start

#rbind results
#test <- rbind.fill(billable_hours, project_hours, scheduled_services, count_by_role, time_by_role)