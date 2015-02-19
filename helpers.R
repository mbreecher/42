monthyear_to_written <- function(old_names){
  new_names <- c()
  for (name in old_names){
    if(grepl("-",name)){
      new_names = c(new_names, paste(format(as.Date(paste(substr(name,1,5),"-01", sep = "")), format = "%b-%y"),
                    substr(name,6,nchar(name)), sep = ""))
    }else{
      new_names = c(new_names, name)
    }
  }
  new_names
}