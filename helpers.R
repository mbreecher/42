monthyear_to_written <- function(old_names){
  new_names <- c()
  for (name in old_names){
    if(grepl("-",name) & nchar(name) == 5){
      new_names = c(new_names, format(as.Date(paste(name,"-01", sep = "")), format = "%b-%y"))
    }else{
      new_names = c(new_names, name)
    }
  }
  new_names
}