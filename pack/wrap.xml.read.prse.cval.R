#' @title Read and parse CVAL .xml files
#' @author Guy Litt
#' @description Given a file directory containing .xml cval files, generate a data.table of cval coeffs
#' @param dirCval the directory containing CVAL .xml files
#' @param mtchStrs The match strings desired for a calibration coefficient. Default "CVALE"
#' @param fileSrce The method used to acquire cal file. Default 'postman' refers to a GET Calibration/Single by Certificate Number (XML) format
#' @example
#' dirCval <- "C:/Users/glitt/Documents/QAQC/driftCalFiles2018/"
#' dtCval <- wrap.xml.read.prse.cval(dirCval)

# Changelog 
#  2021-02-11 created
#  2021-02-17 added fileSrce to accommodate different .xml file formats

library(XML)
library(data.table)
# !!!!
# TODO source("path to dir.read.cal.xml.neon.str.mtch.R")
# !!!!
wrap.xml.read.prse.cval <- function(dirCval, mtchStrs = "CVALE", fileSrce = c("postman","janae")[1]){
  lsCal <- base::list()
  for(fn in base::list.files(dirCval, pattern = ".xml")){
    pathCval <- base::paste0(dirCval, fn)
    
    calRead <- XML::xmlParse(pathCval)
    
    # Extract drift calibration coefficients
    dfCval <- def.read.cal.xml.neon.str.mtch(calRead,mtchStrs = mtchStrs, fileSrce = fileSrce)
    lsCal[[fn]] <- dfCval
  }
  
  dtCval <- data.table::rbindlist(lsCal)

  return(dtCval)  
}



