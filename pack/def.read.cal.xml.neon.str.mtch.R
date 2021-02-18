#' @title Read  coefficients from a CVAL .xml
#' @author Guy Litt
#' @description Reads in the coefficient from a NEON CVAL .xml file
#' @param calRead  XMLInternalDocument class object, a parsed XML file.
#' @param mtchStrs The match strings desired for a calibration coefficient. Default "CVALD"
#' @param fileSrce The method used to acquire cal file. Default 'postman' refers to a GET Calibration/Single by Certificate Number (XML) format
#' @example 
#' calRead <- XML::xmlParse(pathCval)
#' dfCval <- def.read.cal.xml.neon.str.mtch(calRead, mtchStrs = "CVALE")
#' @return A data.frame of calibration coefficient name, value, and associated time range.
#' 
#' 
# TODO create approach for assigning multiple time ranges
# Changelog / contributions
#  2021-02-11 Guy Litt, originally created
#  2021-02-17 added 'postman' as default file source.
def.read.cal.xml.neon.str.mtch <- function(calRead, mtchStrs = c("CVALE","CVALA","CVALD")[1], fileSrce = c("postman","janae")[1]){
  if(!"XMLInternalDocument" %in% base::class(calRead)){
    stop("calRead needs to be in an XML format. See XML::xmlParse()")
  }
  
  # XML file as a list
  listXml <- XML::xmlToList(calRead)#[["DPMSCalValXML"]]
  
  
  if(fileSrce == "janae"){
    idxsDrft <- base::unlist(base::lapply(mtchStrs, function(mtchStr) base::grep(mtchStr,listXml$StreamCalVal)) )
    # Coefficients of interest:
    namzDrft <- base::unlist(base::lapply(idxsDrft, function(i) listXml$StreamCalVal[[i]]$Name))
    valsDrft <- base::unlist(base::lapply(idxsDrft, function(i) listXml$StreamCalVal[[i]]$Value))
    # sensor ID:
    sensId <- listXml$SensorID$MxAssetID
    # Time range:
    timeBgn <- listXml$ValidTimeRange$StartTime
    timeEnd <- listXml$ValidTimeRange$EndTime
    
  } else if (fileSrce == "postman") {
    idxsDrft <- base::unlist(base::lapply(mtchStrs, function(mtchStr) base::grep(mtchStr,listXml)) )
    namzDrft <- base::unlist(base::lapply(idxsDrft, function(i) listXml[[i]]$name))
    valsDrft <- base::unlist(base::lapply(idxsDrft, function(i) base::as.numeric(listXml[[i]]$value)))
    #sensor ID:
    sensId <- listXml$assetUid
    
    # Time range:
    timeBgn <- listXml$validStartTime
    timeEnd <- listXml$validEndTime
  }
  # Build dataframe 
  dfDrft <- base::data.frame(coef = namzDrft, value = valsDrft, stringsAsFactors = FALSE)
  dfDrft$sensId <- sensId
  
  if(base::length(base::unique(timeBgn))==1){
    dfDrft$timeBgnStr <- base::unique(timeBgn)
    dfDrft$timeEndStr <- base::unique(timeEnd)
    
    dfDrft$timeBgn <- base::as.POSIXct(base::unique(timeBgn),format="%Y-%m-%dT%H:%M:%OS",tz="GMT")
    dfDrft$timeEnd <- base::as.POSIXct(timeEnd,format="%Y-%m-%dT%H:%M:%OS",tz="GMT")
    
  } else {
    stop("This need something to match multiple calibration file times.")
  }
  
  return(dfDrft)
}
