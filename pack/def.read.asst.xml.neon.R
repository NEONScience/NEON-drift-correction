#' @title Parse an asset xml info into a dataframe
#' @author Guy Litt
#' @description  Given an asset query that has been parsed into an xml, return a data frame of useful information
#' @param asstXml an XMLInternalDocument / XMLAbstractDocument specific to a particular asset
#' @return dtAsst a data.table ordered in decreasing time that contains information pertaining to an asset such as install/remove date,l ocation, asset details
#' @example
#' rspn <- httr::GET(url="den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp/asset-installs?meas-strm-name=NEON.D08.DELA.DP0.00098.001.01357.000.060.000&install-range-begin=2017-01-01T00:00:00.000Z&install-range-cutoff=2021-02-01T00:00:00.000Z")
#' xml <- XML::xmlParse(cntn)
#' def.read.asst.xml.neon(asstXml = xml)
#' @export

# Changelog / Contributions
#  2021-02-23 GL originally created

def.read.asst.xml.neon <- function(asstXml){
  # XML file as a list
  listXml <- XML::xmlToList(asstXml)
  
  lsAsst <- base::list()
  for(idx in 1:length(listXml)){
    nameIdx <- names(listXml)[idx]
    if(nameIdx == "assetInstall"){ #proceed
      
      # Install date
      if(base::length(listXml[[idx]]$installDate) ==0){
        next() # No install date means no useful info
      } else {
        instDate <- listXml[[idx]]$installDate
      }
      # Remove date
      if(base::length(listXml[[idx]]$removeDate) ==0){
        rmDate <- NA
      } else {
        rmDate <- listXml[[idx]]$removeDate
      }
      
      # Asset details
      asstDeet <- listXml[[idx]]$asset
      asstAttr <- asstDeet$.attrs
      
      if(length(asstDeet) > 0){
        asstDf <- base::data.frame(site = base::ifelse(base::is.null(asstDeet$maximoSite), NA,asstDeet$maximoSite), 
                         locMaximo = base::ifelse(base::is.null(asstDeet$maximoLocation), NA,asstDeet$maximoLocation), 
                         assetNumber = base::ifelse(base::is.null(asstDeet$assetNumber), NA,asstDeet$assetNumber), 
                         assetUid = base::ifelse(base::is.null(base::as.numeric(asstAttr["assetUid"])), NA,base::as.numeric(asstAttr["assetUid"])),
                         assetTag = base::ifelse(base::is.null(asstDeet$assetTag), NA,asstDeet$assetTag), 
                         assetDesc = base::ifelse(base::is.null(asstDeet$description),NA,asstDeet$description),
                         assetUid = base::ifelse(base::is.na(asstDeet$.attrs["assetUid"]),NA,asstDeet$.attrs["assetUid"]),
                         stringsAsFactors = FALSE)
      } else {
        asstDf <- base::data.frame(site = NA, 
                                   locMaximo = NA,
                                   assetNumber = NA,
                                   assetUid = NA,
                                   assetTag = NA,
                                   assetDesc = NA,
                                   assetUid = NA,
                                   stringsAsFactors = FALSE)
      }

      # Term details
      defDeet <- listXml[[idx]]$assetDefinition
      if (length(defDeet) > 0){
        deetDf <- base::data.frame(strmId = base::ifelse(base::is.null(defDeet$ingestTerm$streamId), NA,defDeet$ingestTerm$streamId),
                                   termName = base::ifelse(base::is.null(defDeet$ingestTerm$term$name), NA,defDeet$ingestTerm$term$name),
                                   termNum = base::ifelse(base::is.null(defDeet$ingestTerm$term$termNumber), NA,defDeet$ingestTerm$term$termNumber),
                                   termDesc = base::ifelse(base::is.null(defDeet$ingestTerm$term$description), NA, defDeet$ingestTerm$term$description),
                                   stringsAsFactors = FALSE)
      } else {
        deetDf <- base::data.frame(strmId = NA,
                                   termName = NA,
                                   termNum = NA,
                                   termDesc = NA,
                                   stringsAsFactors = FALSE)
      }  
      
      # Location details
      locDeet <- listXml[[idx]]$namedLocation$.attrs
      if(length(locDeet)>0){
        locDf <- base::data.frame(cfgloc = base::ifelse(base::is.null(locDeet[['name']]), NA,locDeet[['name']]),
                                  locUrl = base::ifelse(base::is.null(locDeet[['location']]), NA,locDeet[['location']]),
                                  stringsAsFactors = FALSE)
      } else {
        locDf <- base::data.frame(cfgloc = NA,
                                  locUrl = NA,
                                  stringsAsFactors = FALSE)
      }
      
      # All together now
      allDf <- base::data.frame(installDate = instDate,
                                removeDate = rmDate,
                                stringsAsFactors = FALSE)
      
      allDf <- base::cbind(allDf, asstDf)
      allDf <- base::cbind(allDf, deetDf)
      allDf <- base::cbind(allDf, locDf)
      
      lsAsst[[idx]] <- allDf
    }
  }
  if(base::length(lsAsst)>0){
    dtAsst <- data.table::rbindlist(lsAsst)
  } else {
    dtAsst <- NULL
  }
  
  dtAsst$installDateText <- dtAsst$installDate
  dtAsst$removeDateText <- dtAsst$removeDate
  
  # Convert character timestamp to POSIXct class and order df based on decreasing install time
  dtAsst$installDate <- base::as.POSIXct(dtAsst$installDate, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC") # dtAsst$installDate
  dtAsst$removeDate <- base::as.POSIXct(dtAsst$removeDate, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC") 
  
  dtAsst <- dtAsst[base::order(dtAsst$installDate,decreasing = TRUE)]
  
  return(dtAsst)
}