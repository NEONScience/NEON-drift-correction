#' @title Wrapper to calibrate and drift-correct a datastream
#' @author Cove Sturtevant, Guy Litt
#' @description given data product ID, un-calibrated L0 data, streamId, and base api URL,
#' download calibration coefficients and correct the data, and download drift
#' correction coefficients and drift correct the data
#' @param idDp
#' @param data
#' @param streamId Integer. Stream ID from a sensor. See engineering avro schemas at \link{https://github.battelleecology.org/Engineering/avro-schemas/tree/develop/schemas}
#' @param funcCal String. Name of function in NEONprocIS.base to apply the calibration. Defaults to def.cal.conv.poly (polynomial conversion with CVALA coefficients)
#' @param urlBaseApi base url. Default \link{'den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp'}
#' @param timeCol time column name in \code{data}. Default 'time'
#' @param dataCol data column name in \code{data}. Default 'data'
#' @return List of two objects: 
#' $data: same dataframe plus calibration-corrected and drift-corrected data columns, boolean drift corrected col, and install date columns
#' $assetHist The asset history data.frame
#' 
# Changelog/Contributions
# Originally created by Cove, adapted to wrapper function by GL 2021-03-03
#  2021-03-24 UTC to GMT, added boolean drift correction indicator & asset install date columns to output data, GL
wrap.cal.corr.drft <- function(idDp, 
                               data, 
                               streamId, 
                               funcCal = 'def.cal.conv.poly',
                               urlBaseApi = 'den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp', 
                               timeCol = "time", 
                               dataCol = "data"
                               ){
  
  if (base::length(streamId) != 1 || !base::is.integer(streamId)){
    stop("streamId integer should only have a length of one.")
  }
  
  
  # Get the asset install history
  timeBgnStr <- format(data[,timeCol][1],'%Y-%m-%dT%H:%M:%OSZ')# Begin range to query asset install history
  timeEndStr <- format(tail(data[,timeCol],1),'%Y-%m-%dT%H:%M:%OSZ') # End range to query asset install history
  urlApi <- base::paste0(urlBaseApi,'/asset-installs?meas-strm-name=',idDp,'&install-range-begin=',timeBgnStr,'&install-range-cutoff=',timeEndStr,collapse='')
  rspn <- httr::GET(url=urlApi)
  cntn <- httr::content(rspn,as="text")
  xml <- XML::xmlParse(cntn)
  
  assetHist <- def.read.asst.xml.neon(asstXml = xml) # Guy's function. Note: the site and locMaximo columns aren't correct. All rows pertain to an intall at the specified idDp
  # assetHist$installDate <- as.POSIXct(assetHist$installDate,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")
  # assetHist$removeDate <- as.POSIXct(assetHist$removeDate,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")
  
  
  # Apply the calibration & drift correction for every sensor install period
  data[,timeCol] <- as.POSIXct(data[,timeCol], tz = "GMT")
  data$calibrated <- NA
  data$driftCorrected <- NA
  data$drftCorrFlag <- FALSE
  data$assetUID <- NA
  data$instDate <- as.POSIXct(NA,tz='GMT')
  data$U_CVALA1 <- NA
  data$U_CVALE5 <- NA
  data$U_CVALE9 <- NA
  for (idxInst in seq_len(nrow(assetHist))){
    
    urlApi <- base::paste0(urlBaseApi,'/calibrations?asset-stream-key=',assetHist$assetUid[idxInst],':',streamId,
                           '&startdate=','2012-01-01T00:00:00.000Z','&enddate=','2030-01-01T00:00:00.000Z',collapse='')
    rspn <- httr::GET(url=urlApi,httr::add_headers(Accept = "application/json"))
    cntn <- httr::content(rspn,as="text")
    cal <- jsonlite::fromJSON(cntn,simplifyDataFrame=T, flatten=T)$calibration
    cal$validStartTime <- base::as.POSIXct('1970-01-01',tz="GMT")+cal$validStartTime/1000 # milliseconds since 1970
    cal$validEndTime <- base::as.POSIXct('1970-01-01',tz="GMT")+cal$validEndTime/1000 # milliseconds since 1970
    
    # # Put cal metadata into a data frame - This not used, but maybe in the future...
    # metaCal <- base::data.frame(path=NA,
    #                             file=cal$certificateFileName,
    #                             timeValiBgn=cal$validStartTime,
    #                             timeValiEnd=cal$validEndTime,
    #                             id=base::as.numeric(cal$certificateNumber),
    #                             stringsAsFactors=FALSE)
    # 
    # # Select the appropriate cal for the time period
    # calSlct <- NEONprocIS.cal::def.cal.slct(metaCal=metaCal,
    #                              TimeBgn=assetHist$installDate[idxInst],
    #                              TimeEnd=assetHist$removeDate[idxInst])
    # 
    
    # Get the calibration coefficients for the install period
    calInst <- cal[cal$validStartTime < assetHist$installDate[idxInst],] # Restrict calibrations to those before the sensor was installed
    idxCalInst <- which.min(assetHist$installDate[idxInst]-calInst$validStartTime) # Get the most recent cal prior to sensor install
    calCoefInst <- calInst$calibrationMetadatum[idxCalInst][[1]] # Get the calibration coefficients.
    nameCalCoefInst <- names(calCoefInst)
    nameCalCoefInst[nameCalCoefInst=='name'] <- "Name"
    nameCalCoefInst[nameCalCoefInst=='value'] <- "Value"
    names(calCoefInst) <-nameCalCoefInst
    calCoef <- calCoefInst[!grepl('U_CVAL',calCoefInst$Name),] # Extract calibration coefficients
    ucrtCoef <- calCoefInst[grepl('U_CVAL',calCoefInst$Name),] # Extract uncertainty coefficients
    infoCal <- list(cal=calCoef,ucrt=ucrtCoef)
    
    # ------- Convert data using the calibration function -------
    setData <- data[,timeCol] >= assetHist$installDate[idxInst] & 
      data[,timeCol] >= calInst$validStartTime[idxCalInst] & 
      (is.na(assetHist$removeDate[idxInst]) | data[,timeCol] < assetHist$removeDate[idxInst])
    
    if(sum(!is.na(data[setData,dataCol])) == 0){
      next
    }

    # Retrieve the calibration function
    if(!is.na(funcCal)){
      funcCalExec <- base::get(funcCal, base::asNamespace("NEONprocIS.cal"))
    
      # Apply the calibration function
      data$calibrated[setData] <- base::do.call(funcCalExec,args=base::list(data=base::subset(data,subset=setData,drop=FALSE),
                                                                           infoCal=infoCal,
                                                                           varConv=dataCol
                                                                           )
      )
    } else {
      data$calibrated[setData] <- data[setData,dataCol]
    } 
    
    # Add asset ID to the dataset
    data$assetUID[setData] <- assetHist$assetUid[idxInst]
      
    # Add asset install time (numeric format) to the dataset
    data$instDate[setData] <- assetHist$installDate[idxInst]
    
    # Record calibration uncertainty
    ucrtCoefA1 <- ucrtCoef$Value[ucrtCoef$Name == 'U_CVALA1']
    if(!is.null(ucrtCoefA1)){
      data$U_CVALA1[setData] <- ucrtCoefA1[1]
    }
    
    # Now get the cal that has drift coefficients for the install period (the subsequent cal has these coefs)
    calDrft <- cal[cal$validStartTime > assetHist$removeDate[idxInst],] # Restrict calibrations to those after the sensor left
    idxCalDrft <- which.min(calDrft$validStartTime-assetHist$removeDate[idxInst]) # Get the most recent cal after sensor left
    
    # Move on if we don't have a subsequent cal
    if(length(idxCalDrft) == 0){
      next
    }
    
    # Pull the drift coefficients
    calCoefDrift <- calDrft$calibrationMetadatum[idxCalDrft][[1]] # Get the calibration coefficients.
    nameCalCoefDrift <- names(calCoefDrift)
    nameCalCoefDrift[nameCalCoefDrift=='name'] <- "Name"
    nameCalCoefDrift[nameCalCoefDrift=='value'] <- "Value"
    names(calCoefDrift) <- nameCalCoefDrift
    coefDrftA <- calCoefDrift$Value[calCoefDrift$Name == 'U_CVALE6']
    coefDrftB <- calCoefDrift$Value[calCoefDrift$Name == 'U_CVALE7']
    coefDrftC <- calCoefDrift$Value[calCoefDrift$Name == 'U_CVALE8']
    coefDrftE5 <- calCoefDrift$Value[calCoefDrift$Name == 'U_CVALE5']
    coefDrftE9 <- calCoefDrift$Value[calCoefDrift$Name == 'U_CVALE9']
    
    # Move on if no drift coefficients
    if(length(c(coefDrftA,coefDrftB,coefDrftC)) != 3){
      next
    }
    
    # Apply drift correction
    daysSinceCal <- as.numeric(difftime(data[,timeCol],calInst$validStartTime[idxCalInst],units='days'))
    data$driftCorrected[setData] <- data$calibrated[setData]-(coefDrftA*data$calibrated[setData]^2 + 
                                                                coefDrftB*data$calibrated[setData] + 
                                                                coefDrftC)*daysSinceCal[setData]
    
    
    data$drftCorrFlag[setData] <- TRUE
    
    # Record drift uncertainties
    data$U_CVALE5[setData] <- coefDrftE5
    data$U_CVALE9[setData] <- coefDrftE9
    
  }
  
  # Ensure time and install date in GMT
  attr(data[,timeCol],'tzone') <- 'GMT'
  attr(data$instDate,'tzone') <- 'GMT'
  return(base::list(data = data, assetHist = assetHist))
}