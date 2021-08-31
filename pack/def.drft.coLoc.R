def.drft.coLoc <- function(dataMain,
                           dataCoLoc,
                           TypeUcrtMain=c('cnst','mult',NA)[3], # Type of calibration uncertainty calculation. 'cnst' for U_CVALA1 as a constant uncertainty, 'mult' for U_CVALA1 as a multiplier, NA to not compute uncertainty
                           TypeUcrtCoLoc=c('cnst','mult',NA)[3] # Type of calibration uncertainty calculation. 'cnst' for U_CVALA1 as a constant uncertainty, 'mult' for U_CVALA1 as a multiplier, NA to not compute uncertainty
                             ){
  
  dmmy <- numeric(0)
  dmmyChar <- character(0)
  rpt <- data.frame(idInst=dmmyChar,
                    site=dmmyChar,
                    assetUidMain=dmmyChar,
                    assetUidCoLoc=dmmyChar,
                    DaysSinceInstall=dmmy,
                    medCalMain=dmmy,
                    medCalCoLoc=dmmy,
                    meanDiffCal=dmmy,
                    medDiffCal=dmmy,
                    medCorrMain=dmmy,
                    medCorrCoLoc=dmmy,
                    meanDiffCorr=dmmy,
                    medDiffCorr=dmmy,
                    meanConv=dmmy,
                    medConv=dmmy,
                    minConv=dmmy,
                    maxConv=dmmy,
                    sdConv=dmmy,
                    nConv=dmmy,
                    medUcrtCalMain=dmmy,
                    medUcrtCalCoLoc=dmmy,
                    medUcrtDrftCorrMain=dmmy,
                    medUcrtDrftCorrCoLoc=dmmy,
                    stringsAsFactors=FALSE)
  
  # Align the timeseries
  dataBoth <- base::merge(dataMain,dataCoLoc,by='time')

  site <- dataMain$site[1]
  
  # Go through each sensor refresh for the main sensor  
  timeInst <- unique(dataMain$instDate)
  timeInst <- sort(timeInst[!is.na(timeInst)])
  for (idxInst in seq_len(length(timeInst)-1)){
    
    # Determine 30-day time blocks to evaluate sensor convergence after drift correction
    daySincInstMax <- as.numeric(difftime(timeInst[idxInst+1],timeInst[idxInst],units='days'))
    if(daySincInstMax <= 30){
      # Skip if time between installs is <= 30 days
      next
    } 
    
    daySincInst <- difftime(dataBoth$time,timeInst[idxInst],units='days')
    seqDaySincInst <- c(seq(from=0,to=daySincInstMax,by=30),daySincInstMax)
    
    # Grab asset UID for the main sensor during this install
    assetUidMain <- setdiff(
                            unique(
                                   dataBoth$assetUID.x[
                                                       daySincInst >= 0 & 
                                                       daySincInst < daySincInstMax
                                                       ]
                                   ),
                            NA)

    if(length(assetUidMain) > 1){
      print('Something is wrong. More than 1 asset UID for the main sensor was found during this install period. Check the data.')
      next
    }
    
    # Run through the 30-day time blocks
    for(idxSeq in seq_len(length(seqDaySincInst)-1)){
      # Data constraints
      dataBothIdx <- subset(dataBoth,
                            subset=daySincInst >= seqDaySincInst[idxSeq] & 
                                   daySincInst < seqDaySincInst[idxSeq+1] & 
                                   dataBoth$drftCorrFlag.x == TRUE & 
                                   dataBoth$drftCorrFlag.y == TRUE
                            )
      if(nrow(dataBothIdx) == 0){
        next
      } 
      
      # Grab asset UIDs for the co-located sensor
      assetUidCoLoc <- setdiff(unique(dataBothIdx$assetUID.y),NA)[1] # There might be a sensor change during this period. Take the one at the beginning of the period.

      # Create an identifier for this particular install so we can parse out individual installs later
      idInst <- paste0(site,assetUidMain)
      
      # Difference the calibrated & drift corrected readings between the two sensors
      dataBothIdx$diffCal <- dataBothIdx$calibrated.y - dataBothIdx$calibrated.x
      dataBothIdx$diffCorr <- dataBothIdx$driftCorrected.y - dataBothIdx$driftCorrected.x
      dataBothIdx$diffChng <- abs(dataBothIdx$diffCorr)-abs(dataBothIdx$diffCal) # Negative is closer to zero, positive is further away from zero
      
      # Compute uncertainties
      if(TypeUcrtMain=='cnst'){
        dataBothIdx$ucrtCalMain <- dataBothIdx$U_CVALA1.x 
        dataBothIdx$ucrtDrftCorrMain <- dataBothIdx$U_CVALE9.x 
      } else if (TypeUcrtMain=='mult'){
        dataBothIdx$ucrtCalMain <- dataBothIdx$U_CVALA1.x * dataBothIdx$driftCorrected.x
        dataBothIdx$ucrtDrftCorrMain <- dataBothIdx$U_CVALE9.x * dataBothIdx$driftCorrected.x
      } else {
        dataBothIdx$ucrtCalMain <- NA
        dataBothIdx$ucrtDrftCorrMain <- NA
      }
      if(TypeUcrtCoLoc=='cnst'){
        dataBothIdx$ucrtCalCoLoc <- dataBothIdx$U_CVALA1.y 
        dataBothIdx$ucrtDrftCorrCoLoc <- dataBothIdx$U_CVALE9.y 
      } else if (TypeUcrtCoLoc=='mult'){
        dataBothIdx$ucrtCalCoLoc <- dataBothIdx$U_CVALA1.y * dataBothIdx$driftCorrected.y
        dataBothIdx$ucrtDrftCorrCoLoc <- dataBothIdx$U_CVALE9.y * dataBothIdx$driftCorrected.y
      } else {
        dataBothIdx$ucrtCalCoLoc <- NA
        dataBothIdx$ucrtDrftCorrCoLoc <- NA
      }
      
      
      rptIdx <- data.frame(idInst=idInst,
                           site=site,
                           assetUidMain=assetUidMain,
                           assetUidCoLoc=assetUidCoLoc,
                           DaysSinceInstall=seqDaySincInst[idxSeq]+15,
                           medCalMain=median(dataBothIdx$calibrated.x,na.rm=TRUE),
                           medCalCoLoc=median(dataBothIdx$calibrated.y,na.rm=TRUE),
                           meanDiffCal=mean(dataBothIdx$diffCal,na.rm=TRUE),
                           medDiffCal=median(dataBothIdx$diffCal,na.rm=TRUE),
                           medCorrMain=median(dataBothIdx$driftCorrected.x,na.rm=TRUE),
                           medCorrCoLoc=median(dataBothIdx$driftCorrected.y,na.rm=TRUE),
                           meanDiffCorr=mean(dataBothIdx$diffCorr,na.rm=TRUE),
                           medDiffCorr=median(dataBothIdx$diffCorr,na.rm=TRUE),
                           meanConv=mean(dataBothIdx$diffChng,na.rm=TRUE),
                           medConv=median(dataBothIdx$diffChng,na.rm=TRUE),
                           minConv=min(dataBothIdx$diffChng,na.rm=TRUE),
                           maxConv=max(dataBothIdx$diffChng,na.rm=TRUE),
                           sdConv=sd(dataBothIdx$diffChng,na.rm=TRUE),
                           nConv=sum(!is.na(dataBothIdx$diffChng)),
                           medUcrtCalMain=median(dataBothIdx$ucrtCalMain,na.rm=TRUE),
                           medUcrtCalCoLoc=median(dataBothIdx$ucrtCalCoLoc,na.rm=TRUE),
                           medUcrtDrftCorrMain=median(dataBothIdx$ucrtDrftCorrMain,na.rm=TRUE),
                           medUcrtDrftCorrCoLoc=median(dataBothIdx$ucrtDrftCorrCoLoc,na.rm=TRUE),
                           stringsAsFactors=FALSE)
      rpt <- rbind(rpt,rptIdx)
    }
  }

  return(rpt)


}