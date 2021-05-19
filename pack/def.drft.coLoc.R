def.drft.coLoc <- function(dataMain,
                           dataCoLoc){
  
  dmmy <- numeric(0)
  rpt <- data.frame(daySinceInst=dmmy,
                    mean=dmmy,
                    med=dmmy,
                    min=dmmy,
                    max=dmmy,
                    sd=dmmy,
                    n=dmmy,
                    stringsAsFactors=FALSE)
  
  # Align the timeseries
  dataBoth <- base::merge(dataMain,dataCoLoc,by='time')

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
    seqDaySincInst <- c(seq(from=30,to=daySincInstMax,by=30),daySincInstMax)
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
      
      # Difference the calibrated & drift corrected readings between the two sensors
      dataBothIdx$diffCal <- dataBothIdx$calibrated.y - dataBothIdx$calibrated.x
      dataBothIdx$diffCorr <- dataBothIdx$driftCorrected.y - dataBothIdx$driftCorrected.x
      dataBothIdx$diffChng <- abs(dataBothIdx$diffCorr)-abs(dataBothIdx$diffCal) # Negative is closer to zero, positive is further away from zero
      
      rptIdx <- data.frame(daySinceInst=seqDaySincInst[idxSeq]+15,
                           mean=mean(dataBothIdx$diffChng,na.rm=TRUE),
                           med=median(dataBothIdx$diffChng,na.rm=TRUE),
                           min=min(dataBothIdx$diffChng,na.rm=TRUE),
                           max=max(dataBothIdx$diffChng,na.rm=TRUE),
                           sd=sd(dataBothIdx$diffChng,na.rm=TRUE),
                           n=sum(!is.na(dataBothIdx$diffChng)),
                           stringsAsFactors=FALSE)
      rpt <- rbind(rpt,rptIdx)
    }
  }

  return(rpt)


}