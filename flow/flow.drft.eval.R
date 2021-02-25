# Code to evaluate CVAL drift correction 
# Two methods of validating the drift. 
#  1. Does the step change in values between one installation and the next diminish after applying the drift correction
#  2. Do the values of co-located replicate sensors converge after applying drift correction
rm(list=ls())
source('~/R/NEON-drift-correction/pack/def.read.asst.xml.neon.R')
library('ggplot2')
library('plotly')

idDp <- 'NEON.D08.DELA.DP0.00098.001.01357.000.060.000' # RH
streamId <- 0 # RH
#idDp <- 'NEON.D08.DELA.DP0.00098.001.01309.000.060.000' # Temp
#streamId <- 1 # Temp



# Either download or read in the L0 data for the time period of interest. Note, it is wise to downsample this data to every 5 min or so
dnld <- FALSE
fileData=paste0('/scratch/SOM/Drift/',idDp,'.RData') # Name of the data file we'll either be saving or loading
if(dnld == TRUE){
  yearMnthBgn <- '2018-06' # Format YYYY-DD as a string
  yearMnthEnd <- '2021-01' # Format YYYY-DD as a string
  timeAgr <- 5 # minutes
  
  timeDist <- base::as.difftime(timeAgr,units="mins") # Time aggregation interval in minutes
  
  yearEnd <- base::as.numeric(base::substr(yearMnthEnd,start=1,stop=4))
  mnthEnd <- base::as.numeric(base::substr(yearMnthEnd,start=6,stop=7))

  if(mnthEnd == 12){
    mnthEnd <- '01'
    yearEnd <- base::as.character(yearEnd + 1)
  } else {
    mnthEnd <- base::as.character(mnthEnd + 1)
  }

  # Set up the output time sequence
  timeBgn <- base::strptime(base::paste0(yearMnthBgn,'-01'),format="%Y-%m-%d",tz='GMT') # Start time
  timeEnd <- base::strptime(base::paste0(yearEnd,'-',mnthEnd,'-01'),format="%Y-%m-%d",tz='GMT')-timeDist
  timeBgn <- base::as.POSIXct(base::seq.POSIXt(from=timeBgn,to=timeEnd,by=timeDist,tz="GMT"))
  timeEnd <- timeBgn + timeDist
  numData <- base::length(timeBgn)

  # We're going to grab data for each month and string it together
  yearMnth <- base::unique(base::format(timeBgn,format='%Y-%m'))
  data <- vector(mode='list',length=length(yearMnth)) # Initialize
  names(data) <- yearMnth

  for(idxMnth in yearMnth){
    print(idxMnth)

    # Get the end time for the data this month
    yearEndIdx <- base::as.numeric(base::substr(idxMnth,start=1,stop=4))
    mnthEndIdx <- base::as.numeric(base::substr(idxMnth,start=6,stop=7))
    
    if(mnthEndIdx == 12){
      mnthEndIdx <- '01'
      yearEndIdx <- base::as.character(yearEndIdx + 1)
    } else {
      mnthEndIdx <- base::as.character(mnthEndIdx + 1)
    }
    
    timeBgnIdx <- base::strptime(base::paste0(idxMnth,'-01'),format="%Y-%m-%d",tz='GMT') # Start time
    timeEndIdx <- base::strptime(base::paste0(yearEndIdx,'-',mnthEndIdx,'-01'),format="%Y-%m-%d",tz='GMT')
    
    # Grab the data
    data[[idxMnth]] <- som::wrap.extr.neon.dp.psto(idDp=list(idDp),
                                 timeBgn=timeBgnIdx,
                                 timeEnd=timeEndIdx,
                                 MethRglr= c("none","CybiEc")[2],
                                 Freq=1/60/timeAgr,
                                 WndwRglr = c("Cntr", "Lead", "Trlg")[3],
                                 IdxWndw = c("Clst","IdxWndwMin","IdxWndwMax")[2],
                                 Srvr=c("https://den-prodpresto-1.ci.neoninternal.org:8443"), 
                                 Type=c('numc','str')[1],
                                 CredPsto=NULL,
                                 PrcsSec = 3
    )[[1]] # Just doing one data stream for now, but could do multiple
    
  } # end loop around months
  
  # Combine all the data into a single data frame and save it
  data <- do.call(rbind,data)
  save(data,file=fileData)
  
} else {
  
  # Just load it in!
  load(file=fileData)
}


urlBaseApi <- 'den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp'

# Get the asset install history
timeBgnStr <- format(data$time[1],'%Y-%m-%dT%H:%M:%OSZ')# Begin range to query asset install history
timeEndStr <- format(tail(data$time,1),'%Y-%m-%dT%H:%M:%OSZ') # End range to query asset install history
urlApi <- base::paste0(urlBaseApi,'/asset-installs?meas-strm-name=',idDp,'&install-range-begin=',timeBgnStr,'&install-range-cutoff=',timeEndStr,collapse='')
rspn <- httr::GET(url=urlApi)
cntn <- httr::content(rspn,as="text")
xml <- XML::xmlParse(cntn)

assetHist <- def.read.asst.xml.neon(asstXml = xml) # Guy's function
# assetHist$installDate <- as.POSIXct(assetHist$installDate,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")
# assetHist$removeDate <- as.POSIXct(assetHist$removeDate,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")


# Apply the calibration & drift correction for every sensor install period
data$calibrated <- NA
data$driftCorrected <- NA
for (idxInst in seq_len(nrow(assetHist))){
  
  urlApi <- base::paste0(urlBaseApi,'/calibrations?asset-stream-key=',assetHist$assetUid[idxInst],':',streamId,
                         '&startdate=','2012-01-01T00:00:00.000Z','&enddate=','2021-03-01T00:00:00.000Z',collapse='')
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
  calCoefInst <- calCoefInst[!grepl('U_CVAL',calCoefInst$Name),]
  infoCal <- list(cal=calCoefInst)
  
  # Apply calibration
  func <-
    NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALA', log = log)
  
  # Convert data using the calibration function
  setData <- data$time >= assetHist$installDate[idxInst] & 
             data$time >= calInst$validStartTime[idxCalInst] & 
             data$time < assetHist$removeDate[idxInst]
  data$calibrated[setData] <- stats::predict(object = func, newdata = data$data[setData])
  
  
  
  
  
  
  # Now get the cal that has drift coefficients for the install period (the subsequent cal has these coefs)
  calDrft <- cal[cal$validStartTime > assetHist$removeDate[idxInst],] # Restrict calibrations to those after the sensor left
  idxCalDrft <- which.min(calDrft$validStartTime-assetHist$removeDate[idxInst]) # Get the most recent cal after sensor left
  
  # Move on if we don't have a subsequent cal
  if(length(idxCalDrft) == 0){
    next
  }
  
  # Pull the drift coefficients
  calCoefDrift <- calDrft$calibrationMetadatum[idxCalDrft][[1]] # Get the calibration coefficients.
  coefDrftA <- calCoefDrift$value[calCoefDrift$name == 'U_CVALE6']
  coefDrftB <- calCoefDrift$value[calCoefDrift$name == 'U_CVALE7']
  coefDrftC <- calCoefDrift$value[calCoefDrift$name == 'U_CVALE8']
  
  # Move on if no drift coefficients
  if(length(c(coefDrftA,coefDrftB,coefDrftC)) != 3){
    next
  }
  
  # Apply drift correction
  daysSinceCal <- as.numeric(difftime(data$time,calInst$validStartTime[idxCalInst],units='days'))
  data$driftCorrected[setData] <- data$calibrated[setData]-(coefDrftA*data$calibrated[setData]^2 + 
                                                            coefDrftB*data$calibrated[setData] + 
                                                            coefDrftC)*daysSinceCal[setData]
}


# Make some plots
dataPlotRaw <- data[,names(data) %in% c('time','data')]
dataPlotRaw <- reshape2::melt(dataPlotRaw,id.vars=c('time'))
plotRaw <- plotly::plot_ly(data=dataPlotRaw, x=~time, y=~value, split = ~variable, type='scatter', mode='lines') %>%
  plotly::add_markers(x=assetHist$installDate,y=min(dataPlotRaw$value,na.rm=TRUE),name='Sensor swap',inherit=FALSE) %>%
  plotly::layout(margin = list(b = 50, t = 50, r=50),
                 title = idDp,
                 xaxis = list(title = base::paste0(c(rep("\n&nbsp;", 3),
                                                     rep("&nbsp;", 20),
                                                     paste0("Date-Time"),
                                                     rep("&nbsp;", 20)),
                                                   collapse = ""),
                              nticks=6,
                              range = dataPlotRaw$time[c(1,base::length(dataPlotRaw$time))],
                              zeroline=FALSE
                 ))
print(plotRaw)





dataPlot <- data[,names(data) %in% c('time','calibrated','driftCorrected')]
dataPlot <- reshape2::melt(dataPlot,id.vars=c('time'))

# plot<-ggplot(dataPlot,aes(time,value,colour=variable)) +
#   geom_line() + labs(title=idDp)

plot <- plotly::plot_ly(data=dataPlot, x=~time, y=~value, split = ~variable, type='scatter', mode='lines') %>%
  plotly::add_markers(x=assetHist$installDate,y=min(dataPlot$value,na.rm=TRUE),name='Sensor swap',inherit=FALSE) %>%
  plotly::layout(margin = list(b = 50, t = 50, r=50),
                 title = idDp,
                 xaxis = list(title = base::paste0(c(rep("\n&nbsp;", 3),
                                                     rep("&nbsp;", 20),
                                                     paste0("Date-Time"),
                                                     rep("&nbsp;", 20)),
                                                   collapse = ""),
                              nticks=6,
                              range = dataPlot$time[c(1,base::length(dataPlot$time))],
                              zeroline=FALSE
                 )) 
print(plot)
