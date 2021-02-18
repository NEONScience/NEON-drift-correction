# Code to evaluate CVAL drift correction 
# Two methods of validating the drift. 
#  1. Does the step change in values between one installation and the next diminish after applying the drift correction
#  2. Do the values of co-located replicate sensors converge after applying drift correction
rm(list=ls())

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

# Get the asset install history - This isn't working - for some reason the xml parsing is copying the first entry.
timeBgnStr <- '2017-01-01T00:00:00.000Z' # Begin range to query asset install history
timeEndStr <- '2021-02-01T00:00:00.000Z' # End range to query asset install history
urlApi <- base::paste0(urlBaseApi,'/asset-installs?meas-strm-name=',idDp,'&install-range-begin=',timeBgnStr,'&install-range-cutoff=',timeEndStr,collapse='')
rspn <- httr::GET(url=urlApi)
cntn <- httr::content(rspn,as="text")
xml <- XML::xmlParse(cntn)
xml = XML::xmlTreeParse(cntn)
assetHist <- XML::xmlToList(xml$doc$children$assetInstallList)

# Reading in by hand...
assetHist <- data.frame(uid=c('45912',
                              '35418',
                              '7106',
                              '30931'
                              ),
                        timeBgn=
                                c('2020-08-26T17:12:12.000Z',
                                  '2019-05-30T15:49:11.000Z',
                                  '2018-06-01T16:27:44.000Z',
                                  '2017-05-31T15:52:14.000Z'
                                  ),
                        timeEnd=
                                c(NA,
                                  '2020-08-26T17:11:15.000Z',
                                  '2019-05-30T15:48:23.000Z',
                                  '2018-06-01T14:21:11.000Z'
                                  ),
                        stringsAsFactors=FALSE
                        )
assetHist$timeBgnPosx <- as.POSIXct(assetHist$timeBgn,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")
assetHist$timeEndPosx <- as.POSIXct(assetHist$timeEnd,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")

# Now get some calibration files for the 2nd install sensor (the one right before last)
idxInst <- 2
urlApi <- base::paste0(urlBaseApi,'/calibrations?asset-stream-key=',assetHist$uid[idxInst],':',streamId,
                       '&startdate=',assetHist$timeBgn[idxInst],'&enddate=','2021-03-01T00:00:00.000Z',collapse='')
rspn <- httr::GET(url=urlApi,httr::add_headers(Accept = "application/json"))
cntn <- httr::content(rspn,as="text")
cal <- jsonlite::fromJSON(cntn,simplifyDataFrame=T, flatten=T)$calibration
cal$validStartTime <- base::as.POSIXct('1970-01-01',tz="GMT")+cal$validStartTime/1000 # milliseconds since 1970
cal$validEndTime <- base::as.POSIXct('1970-01-01',tz="GMT")+cal$validEndTime/1000 # milliseconds since 1970

# Get the calibration coefficients for the install period
calInst <- cal[cal$validStartTime < assetHist$timeBgnPosx[idxInst],] # Restrict calibrations to those before the sensor was installed
idxCalInst <- which.min(assetHist$timeBgnPosx[idxInst]-calInst$validStartTime) # Get the most recent cal prior to sensor install
calCoefInst <- calInst$calibrationMetadatum[idxCalInst][[1]] # Get the calibration coefficients.


# Get the cal that has drift coefficients for the install period (the subsequent cal has these coefs)
calDrft <- cal[cal$validStartTime > assetHist$timeEndPosx[idxInst],] # Restrict calibrations to those after the sensor left
idxCalDrft <- which.min(calDrft$validStartTime-assetHist$timeEndPosx[idxInst]) # Get the most recent cal after sensor left
calCoefDrift <- calDrft$calibrationMetadatum[idxCalDrft][[1]] # Get the calibration coefficients.






# Apply the calibration coefficients to the L0 data

# Apply the drift coefficients

