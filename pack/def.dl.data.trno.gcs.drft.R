#' @title Function to download data from Trino for drift assessemnt and store in GCS
#' @author Cove Sturtevant, Guy Litt
#' @description Reads data from GCS bucket across a time range by 
#' DPID/year-month for a given time aggregation interval (default 5 mins).
#' If data do not exist in bucket, download data from Trino and write to GCS bucket.
#' NOTE: requires that you run def.set.gcp.env to perform authentication and set bucket
#' @param idDp the Full NEON DP ID, e.g. "NEON.D13.WLOU.DP0.20053.001.01325.102.100.000"
#' @param fldrBase Base folder path, non data-stream specific path. If NULL (Default), infers the specific path for L0 or L1 data based on provided DP ID
#' @param ymBgn Begin year-month in YYYY-mm format
#' @param ymEnd End year-month in YYYY-mm format
#' @param timeAgr Time aggregation interval in minutes, default 5
#' 
#' @return Dataframe of a dpID across time range of interest
# Changelog
#    2022-04-18 Cove Sturtevant
#       original creation, after def.dl.data.psto.s3.drft.R
# ---------------------------------------------------------------
def.dl.data.trno.gcs.drft <- function(
  idDp, 
  fldrBase = NULL, 
  ymBgn = '2018-01',
  ymEnd = '2021-01', 
  timeAgr = 5 # minutes - desired L0 sampling interval
  ){
  

  # Return standard DP directory structure
  fileBase <- def.get.bckt.fldr(idDp=idDp,
                                 fldrBase=fldrBase,
                                 type=NULL,
                                 timeAgr=timeAgr)
  
  # ========================================================================= #
  #                                 GRAB DATA
  # ========================================================================= #
  timeDist <- base::as.difftime(timeAgr,units="mins") # Time aggregation interval in minutes
  
  yearEnd <- base::as.numeric(base::substr(ymEnd,start=1,stop=4))
  mnthEnd <- base::as.numeric(base::substr(ymEnd,start=6,stop=7))
  
  if(mnthEnd == 12){
    mnthEnd <- '01'
    yearEnd <- base::as.character(yearEnd + 1)
  } else {
    mnthEnd <- base::as.character(mnthEnd + 1)
  }
  
  # Set up the output time sequence
  timeBgn <- base::strptime(base::paste0(ymBgn,'-01'),format="%Y-%m-%d",tz='GMT') # Start time
  timeEnd <- base::strptime(base::paste0(yearEnd,'-',mnthEnd,'-01'),format="%Y-%m-%d",tz='GMT')-timeDist
  timeBgn <- base::as.POSIXct(base::seq.POSIXt(from=timeBgn,to=timeEnd,by=timeDist,tz="GMT"))
  timeEnd <- timeBgn + timeDist
  numData <- base::length(timeBgn)
  
  # We're going to grab data for each month and string it together
  yearMnth <- base::unique(base::format(timeBgn,format='%Y-%m'))
  data <- base::vector(mode='list',length=length(yearMnth)) # Initialize
  base::names(data) <- yearMnth
  
  for(idxMnth in yearMnth){
    message(base::paste0("Grabbing ",idDp, ' ',idxMnth))
    
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
    
    # Check to see if file exists
    saveName <- base::paste0(fileBase, "_",idxMnth,".rds")
    objExst <- googleCloudStorageR::gcs_list_objects(detail="summary", prefix=saveName)
    
    if(base::nrow(objExst) == 1){
      # Read in data already inside bucket
      data[[idxMnth]] <- googleCloudStorageR::gcs_get_object(saveName,parseFunction=googleCloudStorageR::gcs_parse_rds)

    } else {
      # Grab the data then write it to bucket
      data[[idxMnth]] <- som::wrap.extr.neon.dp.psto(idDp=list(idDp),
                                                     timeBgn=timeBgnIdx,
                                                     timeEnd=timeEndIdx,
                                                     MethRglr= c("none","CybiEc")[2],
                                                     Freq=1/60/timeAgr,
                                                     WndwRglr = c("Cntr", "Lead", "Trlg")[3],
                                                     IdxWndw = c("Clst","IdxWndwMin","IdxWndwMax")[2],
                                                     Srvr=c("https://trino1.gcp.neoninternal.org:443"), 
                                                     Type=c('numc','str')[1],
                                                     CredPsto=NULL,
                                                     PrcsSec = 3
      )[[1]] # Just doing one data stream for now, but could do multiple
      data$exst <- NULL # Get rid of unnecessary column
      
      # Write monthly data to bucket
      googleCloudStorageR::gcs_upload(file=data[[idxMnth]],
                                      name=saveName,
                                      object_function=
                                        function(input,output){
                                          base::saveRDS(object=input,file=output)
                                          },
                                      predefinedAcl='bucketLevel')
    }

  } # end loop around months
  
  # Combine all the data into a single data frame and save it
  data <- base::do.call(rbind,data)
    
  return(data)
}