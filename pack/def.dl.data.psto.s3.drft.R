#' @title wrapper function to download data from presto for drift assessemnt
#' @author Cove Sturtevant, Guy Litt
#' @description Reads data from S3 bucket across a time range by
#'  DPID/year-month for a given time aggregation interval (default 5 mins).
#' If data do not exist in bucket, download data from presto and write to
#' bucket.
#' @param idDp the Full NEON DP ID, e.g. "NEON.D13.WLOU.DP0.20053.001.01325.102.100.000"
#' @param fldrBase Base folder path, non data-stream specific path. If NULL (Default), infers the specific path for L0 or L1 data based on provided DP ID
#' @param ymBgn Begin year-month in YYYY-mm format
#' @param ymEnd End year-month in YYYY-mm format
#' @param timeAgr Time aggregation interval in minutes, default 5
#' @param bucket S3 bucket name, default dev-is-drift
#' @param makeNewFldr Boolean - should we create a new folder to store data? Default FALSE.
#' 
#' @return Dataframe of a dpID across time range of interest
# Changelog
#    2021-Mar Created by Cove, adapted to wrapper by Guy
#    2020-03-23 Adapted to write to s3 bucket instead of local location, GL
def.dl.data.psto.s3.drft <- function(idDp, fldrBase = NULL, ymBgn = '2018-01' , ymEnd = '2021-01', timeAgr = 5, bucket = 'dev-is-drift', makeNewFldr = FALSE){
  # Check that s3 credentials defined.
  if(Sys.getenv("AWS_SECRET_ACCESS_KEY")==""){
    stop("Must define the AWS_SECRET_ACCESS_KEY to use s3 bucket")
  }
  
  spltDp <- som::def.splt.neon.id.dp.full(idDp)
  if(base::is.null(fldrBase)){
    # Infer S3 bucket read/write folder and filename
    lvlId <- base::unique(base::paste0(spltDp$lvl,".",spltDp$id))
    
    if(base::grepl("DP0",lvlId)){
      fldrS3 <- base::paste0("data/L0/",lvlId)
    } else if (base::grepl("DP1",lvlId)){
      fldrS3 <- base::paste0("data/L1/",lvlId)
    }
    
    if(!aws.s3::bucket_exists(fldrS3, bucket = bucket) && makeNewFldr == FALSE){
      stop(paste0(fldrS3, " does not exist yet in ",bucket,". Check the path provided in fldrBase. If you really need to create a new folder, set makeNewFldr = TRUE ") )
    }
    # The base file directory structure + filename
    fileS3base <- base::paste0(fldrS3,"/",spltDp$site,"/", idDp,"_",timeAgr)
    
  } else {
    # Use provided directory path 
    fileS3base <- base::paste0(fldrBase,"/",spltDp$site,"/",idDp,"_",timeAgr)
  }
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
    
    # Check to see if file exists
    saveName <- base::paste0(fileS3base, "_",idxMnth,".rds")
    
    if(aws.s3::object_exists(saveName, bucket = bucket)){
      # Read in data already inside bucket
      data[[idxMnth]] <- aws.s3::s3readRDS(saveName, bucket = bucket)

    } else {
      # Grab the data from presto then write it to s3 bucket
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
      
      # Write monthly S3 data to bucket
      aws.s3::s3saveRDS(x = data[[idxMnth]], object = saveName, bucket = bucket)
    }

  } # end loop around months
  
  # Combine all the data into a single data frame and save it
  data <- base::do.call(rbind,data)
    
  return(data)
}