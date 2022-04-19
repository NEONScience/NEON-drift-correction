#' @title Function for creating standard DP directory structure in cloud-based bucket
#' @author Cove Sturtevant
#' @details Standardizes the directories to the following:
#' data/L1drift/DPN.XXXXX/site/
#' data/L0calib/DP0.XXXXX/site/
#' data/L0/DP0.XXXXX/site/
#' data/L1/DP1.XXXXX/site/
#' NOTE: requires that you run def.set.gcp.env to perform authentication and set bucket
#' @param idDp the full NEON data product id of interest
#' @param fldrBase Base folder path, non data-stream specific path. If NULL (Default), infers the specific path for L0 or L1 data based on provided DP ID
#' @param type Default NULL assumes standard data as defined in idDp. Optionally may specify 'driftCorr' for drift-corrected, or 'calibCorr' for calibration-corrected.
#' @param timeAgr Time aggregation interval in minutes to be added to file save string. Default NA

#' @return base folder object name for saving to cloud bucket

# Changelog / Contributions
#   2021-04-18 original creation, after def.crea.s3.fldr

def.get.bckt.fldr <- function(
  idDp,
  fldrBase = NULL,
  type = c(NULL,"driftCorr","calibCorr")[1], 
  timeAgr = NA
  ){
  
  spltDp <- som::def.splt.neon.id.dp.full(idDp)
  if(base::is.null(fldrBase)){
    # Infer bucket read/write folder and filename
    lvlId <- base::unique(base::paste0(spltDp$lvl,".",spltDp$id))
    
    if(!base::is.null(type)){
      if(type =="driftCorr"){
        fldr <- base::paste0("data/L1drift/",lvlId)
      } else if (type == "L0calib"){
        fldr <- base::paste0("data/L0calib/",lvlId)
      }
    } else if(base::grepl("DP0",lvlId)){
      fldr <- base::paste0("data/L0/",lvlId)
    } else if (base::grepl("DP1",lvlId)){
      fldr <- base::paste0("data/L1/",lvlId)
    }
    
    metaBucket=base::try(googleCloudStorageR::gcs_get_bucket())
    if(base::class(metaBucket)=="try-error"){
      stop(paste0("Bucket: ",googleCloudStorageR::gcs_get_global_bucket(), ' does not exist or you have not authenticated in GCS. Please resolve to continue.'))
    }
    
    # The base file directory structure + filename
    fileBase <- base::paste0(fldr,"/",spltDp$site,"/", idDp,"_",timeAgr)
    
  } else {
    # Use provided directory path 
    fileBase <- base::paste0(fldrBase,"/",spltDp$site,"/",idDp,"_",timeAgr)
  }
  
  return(fileBase)
}