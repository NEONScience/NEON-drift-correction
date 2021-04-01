#' @title Function for creating standard DP directory structure in s3 bucket
#' @author Guy Litt
#' @details Standardizes the S3 directories to the following:
#' data/L1drift/DPN.XXXXX/site/
#' data/L0calib/DP0.XXXXX/site/
#' data/L0/DP0.XXXXX/site/
#' data/L1/DP1.XXXXX/site/
#' @param idDp the full NEON data product id of interest
#' @param fldrBase Base folder path, non data-stream specific path. If NULL (Default), infers the specific path for L0 or L1 data based on provided DP ID
#' @param type Default NULL assumes standard data as defined in idDp. Optionally may specify 'driftCorr' for drift-corrected, or 'calibCorr' for calibration-corrected.
#' @param timeAgr Time aggregation interval in minutes to be added to file save string. Default NA
#' @param bucket S3 bucket name, default dev-is-drift
#' @param makeNewFldr Boolean - should we create a new folder to store data? Default FALSE.

#' @return base folder object name for saving to s3 bucket

#' @seealso def.dl.data.pst.s3.drft

# Changelog / Contributions
#   2021-03-23 originally created

def.crea.s3.fldr <- function(
  idDp,
  fldrBase = NULL,
  type = c(NULL,"driftCorr","calibCorr")[1], 
  bucket = "dev-is-drift", 
  timeAgr = NA, 
  makeNewFldr = FALSE
  ){
  
  spltDp <- som::def.splt.neon.id.dp.full(idDp)
  if(base::is.null(fldrBase)){
    # Infer S3 bucket read/write folder and filename
    lvlId <- base::unique(base::paste0(spltDp$lvl,".",spltDp$id))
    
    if(!base::is.null(type)){
      if(type =="driftCorr"){
        fldrS3 <- base::paste0("data/L1drift/",lvlId)
      } else if (type == "L0calib"){
        fldrS3 <- base::paste0("data/L0calib/",lvlId)
      }
    } else if(base::grepl("DP0",lvlId)){
      fldrS3 <- base::paste0("data/L0/",lvlId)
    } else if (base::grepl("DP1",lvlId)){
      fldrS3 <- base::paste0("data/L1/",lvlId)
    }
    
    if(!aws.s3::bucket_exists(bucket = bucket, object = fldrS3) && makeNewFldr == FALSE){
      stop(paste0(fldrS3, " does not exist yet in ",bucket,". Check the path provided in fldrBase. If you really need to create a new folder, set makeNewFldr = TRUE ") )
    }
    # The base file directory structure + filename
    fileS3base <- base::paste0(fldrS3,"/",spltDp$site,"/", idDp,"_",timeAgr)
    
  } else {
    # Use provided directory path 
    fileS3base <- base::paste0(fldrBase,"/",spltDp$site,"/",idDp,"_",timeAgr)
  }
  
  return(fileS3base)
}