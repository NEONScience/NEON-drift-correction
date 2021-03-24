#' @title NEON ECS S3 bucket design for drift working group
#' @author Guy Litt
#' @description This script shouldn't run much - just sets up the bucket.
#'  Users will be able to create their own sub-folders as needed

# Changelog / contributions
#   2021-03-23 originally created, GL

rm(list=ls())
library(aws.s3)

# Enter in dp IDs for sub-folder data storage:
dpIdsL1 <- base::c("DP1.20053","DP1.20016")
dpIdsL0 <- base::c("DP0.00098", "DP0.20016")
# =========================================================================== #
# --------------------------------------------------------------------------- #
#                    READ IN SECRET ACCESS KEY
# --------------------------------------------------------------------------- #
CRED_USR <-  utils::read.delim("~/.profile", sep = "\n",header = 0)
AWS_SECRET_ACCESS_KEY <-  base::gsub("AWS_DRIFT_SECRET=","",CRED_USR[grep("AWS_DRIFT_SECRET=",CRED_USR[[1]]),])
# Ensure AWS environment variables assigned
base::Sys.setenv("AWS_S3_ENDPOINT" =  "neonscience.org",
                 "AWS_ACCESS_KEY_ID" = "dev-is-drift",
                 "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
                 "AWS_DEFAULT_REGION" = "test-s3.data")
# The name of this bucket
bucket <- "dev-is-drift"
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
if(aws.s3::bucket_exists(bucket = "dev-is-drift")){
  print("building environment")
  
  # Folder creation
  # Create a folder in the bucket for data
  aws.s3::put_folder(folder = "data", bucket = bucket)
  aws.s3::put_folder(folder = "data/L0", bucket = bucket)
  aws.s3::put_folder(folder = "data/L1", bucket = bucket)
  aws.s3::put_folder(folder = "data/L1drift", bucket = bucket)
  aws.s3::put_folder(folder = "data/L0calib", bucket = bucket) # Calibrated L0 data goes here 
  aws.s3::put_folder(folder = "data/CVAL/", bucket = bucket) # CVAL coeffs can go here
  
  # 
  for(dpId in dpIdsL1){
    if(!aws.s3::object_exists(paste0("data/L1/",dpId), bucket = bucket)){
      aws.s3::put_folder(folder = paste0("data/L1/",dpId), bucket = bucket)
    }
  }
  
  for(dpId in dpIdsL0){
    if(!aws.s3::object_exists(paste0("data/L0/",dpId), bucket = bucket)){
      aws.s3::put_folder(folder = paste0("data/L0/",dpId), bucket = bucket)
    }
  }
  
  for(dpId in dpIdsL0){
    if(!aws.s3::object_exists(paste0("data/L0calib/",dpId), bucket = bucket)){
      aws.s3::put_folder(folder = paste0("data/L0calib/",dpId), bucket = bucket)
    }
  }
  
  # Test parameters can be stored here:
  aws.s3::put_folder(folder = "params", bucket = bucket)
  
  # Analysis results can go here (sub-folders within each preferred)
  aws.s3::put_folder(folder="analysis", bucket = bucket)
  aws.s3::put_folder(folder="analysis/colocation", bucket)
  aws.s3::put_folder(folder="analysis/colocation/plots", bucket)
  aws.s3::put_folder(folder="analysis/colocation/tables", bucket)
}
