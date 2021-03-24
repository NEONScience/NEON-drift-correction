#' @title Set environmental variables for bucket access
#' @author Guy Litt
#' @description Sets system environment credentials for s3 bucket access,
#' which relies on reading a file that contains the secret access key
#' @param bucket Optional bucket name, purely for testing the s3 connection. Default 'dev-is-drift' corresponds to other defaults.
#' @param filePath Default "~/.profile". The file containing the secret access key, with a line beginning with \code{scrtTitl} followed by the access key
#' @param AWS_S3_ENDPOINT Default "neonscience.org"
#' @param AWS_ACCESS_KEY_ID The access key ID. This should differ by bucket and read vs. write access. Default 'dev-is-drift'
#' @param AWS_DEFAULT_REGION Default 'test-s3.data'
#' @param scrtTitl The string title that precedes the same-line secret key in the \code{filePath}. Default 'AWS_DRIFT_SECRET='
#' 
#' @export
#' 
# Changelog / contributions
#    2021-03-23 Originally created
def.set.s3.env <- function(bucket = "dev-is-drift",
                           filePath = "~/.profile",
                           AWS_S3_ENDPOINT="neonscience.org",
                           AWS_ACCESS_KEY_ID = "dev-is-drift",
                           AWS_DEFAULT_REGION = c("test-s3.data","s3.data")[1],
                           scrtTitl = "AWS_DRIFT_SECRET="
                          ){
  if(!base::file.exists(filePath)){
    stop(base::paste0(filePath, " does not exist. Create this with the secret access key of interest"))
  }
  
  CRED_USR <-  utils::read.delim(filePath, sep = "\n",header = 0)
  
  if(!base::any(base::grepl(scrtTitl, CRED_USR[[1]])) ){
    stop(base::paste0(scrtTitl, " does not exist in ", filePath))
  }
  
  AWS_SECRET_ACCESS_KEY <-  base::gsub(scrtTitl,"",CRED_USR[grep(scrtTitl,CRED_USR[[1]]),])
  
  # Ensure AWS environment variables assigned
  base::Sys.setenv("AWS_S3_ENDPOINT" = AWS_S3_ENDPOINT,
                   "AWS_ACCESS_KEY_ID" = AWS_ACCESS_KEY_ID,
                   "AWS_SECRET_ACCESS_KEY" = AWS_SECRET_ACCESS_KEY,
                   "AWS_DEFAULT_REGION" = AWS_DEFAULT_REGION)
  
  if(!base::is.null(bucket)){
    if(!aws.s3::bucket_exists(bucket = bucket)){
      stop(base::paste0("Cannot connect to ",bucket, " bucket. Check bucket name, AWS environmental variables, and file that stores the secret access key."))
    } else {
      base::message(base::paste0("Successful connection to the ", bucket, " bucket."))
    }
  } 
}
  
  