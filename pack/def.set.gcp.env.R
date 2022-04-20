#' @title Set GCP configuration for drift bucket access
#' @author Cove Sturtevant
#' @description Retrieves and passes credentials to GCP. Sets project and bucket. User may need to log in. 
#' @param bucket Optional. Bucket name. Default 'neon-dev-is-drift'.
#' 
#' @export
#' 
# Changelog / contributions
#  2022-04-18 Cove Sturtevant
#     migrate to trino and GCP
# ---------------------------------------------------
def.set.gcp.env <- function(bucket = "neon-dev-is-drift"
  ){

  # Retrive and pass crediantials to GCP
  token <- gargle::token_fetch(scope='https://www.googleapis.com/auth/cloud-platform')
  googleCloudStorageR::gcs_auth(token = token)
  
  # Set the global bucket name
  googleCloudStorageR:: gcs_global_bucket(bucket=bucket)
  
}
  
  