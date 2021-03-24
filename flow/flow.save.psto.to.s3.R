#' @title Flow script to grab presto data and save to s3 bucket
#' @author Guy Litt
#' @description Given a list of full DP0 NEON DP IDs for (dlType="cstm"),
#' download data from presto and place in s3 dev-is-drift bucket

# Changelog / contributions
#  2021-03-23 originally created, GL

library(aws.s3)
library(som)
# Import functions
funcDir <- '~/R/NEON-drift-correction/pack/'
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))

# TODO Define these parameters
dlType <- c("cstm", "coLoc")[1] # cstm for a custom approach
ymBgn <- '2018-01' # YYYY-MM begin month
ymEnd <- '2021-01' # YYYY-MM end month
timeAgr <- 5 # The time aggregation interval in mins

# =========================================================================== #
# TODO Add your own idDps here, or create a way to read them in
if(dlType == "cstm"){
  idDps <- base::c("NEON.D13.WLOU.DP0.20053.001.01325.102.100.000") 
} else if (dlType == "coLoc"){
  testPara <- aws.s3::s3readRDS(object = "params/testParaCoLoc.rds",bucket = bucket)
  idDps <- base::unique(base::c(testPara$idDp, testPara$idDpCoLoc))
}
# =========================================================================== #
#                        SET S3 ENVIRONMENT/ACCESS
# =========================================================================== #
bucket <- "dev-is-drift"
# Set the system environment for this bucket (assumes ~/.profile exists w/ secret key)
def.set.s3.env(bucket = bucket)


# =========================================================================== #
#                                DOWNLOAD DATA
# =========================================================================== #
for(idDp in idDps){
  # Either download or read in the L0 data for the time period of interest. 
  #  Note, it is wise to downsample this data to every 5 min or so
  if(base::grepl("DP0", idDp)){
    data <- def.dl.data.psto.s3.drft(idDp = idDp,fldrBase = NULL,
                                     ymBgn = ymBgn, ymEnd = ymEnd,
                                     timeAgr = timeAgr,
                                     bucket = bucket,
                                     makeNewFldr = FALSE)
  } else {
    stop("Cannot download non-presto data (presently only DP0)")
  }
}
