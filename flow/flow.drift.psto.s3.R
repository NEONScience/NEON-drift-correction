#' @title Flow script to grab presto data and save to s3 bucket
#' @author Guy Litt
#' @description Given a list of full DP0 NEON DP IDs for (dlType="cstm"),
#' download data from presto and place in s3 dev-is-drift bucket

# Changelog / contributions
#  2021-03-23 originally created, GL

library(aws.s3)
library(som)
library(data.table)
library(stringr)
library(dplyr)
# Import functions
funcDir <- '~/R/NEON-drift-correction/pack/'
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))

# TODO Define these parameters
dlType <- c("cstm", "coLoc")[1] # cstm for a custom approach
ymBgn <- '2018-01' # YYYY-MM begin month
ymEnd <- '2021-01' # YYYY-MM end month
timeAgr <- 5 # The time aggregation interval in mins
urlBaseApi <- 'den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp'

# =========================================================================== #
# TODO Add your own idDps here, or create a way to read them in
if(dlType == "cstm"){
  idDps <- base::c("NEON.D13.WLOU.DP0.20053.001.01325.102.100.000") 
  streamIds <- base::c(0)
} else if (dlType == "coLoc"){
  testPara <- aws.s3::s3readRDS(object = "params/testParaCoLoc.rds",bucket = bucket)
  idDps <- base::c(testPara$idDp, testPara$idDpCoLoc) %>%  base::gsub(pattern = "[\"]", replacement = "")
  streamIds <- base::c(testPara$streamId, testPara$streamIdCoLoc)
  
}
# =========================================================================== #
#                        SET S3 ENVIRONMENT/ACCESS
# =========================================================================== #
bucket <- "dev-is-drift"
# Set the system environment for this bucket (assumes ~/.profile exists w/ secret key)
def.set.s3.env(bucket = bucket)


# =========================================================================== #
#                      DOWNLOAD DATA & DRIFT CORRECT
# =========================================================================== #
ctr <- 0
for(idDp in idDps){
  ctr = ctr + 1
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
  
  # TODO create automated method to find streamId
  
  # ========================================================================= #
  #                 Calibrate, drift-correct, & write
  # ========================================================================= #
  rtrnDrft <- wrap.cal.corr.drft(idDp = idDp, data = data, streamId = streamIds[ctr], urlBaseApi = urlBaseApi, timeCol = "time", dataCol = "data")
  dataDrft <- rtrnDrft$data
  assetHist <- rtrnDrft$assetHist
  
  
  # ------------------------------------------------------------------------- #
  #          Subset drift-corrected data & write to S3 bucket
  # ------------------------------------------------------------------------- #
  dataDrft <- dataDrft %>% base::subset(drftCorrFlag) %>% data.table::as.data.table()
  
  # Split drift-corrected data into monthly datasets & Write to S3
  dataDrft$yearMnth <- paste0(lubridate::year(dataDrft$time), "-", stringr::str_pad(lubridate::month(dataDrft$time), 2, pad = "0"))
  lsDrft <- dataDrft %>% base::split(by = c("yearMnth"))
  for(ym in base::names(lsDrft) ){
    baseDirNam <- def.crea.s3.fldr(idDp,fldrBase = NULL, type ="driftCorr", bucket = bucket, timeAgr = timeAgr, makeNewFldr = FALSE)
    fullPathNam <- base::paste0(baseDirNam,"_", ym,"_driftCorr.rds")
    dataDrftMnth <- lsDrft[[ym]] %>% dplyr::select(-"yearMnth")
    aws.s3::s3saveRDS(x = dataDrftMnth, object = fullPathNam, bucket = bucket)
  }
  
  # ========================================================================= #
  #                               ANALYSIS 
  # ========================================================================= #
  
  # TODO assess drift periods using the instDate column in dataDrft as identifier
  
}
