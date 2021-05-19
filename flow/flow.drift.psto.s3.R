#' @title Flow script to grab presto data and save to s3 bucket
#' @author Guy Litt, Cove Sturtevant
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
dlType <- c("cstm", "coLoc","allLocs")[3] # cstm for a custom approach (individual specification of DP IDs), coLoc to read in a parameter file of paired ID DPs that are colocated, allLocs to dynamically retrieve DP IDs for all locations
ymBgn <- '2018-01' # YYYY-MM begin month
ymEnd <- '2021-01' # YYYY-MM end month
timeAgr <- 5 # L0 time sampling interval in mins
urlBaseApi <- 'den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp'

# =========================================================================== #
#                        SET S3 ENVIRONMENT/ACCESS
# =========================================================================== #
bucket <- "dev-is-drift"
# Set the system environment for this bucket (assumes ~/.profile exists w/ secret key)
def.set.s3.env(bucket = bucket)


# =========================================================================== #
#                        SPECIFY DATA STREAMS
# =========================================================================== #
# TODO Add your own idDps here, or create a way to read them in
if(dlType == "cstm"){
  idDps <- data.frame(idDp="NEON.D13.WLOU.DP0.20053.001.01325.102.100.000",funcCal="def.cal.conv.poly",stringsAsFactors=FALSE)
} else if (dlType == "coLoc"){
  # !!!The parameter file from S3 needs to be re-written in the format of testPara given below (in this if statement)
  # testPara <- aws.s3::s3readRDS(object = "params/testParaCoLoc.rds",bucket = bucket)
  # idDps <- base::c(testPara$idDp, testPara$idDpCoLoc) %>%  base::gsub(pattern = "[\"]", replacement = "")
  testPara <- data.frame(idDp="NEON.D10.CPER.DP0.00022.001.01324.000.040.000",funcCal="def.cal.conv.poly",
                      idDpCoLoc="NEON.D10.CPER.DP0.00023.001.01315.000.040.000",funcCalCoLoc="def.cal.conv.poly",
                      stringsAsFactors=FALSE)
  # DON'T EDIT THE FOLLOWING LINE
  idDps <- data.frame(idDp=c(testPara$idDp,testPara$idDpCoLoc),funcCal=c(testPara$funcCal,testPara$funcCalCoLoc),
                      stringsAsFactors=FALSE) # Merge main and coLoc sensors for download. 
} else if (dlType == "allLocs"){
  # Get the full list of all possible instances (locations) of a data product. 
  # Note, not all may be active or real, the code below will ignore any non-existent locs
  # To use this section, you MUST be running the code from den-devissom-1
  Para <- list(
    # list(
    #   idDpMain="DP0.20053.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01325", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20016.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01378", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00098.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01357", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal= NA # The function from NEONprocIS.cal to apply the calibration. NA for no calibration conversion.
    # ),
    # list(
    #   idDpMain="DP0.00098.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01309", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal= NA # The function from NEONprocIS.cal to apply the calibration. NA for no calibration conversion.
    # ),
    # list(
    #   idDpMain="DP0.00003.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01325", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00004.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01311", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    list(
      idDpMain="DP0.00022.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
      idTerm="01324", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
      site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
      funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    )
    # list(
    #   idDpMain="DP0.00023.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01316", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00023.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01315", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00024.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01320", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00024.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01321", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00014.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01332", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00014.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01333", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.00066.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01329", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20042.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01320", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20261.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01320", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20261.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="01321", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02887", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02888", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02889", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02890", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02891", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02892", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02893", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02894", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02895", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="02896", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # ),
    # list(
    #   idDpMain="DP0.20264.001", # DP ID (you can find the DP ID with Blizzard L0 data viewer)
    #   idTerm="05516", # Term ID (you can find the stream ID with Blizzard L0 data viewer)
    #   site=NULL, # NULL to retrieve all sites. Otherwise, a character vector of NEON site codes, e.g. c('CPER','BART')
    #   funcCal='def.cal.conv.poly' # The function from NEONprocIS.cal to apply the calibration
    # )
  )

  idDps <- c() 
  idDps <- base::lapply(Para,FUN=function(idxPara){
    message(paste0(idxPara$idDpMain,',',idxPara$idTerm,',',idxPara$site))
    idDpsIdx <- def.dp.list(idDpMain=idxPara$idDpMain,
                            idTerm=idxPara$idTerm,
                            site=idxPara$site
                            )
    if(is.null(idxPara$funcCal)){
      idxPara$funcCal <- NA
    }
    rpt <- data.frame(idDp=idDpsIdx,funcCal=idxPara$funcCal,stringsAsFactors=FALSE)
    return(rpt)
  })
  
  idDps <- do.call(rbind,idDps)
}

# =========================================================================== #
#                      DOWNLOAD DATA & DRIFT CORRECT
# =========================================================================== #
tryMax <- 3 # Number of tries to download data from Presto without error
for(idxRow in seq_len(nrow(idDps))){
  idDp <- idDps$idDp[idxRow] # Full DP ID
  funcCal <- idDps$funcCal[idxRow] # Calibration function

  # Find streamId
  streamId <- som::def.extr.neon.cal.api(idDp=idDp,
                                         timeBgn=base::as.POSIXct(paste0(ymBgn,'-01'),tz='GMT'),
                                         timeEnd=base::as.POSIXct(paste0(ymEnd,'-28'),tz='GMT')
  )
  # Move on if not found. Likely the DP ID is not active or does not exist.
  streamId <- base::unique(streamId$sensorStreamNum)
  if(base::is.null(streamId) ){
    message(paste0(idDp, 'does not exist. Skipping...'))
    next
  } else if (base::length(streamId) != 1 && !base::is.na(streamId)){
    message(paste0('Cannot unambiguously determine calibration stream ID for ',
    idDp, 
    '. Potential values are [',paste0(streamId,collapse=','),']. Skipping...'))
    next
  }
  
  
  # Either download or read in the L0 data for the time period of interest. 
  #  Note, it is wise to downsample this data to every 5 min or so
  if(base::grepl("DP0", idDp)){
    idxTry <- 0
    while (idxTry <= tryMax) {
      data <- try(def.dl.data.psto.s3.drft(idDp = idDp,fldrBase = NULL,
                                           ymBgn = ymBgn, ymEnd = ymEnd,
                                           timeAgr = timeAgr,
                                           bucket = bucket,
                                           makeNewFldr = FALSE),
                  silent=FALSE)
      if('try-error' %in% base::class(data)){
        idxTry <- idxTry + 1
        Sys.sleep(60*5) # Wait for some time and try again
      } else {
        idxTry <- tryMax + 1
      }
    }

  } else {
    message(paste0('Cannot download ',idDp, '. Only presto data (presently only DP0) can be downloaded at this time. Skipping...'))
    next
  }
  

  # ========================================================================= #
  #                 Calibrate, drift-correct, & write
  # ========================================================================= #
  rtrnDrft <- wrap.cal.corr.drft(idDp = idDp, 
                                 data = data, 
                                 streamId = streamId, 
                                 funcCal = funcCal, 
                                 urlBaseApi = urlBaseApi, 
                                 timeCol = "time", 
                                 dataCol = "data")
  dataDrft <- rtrnDrft$data
  assetHist <- rtrnDrft$assetHist
  
  # ------------------------------------------------------------------------- #
  #          Subset drift-corrected data & write to S3 bucket
  # ------------------------------------------------------------------------- #
  # Commenting this for now. Do filtering later on analysis in case the non-drift corrected periods after last install are worth anything.
  #dataDrft <- dataDrft %>% base::subset(drftCorrFlag) %>% data.table::as.data.table() 
  
  # Split drift-corrected data into monthly datasets & Write to S3
  dataDrft$yearMnth <- paste0(lubridate::year(dataDrft$time), "-", stringr::str_pad(lubridate::month(dataDrft$time), 2, pad = "0"))
  lsDrft <- base::split(dataDrft,dataDrft$yearMnth)
  for(ym in base::names(lsDrft) ){
    baseDirNam <- def.crea.s3.fldr(idDp,fldrBase = NULL, type ="driftCorr", bucket = bucket, timeAgr = timeAgr, makeNewFldr = FALSE)
    fullPathNam <- base::paste0(baseDirNam,"_", ym,"_driftCorr.rds")
    dataDrftMnth <- lsDrft[[ym]] %>% dplyr::select(-"yearMnth")
    aws.s3::s3saveRDS(x = dataDrftMnth, object = fullPathNam, bucket = bucket)
  }
  
}
