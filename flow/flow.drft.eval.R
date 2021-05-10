#' @title Flow script to grab presto data and save to s3 bucket
#' @author Cove Sturtevant, Guy Litt
#' @description Download drift-corrected L0 data from the S3 bucket and analyze it

# Changelog / contributions
#  2021-04-29 originally created, CS
#  2021-05-07 added swap gap analysis, GL
library(aws.s3)

# Import functions
funcDir <- '~/R/NEON-drift-correction/pack/'
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))

# TODO Define these parameters
evalType <- c("cstm", "coLoc","allLocs","swapGapsNoDl")[3] # cstm for a custom approach (individual specification of DP IDs), coLoc to analyze pairs of co-located sensors, allLocs to grab whatever is in the bucket, swapGapsNoDl just runs the swap gap analysis wrapper
timeAgr = 5 # minutes - L0 sampling interval in which the L0 data were gathered and drift corrected. Probably 5 minutes.
dlData <- TRUE # Should data be downloaded?
# =========================================================================== #
#                        SET S3 ENVIRONMENT/ACCESS
# =========================================================================== #
bucket <- "dev-is-drift"
# Wanna see what's in there in a web browser? Go to https://test-s3.data.neonscience.org/dev-is-drift/browse 
# Set the system environment for this bucket (assumes ~/.profile exists w/ secret key)
def.set.s3.env(bucket = bucket)


# =========================================================================== #
#                        SPECIFY DATA STREAMS
# =========================================================================== #
# TODO Add your own idDps here, or create a way to read them in. 
# NOTE: You must have already run flow.drift.psto.s3 to create drift-corrected data in the bucket
if(evalType == "cstm"){
  # A data frame of specific DP IDs to be analyzed individually. Data will be read in and available for each one
  idDps <- data.frame(idDp="NEON.D13.WLOU.DP0.20053.001.01325.102.100.000",stringsAsFactors=FALSE)
} else if (evalType == "coLoc"){
  # A data frame of co-located DP IDs. Data will be read in and available analysis for each pair as a set
  # !!!The parameter file from S3 needs to be re-written in the format of idDps given below (in this if statement)
  # testPara <- aws.s3::s3readRDS(object = "params/testParaCoLoc.rds",bucket = bucket)
  # idDps <- base::c(testPara$idDp, testPara$idDpCoLoc) %>%  base::gsub(pattern = "[\"]", replacement = "")
  idDps <- data.frame(idDp="NEON.D10.CPER.DP0.00022.001.01324.000.040.000", # Main DP ID
                      idDpCoLoc="NEON.D10.CPER.DP0.00023.001.01315.000.040.000", # Colocated DP ID
                         stringsAsFactors=FALSE)

} else if (evalType == "allLocs"){
  # Get the full list of available drift-corrected data in the S3 bucket for a DP ID and term combo (and optionally filtered for a single site)
  idDpMain="DP0.20053" # DP ID (you can find the DP ID with Blizzard L0 data viewer). e.g. DP0.20053
  idTerm="01325" # Term ID (you can find the stream ID with Blizzard L0 data viewer)
  site='ARIK' # A single 4-character site code if you want to narrow down to a site. Otherwise, NULL for all sites.

  if (is.null(site)){
    prfx <- paste0('data/L1drift/',idDpMain)
  } else {
    prfx <- paste0('data/L1drift/',idDpMain,'/',site)
  }
  message('Querying the S3 bucket...')
  files <- aws.s3::get_bucket(bucket=bucket,
                              prefix=prfx,
                              max=Inf
  )
  files <- unlist(lapply(files,FUN=function(idx){idx$Key}))
  idDps <- lapply(files,base::regexpr,pattern=paste0('NEON.D[0-9]{2}.[A-Z]{4}.',idDpMain,'.001.',idTerm,'.[0-9]{3}.[0-9]{3}.[0-9]{3}_',timeAgr))
  idDps <- unique(unlist(base::regmatches(files,idDps)))
  idDps <- substr(idDps,start=1,stop=45)

  idDps <- data.frame(idDp=idDps,stringsAsFactors=FALSE)
} else if (evalType == "swapGapsNoDl"){
  # If interested in conducting the analysis on swapGaps using existing drift-corrected data in the bucket,
  # skip the DOWNLOAD DRIFT CORRECTED DATA  section and skip to ANALYSIS
  dlData <- FALSE
  allDpIdzVerTerm <- c("DP0.00003.001.TERMN","DP0.00004.001.TERMN",
                       "DP0.00014.001.01332","DP0.00014.001.01333","DP0.00022.001.TERMN",
                       "DP0.00024.001.01320","DP0.00024.001.01321","DP0.00066.001.TERMN",
                       "DP0.00098.001.01309","DP0.00098.001.01357",
                       "DP0.20016.001.TERMN","DP0.20042.001.TERMN", "DP0.20053.001.TERMN",
                       "DP0.20261.001.01320","DP0.20261.001.01321",
                       "DP0.20264.001.02887","DP0.20264.001.02888","DP0.20264.001.02889","DP0.20264.001.02890","DP0.20264.001.02891","DP0.20264.001.02892","DP0.20264.001.02893","DP0.20264.001.02894","DP0.20264.001.02895","DP0.20264.001.02896") # 02887, 02888, 02889, 02890, 02891, 02892, 02893, 02894, 02895, 02896
  idDps <- data.frame(idDp=paste0("NEON.DOM.SITE.",allDpIdzVerTerm,".HOR.VER.TMI"),stringsAsFactors=FALSE)
  # Note that once this is run for a given dp, and assuming not interested in updating plots with recent results, result plots may be accessed as follows:
  # print(aws.s3::s3readRDS(object='analysis/sensorSwap/plots/DP0.00004_60GapMins_ReadingChangeFromSwapRawVsDrift.rds', bucket = bucket)) # using DP0.00004 (baro P) as an example
}

# =========================================================================== #
#                      DOWNLOAD DRIFT CORRECTED DATA
# =========================================================================== #
if (dlData){
  # If we're doing co-located analysis, we'll pass through the download twice to get both the main sensor and colocated sensor 
  if(evalType == 'coLoc'){
    numIter <- 2 
  } else {
    numIter <- 1
  }
  
  for(idxRow in seq_len(nrow(idDps))){
    dataMain <- NULL
    dataCoLoc <- NULL
    
    # Run through the both main and co-located sensor as necessary
    for(idxIter in seq_len(numIter)){
      
      # main sensor or co-located?
      if(idxIter == 1){
        # Main sensor
        idDp <- idDps$idDp[idxRow] # Full DP ID
      } else if (idxIter == 2) {
        # Co-located sensor
        idDp <- idDps$idDpCoLoc[idxRow] # Full DP ID
      }
      
      # Query the S3 bucket for all files with this DP ID
      message(paste0('Querying the S3 bucket for all available ',idDp, ' files'))
      siteIdx <- substr(x=idDp,start=10,stop=13)
      idDpMainIdx <- substr(x=idDp,start=15,stop=23)
      #idTermIdx <- substr(x=idDp,start=29,stop=33)
      prfxIdx <- paste0('data/L1drift/',idDpMainIdx,'/',siteIdx,'/',idDp,'_',timeAgr)
      filesIdx <- aws.s3::get_bucket(bucket=bucket,
                                     prefix=prfxIdx,
                                     max=Inf
      )
      filesIdx <- sort(unlist(lapply(filesIdx,FUN=function(idx){idx$Key})))
      
      # Open them and string them together
      message(paste0('Reading ',length(filesIdx),' files for ',idDp, ' into data frame'))
      
      data <- lapply(filesIdx,aws.s3::s3readRDS,bucket = bucket)
      data <- do.call(rbind,data)
      
      # Record as the main sensor or co-located sensor
      if(idxIter == 1){
        # Main sensor
        dataMain <- data
      } else if (idxIter == 2) {
        # Co-located sensor
        dataCoLoc <- data
      }
      
    }
    
    # ========================================================================= #
    #                               Plots 
    # ========================================================================= #
    # Plot the main sensor
    message(paste0('Plotting ',idDps$idDp[idxRow]))
    def.plot.drft(data=dataMain,idDp=idDps$idDp[idxRow])
    
    # plot the co-located sensor
    if(evalType == 'coLoc'){
      message(paste0('Plotting ',idDps$idDpCoLoc[idxRow]))
      def.plot.drft(data=dataCoLoc,idDp=idDps$idDpCoLoc[idxRow])
    }
    
  }
}

# =========================================================================== #
#                               ANALYSIS 
# =========================================================================== #


# ------------------------- SWAP GAP ANALYSIS ------------------------------- #
# Assess the data gap during sensor swaps b/w drift-corrected and plain calibrated:
# Note this analysis automatically considers all sites available for given idDp(s)
plotSwapGapRslt <- wrap.swap.chng.anls(idDpsVec = idDps$idDp,
                                       bucket = bucket,
                                       maxGapMins=60,
                                       manlOtlrThr = FALSE)
# --------------------------------------------------------------------------- #
# TODO assess drift periods using the instDate column (sensor swap date) in dataMain and/or dataCoLoc 


