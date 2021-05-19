#' @title Flow script to grab presto data and save to s3 bucket
#' @author Cove Sturtevant, Guy Litt
#' @description Download drift-corrected L0 data from the S3 bucket and analyze it

# Changelog / contributions
#  2021-04-29 originally created, CS
#  2021-05-07 added swap gap analysis, GL
rm(list=ls())
library(aws.s3)

# Import functions
mainDir <- '~/R/NEON-drift-correction/'
funcDir <- paste0(mainDir,'pack/')
flowDir <- paste0(mainDir,'flow/')
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))

# TODO Define these parameters
evalType <- c("cstm", "coLoc","allLocs","swapGapsNoDl")[2] # cstm for a custom approach (individual specification of DP IDs), coLoc to analyze pairs of co-located sensors, allLocs to grab whatever is in the bucket, swapGapsNoDl just runs the swap gap analysis wrapper
timeAgr = 5 # minutes - L0 sampling interval in which the L0 data were gathered and drift corrected. Probably 5 minutes.

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
  #source(paste0(flowDir,'NR01_CMP22comparison.R'))
  #source(paste0(flowDir,'NR01_DeltaTcomparison.R'))
  #source(paste0(flowDir,'DeltaT_CMP22comparison.R'))
  source(paste0(flowDir,'hmp155_TAAT_tempComparison.R'))
  

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
  # Conducting the analysis on swapGaps using existing drift-corrected data in the bucket
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
#                        Load in NEON site info
# =========================================================================== #
fileDomnSite <- "/scratch/SOM/dataProductInfo/DomainSiteList.csv" # List of domains and associated sites (domain, state, etc.)
DomnSite <- utils::read.csv(file=fileDomnSite,header=TRUE,stringsAsFactors=FALSE)


if (evalType %in% c('cstm', 'coLoc', 'allLocs')){
  
  # =========================================================================== #
  #                      DOWNLOAD DRIFT CORRECTED DATA
  # =========================================================================== #
  
  # If we're doing co-located analysis, we'll pass through the download twice to get both the main sensor and colocated sensor 
  if(evalType == 'coLoc'){
    numIter <- 2 
    coLocConvergence <- list()
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
      
      if(length(filesIdx) == 0){
        next
      }
      
      # Open them and string them together
      message(paste0('Reading ',length(filesIdx),' files for ',idDp, ' into data frame'))
      
      data <- lapply(filesIdx,aws.s3::s3readRDS,bucket = bucket)
      data <- do.call(rbind,data)
      
      # Split the DP ID to get the site
      idDpSplt <- som::def.splt.neon.id.dp.full(idDp) 
      
      # Get location information
      loc <- jsonify::from_json(paste0('https://data.neonscience.org/api/v0/locations/',idDpSplt$site))$data
      lat <- loc$locationDecimalLatitude
      lon <- loc$locationDecimalLongitude
      elev <- loc$locationElevation
      
      # Get zenith angle
      data$zenith<-GeoLight::zenith(sun = GeoLight::solar(data$time),
                                    lon = lon,
                                    lat=lat)
      
      # Get theoretical solar insolation (making assumptions for RH and temperature)
      jday <- as.POSIXlt(data$time)$yday 
      theoSol <- insol::insolation(zenith = data$zenith,
                                   jd = jday,
                                   height = elev, #converting from feet to meters
                                   visibility = 10,
                                   RH = 30,
                                   tempK=288.15, #temperature has to be in kelvins
                                   O3 = 0.003,
                                   alphag = 0.2)
      theoSol <- as.data.frame(theoSol,stringsAsFactors = FALSE)
      data$theoSol <- theoSol$In + theoSol$Id # Direct + diffuse
      
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
    if (FALSE){
      
      # Plot the main sensor
      message(paste0('Plotting ',idDps$idDp[idxRow]))
      def.plot.drft(data=dataMain,idDp=idDps$idDp[idxRow])
      
      # plot the co-located sensor
      if(evalType == 'coLoc'){
        message(paste0('Plotting ',idDps$idDpCoLoc[idxRow]))
        def.plot.drft(data=dataCoLoc,idDp=idDps$idDpCoLoc[idxRow])
        
        def.plot.drft.coLoc(dataMain=dataMain,
                            dataCoLoc=dataCoLoc,
                            idDpMain=idDps$idDp[idxRow],
                            idDpCoLoc=idDps$idDpCoLoc[idxRow])
        
      }
      
    }
    
    # =========================================================================== #
    #                               ANALYSIS 
    # =========================================================================== #
    
    # ------------------------- CO-LOCATED ANALYSIS ------------------------------- #
    if(evalType == 'coLoc' && !is.null(dataMain) && !is.null(dataCoLoc)){

      # Restrict data to one hour around solar noon and clear-sky
      if(FALSE){

        # Convert all times to local standard time (assume co-located at same site)
        tz <- DomnSite$TimeZone[DomnSite$SiteCode == idDpSplt$site]
        timeDataMain <- dataMain$time
        timeDataCoLoc <- dataCoLoc$time
        attr(timeDataMain,'tzone') <- tz
        attr(timeDataCoLoc,'tzone') <- tz

        # Subset to within 1 hour of solar noon
        timeDataMain <- as.POSIXlt(timeDataMain)
        timeDataCoLoc <- as.POSIXlt(timeDataCoLoc)
        dataMain <- subset(dataMain,subset=timeDataMain$hour >= 10 & timeDataMain$hour < 14)
        dataCoLoc <- subset(dataCoLoc,subset=timeDataCoLoc$hour >= 10 & timeDataCoLoc$hour < 14)
        
        # Restrict to aligned daily peaks (within 10 min)
        if(TRUE){
          if(nrow(dataMain) != nrow(dataCoLoc)){
            message('Need same data size for main and co-located sensors. Skipping...')
            next
          }
          nDiv <- 60*4/timeAgr # Divide up into daily chunks. Must be the total number of points per day after restricting to solar noon bracket
          dataMainCal <- matrix(dataMain$calibrated,nrow=nDiv,ncol=nrow(dataMain)/nDiv) # Each column is the subset
          dataCoLocCal <- matrix(dataCoLoc$calibrated,nrow=nDiv,ncol=nrow(dataCoLoc)/nDiv) # Each column is the subset
          setMaxMain <- apply(dataMainCal,2,FUN=function(col){
            x <- which.max(col)
            if(length(x) >= 1){
              return(x[1])
            } else {
              return(NA)
            }
            }
          )
          setMaxCoLoc <- apply(dataCoLocCal,2,FUN=function(col){
            x <- which.max(col)
            if(length(x) >= 1){
              return(x[1])
            } else {
              return(NA)
            }
          }
          )
          filt <- rep(TRUE,length(setMaxMain)) # set up a filter for non-aligned series
          filt[abs(setMaxMain-setMaxCoLoc) > 2] <- FALSE
          setKeep <- rep(filt,rep(nDiv,dim(dataMainCal)[2]))
          dataMain <- subset(dataMain,subset=setKeep)
          dataCoLoc <- subset(dataCoLoc,subset=setKeep)
        }
        
        # Subset main sensor to clear-sky
        nDiv <- 60/timeAgr # Divide up into n consecutive data points. Must be clean divider of the number of points in each day after restricting to solar noon bracket
        diffSolTheo <- dataMain$theoSol-dataMain$calibrated
        diffSolTheoMat <- matrix(diffSolTheo,nrow=nDiv,ncol=nrow(dataMain)/nDiv) # Each column is the subset
        sdDiffTheoMat <- apply(diffSolTheoMat,2,sd) # Take standard deviation of each column 
        sdDiffTheo <- rep(sdDiffTheoMat,rep(nDiv,dim(diffSolTheoMat)[2]))
        setKeep <- sdDiffTheo < 6 & diffSolTheo < 400 & dataMain$calibrated > 200 # Criteria for clear sky
        dataMain <- subset(dataMain,subset=setKeep)

        # Subset co-located sensor to clear-sky
        nDiv <- 60/timeAgr # Divide up into n consecutive data points. Must be clean divider of the number of points in each day after restricting to solar noon bracket
        diffSolTheo <- dataCoLoc$theoSol-dataCoLoc$calibrated
        diffSolTheoMat <- matrix(diffSolTheo,nrow=nDiv,ncol=nrow(dataCoLoc)/nDiv) # Each column is the subset
        sdDiffTheoMat <- apply(diffSolTheoMat,2,sd) # Take standard deviation of each column 
        sdDiffTheo <- rep(sdDiffTheoMat,rep(nDiv,dim(diffSolTheoMat)[2]))
        setKeep <- sdDiffTheo < 6 & diffSolTheo < 400 & dataCoLoc$calibrated > 200 # Criteria for clear sky
        dataCoLoc <- subset(dataCoLoc,subset=setKeep)

      }
      
      # Restrict data to midnight
      if(TRUE){
        stop()
        # Convert all times to local standard time (assume co-located at same site)
        tz <- DomnSite$TimeZone[DomnSite$SiteCode == idDpSplt$site]
        timeDataMain <- dataMain$time
        timeDataCoLoc <- dataCoLoc$time
        attr(timeDataMain,'tzone') <- tz
        attr(timeDataCoLoc,'tzone') <- tz
        
        # Subset to midnight to 2 am
        timeDataMain <- as.POSIXlt(timeDataMain)
        timeDataCoLoc <- as.POSIXlt(timeDataCoLoc)
        dataMain <- subset(dataMain,subset=timeDataMain$hour >= 0 & timeDataMain$hour < 2)
        dataCoLoc <- subset(dataCoLoc,subset=timeDataMain$hour >= 0 & timeDataMain$hour < 2)
        
        # Subset to within 10 degrees
        if(nrow(dataMain) != nrow(dataCoLoc)){
          message('Need same data size for main and co-located sensors. Skipping...')
          next
        }
        setKeep <- abs(dataMain$calibrated - dataCoLoc$calibrated) < 10
        dataMain <- subset(dataMain,subset=setKeep)
        dataCoLoc <- subset(dataCoLoc,subset=setKeep)
      }
      
      if(nrow(dataMain) == 0 || nrow(dataCoLoc) == 0){
        message('Filtering removed all rows. Skipping...')
        next
      }
      
      # Assess how the spread of co-located sensor data changes with drift-correction applied
      # Negative values indicate a convergence of the co-located sensor values after drift correction
      # Positive values indicate a divergence of the co-located sensor values after drift correction
      coLocConvergence[[idxRow]] <- def.drft.coLoc(dataMain,dataCoLoc)
      # --------------------------------------------------------------------------- #

    } # End co-located analysis
    
  } # End loop around DP IDs

} # End download and eval section



# ------------------ SYNTHESIZE CO-LOCATED ANALYSIS ------------------------- #
coLocConvergenceAll <- do.call(rbind,coLocConvergence)
names(coLocConvergenceAll)[1] <- 'DaysSinceInstall'
# Create a box plot timeseries
titl=base::paste0('Convergence/divergence of co-located NR01 & DeltaT sensors after drift correction')

plotBoxConv <- plotly::plot_ly(coLocConvergenceAll,x=~DaysSinceInstall,y=~mean,type='box') %>%
  plotly::layout(
                 yaxis=list(title='Convergence (-) or Divergence (+) [W m-2]'),
                 xaxis=list(title='Days Since Install',
                            range = c(30,420)
                            ),
                 title=titl
  )
print(plotBoxConv)

# --------------------------------------------------------------------------- #
  


  
# ------------------------- SWAP GAP ANALYSIS ------------------------------- #
if (evalType %in% c('swapGapsNoDl')){
  
  # Assess the data gap during sensor swaps b/w drift-corrected and plain calibrated:
  # Note this analysis automatically considers all sites available for given idDp(s)
  plotSwapGapRslt <- wrap.swap.chng.anls(idDpsVec = idDps$idDp,
                                         bucket = bucket,
                                         maxGapMins=60,
                                         manlOtlrThr = FALSE)
}
# --------------------------------------------------------------------------- #


