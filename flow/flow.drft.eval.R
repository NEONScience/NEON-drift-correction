#' @title Flow script to analyze impact of drift correction
#' @author Cove Sturtevant, Guy Litt
#' @description Download drift-corrected L0 data from the cloud bucket and analyze it

# Changelog / contributions
#  2021-04-29 originally created, CS
#  2021-05-07 added swap gap analysis, GL
rm(list=ls())
library(googleCloudStorageR)
library(gargle)

# Import functions
mainDir <- '~/R/NEON-drift-correction/'
funcDir <- paste0(mainDir,'pack/')
flowDir <- paste0(mainDir,'flow/')
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))

# TODO Define these parameters
evalType <- c("cstm", "coLoc","allLocs","swapGapsNoDl")[4] # cstm for a custom approach (individual specification of DP IDs), coLoc to analyze pairs of co-located sensors, allLocs to grab whatever is in the bucket, swapGapsNoDl just runs the swap gap analysis wrapper
timeAgr = 5 # minutes - L0 sampling interval in which the L0 data were gathered and drift corrected. Probably 5 minutes.

# =========================================================================== #
#                        SET ENVIRONMENT/ACCESS
# =========================================================================== #
bucket <- "neon-dev-is-drift"
# Wanna see what's in there in a web browser? Go to https://console.cloud.google.com/storage/browser/neon-dev-is-drift
def.set.gcp.env(bucket=bucket)


# =========================================================================== #
#                        SPECIFY DATA STREAMS
# =========================================================================== #
# TODO Add your own idDps here, or create a way to read them in. 
# NOTE: You must have already run flow.drift.corr to create drift-corrected data in the bucket
if(evalType == "cstm"){
  # A data frame of specific DP IDs to be analyzed individually. Data will be read in and available for each one
  idDps <- data.frame(idDp="NEON.D13.WLOU.DP0.20053.001.01325.102.100.000",stringsAsFactors=FALSE)
} else if (evalType == "coLoc"){
  # A data frame of co-located DP IDs. Data will be read in and available analysis for each pair as a set
  # !!!The parameter file from GCS needs to be re-written in the format of testPara given below (in this if statement)
  # testPara <- googleCloudStorageR::gcs_get_object("params/testParaCoLoc.rds",parseFunction=googleCloudStorageR::gcs_parse_rds)
  # idDps <- base::c(testPara$idDp, testPara$idDpCoLoc) %>%  base::gsub(pattern = "[\"]", replacement = "")
  #fileParaCoLoc <- 'NR01_CMP22comparison.R'
  #fileParaCoLoc <- 'NR01_DeltaTcomparison.R'
  #fileParaCoLoc <- 'DeltaT_CMP22comparison.R'
  #fileParaCoLoc <- 'PQS1_Top2TowerLevels_Comparison.R'
  fileParaCoLoc <- 'TAAT_PRTtempComparison.R'
  #fileParaCoLoc <- 'hmp155_TAAT_tempComparison.R'
  #fileParaCoLoc <- 'MetBuoyPressureComparison.R'
  #fileParaCoLoc <- 'PRT_TROLLtemp_comparison.R'
  
  
  source(paste0(flowDir,fileParaCoLoc))
  
  # --- Indicate type of calibration uncertainty calculation. ---
  # Use 'cnst' for U_CVALA1 as a constant uncertainty, 
  # Use 'mult' for U_CVALA1 as a multiplier, 
  # Use NA to not compute uncertainty
  TypeUcrtMain=c('cnst','mult',NA)[1] 
  TypeUcrtCoLoc=c('cnst','mult',NA)[1]
  
  # Add some description for plots
  nameComp <- fileParaCoLoc
  units <- 'deg C'
  
  RstrSolNoonClrSky <- FALSE # Restrict to 1 hour around solar noon and clear sky conditions?
  RstrMidn <- FALSE # Restrict to midnight to 2 am local time?
  RstrCalDiff <- FALSE # Restrict to calibrated difference of less than a certain amount?
  CalDiffMax <- 500 # Max difference in calibrated values between co-located sensors (applies only if RstrCalDiff = TRUE)
  
  # Filter crazy convergence metrics (usually resulting from bad drift coefficients)
  maxConvAbs <- 10
  
  
} else if (evalType == "allLocs"){
  # Get the full list of available drift-corrected data in the bucket for a DP ID and term combo (and optionally filtered for a single site)
  idDpMain="DP0.20053" # DP ID (you can find the DP ID with Blizzard L0 data viewer). e.g. DP0.20053
  idTerm="01325" # Term ID (you can find the stream ID with Blizzard L0 data viewer)
  site='ARIK' # A single 4-character site code if you want to narrow down to a site. Otherwise, NULL for all sites.

  if (is.null(site)){
    prfx <- paste0('data/L1drift/',idDpMain)
  } else {
    prfx <- paste0('data/L1drift/',idDpMain,'/',site)
  }
  message('Querying the bucket...')
  files <- googleCloudStorageR::gcs_list_objects(detail="summary", prefix=prfx)
  
  files <- unlist(lapply(files,FUN=function(idx){idx$name}))
  idDps <- lapply(files$name,base::regexpr,pattern=paste0('NEON.D[0-9]{2}.[A-Z]{4}.',idDpMain,'.001.',idTerm,'.[0-9]{3}.[0-9]{3}.[0-9]{3}_',timeAgr))
  idDps <- unique(unlist(base::regmatches(files$name,idDps)))
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
  # print(googleCloudStorageR::gcs_get_object('analysis/sensorSwap/plots/DP0.00004_60GapMins_ReadingChangeFromSwapRawVsDrift.rds',parseFunction=googleCloudStorageR::gcs_parse_rds)
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
      
      # Query the bucket for all files with this DP ID
      message(paste0('Querying the bucket for all available ',idDp, ' files'))
      siteIdx <- substr(x=idDp,start=10,stop=13)
      idDpMainIdx <- substr(x=idDp,start=15,stop=23)
      #idTermIdx <- substr(x=idDp,start=29,stop=33)
      prfxIdx <- paste0('data/L1drift/',idDpMainIdx,'/',siteIdx,'/',idDp,'_',timeAgr)
      filesIdx <- googleCloudStorageR::gcs_list_objects(detail="summary", prefix=prfxIdx)$name
      
      if(length(filesIdx) == 0){
        next
      }
      
      # Open them and string them together
      message(paste0('Reading ',length(filesIdx),' files for ',idDp, ' into data frame'))
      
      data <- lapply(filesIdx,googleCloudStorageR::gcs_get_object,parseFunction=googleCloudStorageR::gcs_parse_rds)
      data <- do.call(rbind,data)
      
      # Split the DP ID to get the site
      idDpSplt <- som::def.splt.neon.id.dp.full(idDp) 
      
      # Record the site
      data$site <- idDpSplt$site
      
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
      if(RstrSolNoonClrSky){

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
        if(FALSE){
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
      if(RstrMidn){
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
      
      # Subset calibrated to within 10 degrees
      if(RstrCalDiff){
        setKeep <- abs(dataMain$calibrated - dataCoLoc$calibrated) < CalDiffMax
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
      coLocConvergence[[idxRow]] <- def.drft.coLoc(dataMain=dataMain,
                                                   dataCoLoc=dataCoLoc,
                                                   TypeUcrtMain=TypeUcrtMain,
                                                   TypeUcrtCoLoc=TypeUcrtCoLoc
      )
      
      # --------------------------------------------------------------------------- #

    } # End co-located analysis
    
  } # End loop around DP IDs

} # End download and eval section



## ------------------ SYNTHESIZE CO-LOCATED ANALYSIS ------------------------- #
coLocConvergenceAll <- do.call(rbind,coLocConvergence)

# Filter crazy convergence metrics (usually resulting from bad drift coefficients)
goodCorr <- abs(coLocConvergenceAll$medConv) < maxConvAbs
badCorr <- abs(coLocConvergenceAll$medConv) >= maxConvAbs
coLocConvergenceGood <- coLocConvergenceAll[goodCorr,]
coLocConvergenceBad <- coLocConvergenceAll[badCorr,]
if(TRUE && nrow(coLocConvergenceAll) > 0){
  coLocConvergenceAll <- coLocConvergenceGood # Filter crazy 
}

xMin <- 0
xMax <- NA

# Crunch some data to add to box plots below
DSI <- coLocConvergenceAll$DaysSinceInstall
DSIuniq <- setdiff(unique(DSI),NA)
numSamp <- unlist(lapply(DSIuniq,FUN=function(idxDSI){sum(DSI==idxDSI,na.rm=TRUE)}))
# Add the main and co-located uncertainties in quadrature, take the median, and expand to 2-sigma (95%) uncertainty 
ucrtCal <- unlist(lapply(DSIuniq,FUN=function(idxDSI){
  setDataIdx <- DSI==idxDSI
  medUcrtCalIdx <- 2*median(
                            sqrt(
                                 coLocConvergenceAll$medUcrtCalMain[setDataIdx]^2 + 
                                 coLocConvergenceAll$medUcrtCalCoLoc[setDataIdx]^2
                                 ),
                            na.rm=TRUE)
  }))
dataPlot <- data.frame(DSI=DSIuniq,numSamp=numSamp,ucrtCal=ucrtCal,ucrtCalNeg=-1*ucrtCal,idInst = '2-sigma uncertainty',stringsAsFactors=FALSE)

# Overall 2-sigma calibration uncertainty (median of the full trace)
ucrtCal <- median(ucrtCal)


# How many co-located sensors showed a change in their difference above the 2-sigma uncertainty
# We look for change rather than increase because sensors may actually drift towards each other
idInstUniq <- setdiff(unique(coLocConvergenceAll$idInst),NA)
numIdInst <- length(idInstUniq)
dmmyChar <- rep(as.character(NA),numIdInst)
dmmyNumc <- rep(as.numeric(NA),numIdInst)
coLocSmmy <- data.frame(idInst=dmmyChar,maxDaySincInst=dmmyNumc,diffChng=dmmyNumc,convMin=dmmyNumc,convMax=dmmyNumc,stringsAsFactors = FALSE)
for(idxIdInst in seq_len(numIdInst)){
  setInst <- which(coLocConvergenceAll$idInst == idInstUniq[idxIdInst])
  diffMin <- min(coLocConvergenceAll$medDiffCal[setInst],na.rm=TRUE)
  diffMax <- max(coLocConvergenceAll$medDiffCal[setInst],na.rm=TRUE)
  diffChng <- diffMax-diffMin
  coLocSmmy$idInst[idxIdInst] <- idInstUniq[idxIdInst]
  coLocSmmy$maxDaySincInst[idxIdInst] <- coLocConvergenceAll$DaysSinceInstall[tail(setInst,1)]
  coLocSmmy$diffChng[idxIdInst] <- diffChng
  convMin <- min(coLocConvergenceAll$medConv[setInst],na.rm=TRUE)
  convMax <- max(coLocConvergenceAll$medConv[setInst],na.rm=TRUE)
  coLocSmmy$convMin[idxIdInst] <- convMin
  coLocSmmy$convMax[idxIdInst] <- convMax
}
# How many (and which) co-located sensor readings changed beyond the 2-sigma uncertainty?
setDrft <- abs(coLocSmmy$diffChng) > ucrtCal # (+-) 2-sigma uncert
print(paste0(sum(setDrft),' out of ',numIdInst,' co-located sensor comparisons (',round(sum(setDrft)/numIdInst*100,0),'%) suggest drift beyond the 2-sigma uncertainty of ', round(ucrtCal,2), ' ', units))
print(paste0('They are: ',paste0(coLocSmmy$idInst[setDrft],collapse=',')))

# How many co-located sensor comparisons converged or diverged beyond the 2-sigma measurement uncertainty
setConv <- coLocSmmy$convMin < -1*ucrtCal
setDivg <- coLocSmmy$convMax > ucrtCal
print(paste0(sum(setConv),' out of ',numIdInst,' co-located sensor comparisons (',round(sum(setConv)/numIdInst*100,0),'%) converged after drift correction beyond the 2-sigma uncertainty of ', round(ucrtCal,2), ' ', units))
print(paste0('They are: ',paste0(coLocSmmy$idInst[setConv],collapse=',')))
print(paste0(sum(setDivg),' out of ',numIdInst,' co-located sensor comparisons (',round(sum(setDivg)/numIdInst*100,0),'%) diverged after drift correction beyond the 2-sigma uncertainty of ', round(ucrtCal,2), ' ', units))
print(paste0('They are: ',paste0(coLocSmmy$idInst[setDivg],collapse=',')))

# Create a box plot timeseries of co-located sensor difference (absolute) before drift correction
coLocConvergenceAll$medAbsDiffCal <- abs(coLocConvergenceAll$medDiffCal)
titl=base::paste0('Magnitude of sensor difference before drift correction')
ylab <- paste0('Absolute sensor difference [', units,']')
plotBoxDiffCal <- plotly::plot_ly(coLocConvergenceAll,x=~DaysSinceInstall,y=~medAbsDiffCal,type='box',name=ylab) %>%
  plotly::add_lines(data=dataPlot,x=~DSI,y=~ucrtCal,name=paste0('Calibration uncertainty (2-sigma) [', units,']'),
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::layout(
    yaxis=list(title=ylab),
    xaxis=list(title='Days Since Install',
               range = c(xMin,xMax)
    ),
    title=titl,
    legend=FALSE
  )
#print(plotBoxDiffCal)

# Create a box plot timeseries of convergence or divergence after drift correction
titl=base::paste0('Convergence/divergence of co-located sensors after drift correction')
ylab <- paste0('Convergence (-) or Divergence (+) [',units,']')
plotBoxConv <- plotly::plot_ly(coLocConvergenceAll,x=~DaysSinceInstall,y=~medConv,type='box',name=ylab) %>%
  plotly::add_lines(data=dataPlot,x=~DSI,y=~ucrtCal,name=paste0('Calibration uncertainty (2-sigma) [', units,']'),
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::add_lines(data=dataPlot,x=~DSI,y=~ucrtCalNeg,name=paste0('Calibration uncertainty (2-sigma) [', units,']'),
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::layout(
                 yaxis=list(title=ylab),
                 xaxis=list(title='Days Since Install',
                            range = c(xMin,xMax)
                            ),
                 title=titl,
                 legend=FALSE
  )
#print(plotBoxConv)

# Show sample size at each x axis value
titl <- 'Sample size'
ylab <- paste0('Sample size')
plotNumSamp <- plotly::plot_ly(dataPlot,x=~DSI,y=~numSamp,type='scatter',mode='markers',name=ylab) %>%
  plotly::layout(
    yaxis=list(title=ylab),
    xaxis=list(title='Days Since Install',
               range = c(xMin,xMax)
    ),
    title=nameComp,
    legend=FALSE
  )
#print(plotNumSamp)

plotAll <- plotly::subplot(plotBoxDiffCal,plotBoxConv,plotNumSamp,nrows=3,titleY=FALSE,shareX=TRUE)
print(plotAll)





# Show a scatter plot of Days since install vs. sensor difference. This will show
# individual sensor comparison traces over time
ylab <- paste0('Sensor difference [', units,']')
plot <- plotly::plot_ly(data=coLocConvergenceAll,x=~DaysSinceInstall,y=~medDiffCal,color=~idInst,type='scatter',mode='lines') %>%
  plotly::add_lines(data=dataPlot,
                    x=~DSI,
                    y=~ucrtCal,
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::add_lines(data=dataPlot,
                    x=~DSI,
                    y=~ucrtCal*-1,
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%  plotly::layout(
    yaxis=list(title=ylab),
    xaxis=list(title='Days Since Install'
    ),
    title=nameComp,
    legend=FALSE
  )
print(plot)


# Show a scatter plot of abs sensor difference vs. convergence of colocated sensors. This will show
# the degree that sensor differences are attributable to drift (and thus correctable)
xlab <- paste0('Absolute sensor difference [', units,']')
ylab <- paste0('Convergence (-) or Divergence (+) [',units,']')
plot <- plotly::plot_ly(data=coLocConvergenceAll,x=~medAbsDiffCal,y=~medConv,color=~idInst,type='scatter',mode='lines') %>%
  plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                    medAbsDiffCal=c(0,max(c(coLocConvergenceAll$medAbsDiffCal,ucrtCal),na.rm=TRUE)),
                                    ucrtCal=rep(ucrtCal,2),
                                    stringsAsFactors=FALSE),
                    x=~medAbsDiffCal,
                    y=~ucrtCal,
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                    medAbsDiffCal=c(0,max(c(coLocConvergenceAll$medAbsDiffCal,ucrtCal),na.rm=TRUE)),
                                    ucrtCal=rep(-1*ucrtCal,2),
                                    stringsAsFactors=FALSE),
                    x=~medAbsDiffCal,
                    y=~ucrtCal,
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                    medConv=c(min(c(coLocConvergenceAll$medConv,-1*ucrtCal),na.rm=TRUE),max(c(coLocConvergenceAll$medConv,ucrtCal),na.rm=TRUE)),
                                    ucrtCal=rep(ucrtCal,2),
                                    stringsAsFactors=FALSE),
                    x=~ucrtCal,
                    y=~medConv,
                    line = list(dash='dash',
                                color = 'rgb(0, 0, 0)',
                                width = 2)) %>%
  plotly::layout(
    yaxis=list(title=ylab),
    xaxis=list(title=xlab),
    title=nameComp,
    legend=FALSE
  )
print(plot)

if (FALSE){
  # Show a scatter plot of (signed) sensor difference vs. convergence of colocated sensors. This will show
  # the degree that sensor differences are attributable to drift (and thus correctable)
  xlab <- paste0('Sensor difference [', units,']')
  ylab <- paste0('Convergence (-) or Divergence (+) [',units,']')
  plot <- plotly::plot_ly(data=coLocConvergenceAll,x=~medDiffCal,y=~medConv,color=~idInst,type='scatter',mode='lines') %>%
    plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                      medDiffCal=c(min(coLocConvergenceAll$medDiffCal,na.rm=TRUE),max(coLocConvergenceAll$medDiffCal,na.rm=TRUE)),
                                      ucrtCal=rep(ucrtCal,2),
                                      stringsAsFactors=FALSE),
                      x=~medDiffCal,
                      y=~ucrtCal,
                      line = list(dash='dash',
                                  color = 'rgb(0, 0, 0)',
                                  width = 2)) %>%
    plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                      medDiffCal=c(min(coLocConvergenceAll$medDiffCal),max(coLocConvergenceAll$medDiffCal,na.rm=TRUE)),
                                      ucrtCal=rep(-1*ucrtCal,2),
                                      stringsAsFactors=FALSE),
                      x=~medDiffCal,
                      y=~ucrtCal,
                      line = list(dash='dash',
                                  color = 'rgb(0, 0, 0)',
                                  width = 2)) %>%
    plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                      medConv=c(min(coLocConvergenceAll$medConv,na.rm=TRUE),max(coLocConvergenceAll$medConv,na.rm=TRUE)),
                                      ucrtCal=rep(ucrtCal,2),
                                      stringsAsFactors=FALSE),
                      x=~ucrtCal,
                      y=~medConv,
                      line = list(dash='dash',
                                  color = 'rgb(0, 0, 0)',
                                  width = 2)) %>%
      plotly::add_lines(data=data.frame(idInst = '2-sigma uncertainty',
                                        medConv=c(min(coLocConvergenceAll$medConv,na.rm=TRUE),max(coLocConvergenceAll$medConv,na.rm=TRUE)),
                                        ucrtCal=rep(-1*ucrtCal,2),
                                        stringsAsFactors=FALSE),
                        x=~ucrtCal,
                        y=~medConv,
                        line = list(dash='dash',
                                    color = 'rgb(0, 0, 0)',
                                    width = 2)) %>%
      plotly::layout(
      yaxis=list(title=ylab),
      xaxis=list(title=xlab),
      title=nameComp,
      legend=FALSE
    )
  print(plot)
}
# --------------------------------------------------------------------------- #
  


  
# ------------------------- SWAP GAP ANALYSIS ------------------------------- #
if (evalType %in% c('swapGapsNoDl')){
  
  # Assess the data gap during sensor swaps b/w drift-corrected and plain calibrated:
  # Note this analysis automatically considers all sites available for given idDp(s)
  plotSwapGapRslt <- wrap.swap.chng.anls(idDpsVec = idDps$idDp,
                                         maxGapMins=60,
                                         manlOtlrThr = FALSE)
}
# --------------------------------------------------------------------------- #


