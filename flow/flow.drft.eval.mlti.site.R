# Code to evaluate CVAL drift correction 
# Two methods of validating the drift. 
#  1. Does the step change in values between one installation and the next diminish after applying the drift correction
#  2. Do the values of co-located replicate sensors converge after applying drift correction

# TODO refer to avro schemas for stream ID: https://github.battelleecology.org/Engineering/avro-schemas/tree/develop/schemas

rm(list=ls())


funcDir <- '~/R/NEON-drift-correction/pack/'
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))
library('ggplot2')
library('plotly')

baseFileDir <- '~/analysesQAQC/drift/testData/'
plotSaveDir <- '~/analysesQAQC/drift/results/'
# PRT
idDp <- 'NEON.D13.WLOU.DP0.20053.001.01325.102.100.000' # Resistance
streamId <- 0 # RH 
term <- "PRT" # an informal term to use
# TROLL
idDpCoLoc <- base::gsub("20053.001.01325","20016.001.01378",idDp)
streamIdCoLoc <- 2 
termCoLoc <- "TROLL" # an informal term to use


urlBaseApi <- 'den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp'
# Read in the file setting up test parameters
testPara <- read.csv(paste0(baseFileDir,"DriftTestParameters.csv"), stringsAsFactors = FALSE)


for(idx in 3:base::nrow(testPara)){ # Loop by each test scenario of interest
  # Define Parameters
  idDp <- base::gsub("[\"]","",testPara$idDp[idx])
  idDpCoLoc <- base::gsub("[\"]","",testPara$idDpCoLoc[idx])
  streamId <- testPara$streamId[idx]
  streamIdCoLoc <- testPara$streamIdCoLoc[idx]
  term <- testPara$term[idx]
  termCoLoc <- testPara$termCoLoc[idx]
  
  # ========================================================================= #
  #  ------------------------ GRAB & CORRECT DATA --------------------------  #
  # ========================================================================= #
  # Either download or read in the L0 data for the time period of interest. Note, it is wise to downsample this data to every 5 min or so
  fileData=paste0(baseFileDir,idDp,'.RData') # Name of the data file we'll either be saving or loading
  data <- try(wrap.load.data.psto.drft(fileData = fileData,idDp = idDp, dnld = FALSE))
  if(base::nrow(data) == 0 || 'try-error' %in% base::class(data)){
    data <- wrap.load.data.psto.drft(fileData = fileData,idDp = idDp, dnld = TRUE)
  }

  # Calibrate and drift-correct
  rtrnDrft <- wrap.cal.corr.drft(idDp = idDp, data = data, streamId = streamId, urlBaseApi = urlBaseApi, timeCol = "time", dataCol = "data")
  data <- rtrnDrft$data
  assetHist <- rtrnDrft$assetHist
  
  # ========================================================================= #
  #  ---------------------------- CO-LOCATED DATA --------------------------  #
  # ========================================================================= #
  # Co-located dpID
  fileDataCo=paste0(baseFileDir,idDpCoLoc,'.RData') # Name of the data file we'll either be saving or loading
  dataCoLoc <- base::try(wrap.load.data.psto.drft(fileData = fileDataCo,idDp = idDpCoLoc, dnld = FALSE))
  if(base::nrow(dataCoLoc) == 0 || 'try-error' %in% base::class(dataCoLoc)){
    dataCoLoc <- wrap.load.data.psto.drft(fileData = fileDataCo,idDp = idDpCoLoc, dnld = TRUE)
  }
  # Calibrate and drift-correct
  rtrnCoLoc <- wrap.cal.corr.drft(idDp = idDpCoLoc, data = dataCoLoc, streamId = streamIdCoLoc,
                                  urlBaseApi = urlBaseApi, timeCol = "time", dataCol = "data")
  
  dataCoLoc <- rtrnCoLoc$data
  asstHistCoLoc <- rtrnCoLoc$assetHist
  
  # ========================================================================= #
  #  ------------------------------- ANALYSIS  -----------------------------  #
  # ========================================================================= #
  
  spltDps <- som::def.splt.neon.id.dp.full(c(idDp,idDpCoLoc))
  
  d <- data
  dcl <- dataCoLoc
  # Merge datasets and compute difference, Assumes regularized timestamps:
  def.cmbn.data.coloc <- function(d, dcl, term, termCoLoc){
    
    
    
  }
  colnames(d) <- base::paste0(base::colnames(d), term)
  colnames(dcl) <- base::paste0(colnames(dcl), termCoLoc)
  
  
  cmboData <- base::merge(x = d, y = dcl, by.x = paste0("time",term), by.y = paste0("time",termCoLoc),)
  
  cmboData$diffCal <- cmboData[,paste0('calibrated',term)] - cmboData[,paste0('calibrated',termCoLoc)]
  cmboData$diffDrftCorr <- cmboData[,paste0('driftCorrected',term)] - cmboData[,paste0('driftCorrected',termCoLoc)]
  
  
  # Subset to just the data with drift:
  subCmboData <- cmboData[!base::is.na(cmboData$diffDrftCorr),]
  
  sdDiffAbsCal <- stats::sd(base::abs(subCmboData$diffCal), na.rm = TRUE)
  
  sdDiffAbsDrft <- stats::sd(base::abs(subCmboData$diffDrftCorr), na.rm = TRUE)
  
  rsltDiff <- base::paste(spltDps$site[1], spltDps$hor[1], term, "vs.",termCoLoc, "\nDiff Calib Abs SD:", base::round(sdDiffAbsCal,digits = 3), "/ Diff Drift Abs SD: ", base::round(sdDiffAbsDrft, digits = 3))
  
  
  # ========================================================================= #
  #  ------------------------------- PLOTTING  -----------------------------  #
  # ========================================================================= #
  
  # Difference analysis
  plotDiff <- def.plot.cal.drft(data = cmboData, assetHist = assetHist, idDp = rsltDiff, timeCol = "timePRT", colsPlot = c("diffCal","diffDrftCorr"))
  saveDiffPlotName <- base::paste0(plotSaveDir,"Diff_",idDp, "_vs_", idDpCoLoc,".html")
  htmlwidgets::saveWidget(widget = plotDiff, file = saveDiffPlotName)
  
  
  
  # Just PRT Plots:
  plotDrftAndNonPrt <- def.plot.cal.drft(data = data, assetHist = assetHist, idDp = idDp, term = term)
  plotDrftOnlyPrt <- def.plot.cal.drft(data = data, assetHist = assetHist, idDp = idDp, colsPlot = c('driftCorrected'), term = term) 
  plotCalOnlyPrt <-def.plot.cal.drft(data = data, assetHist = assetHist, idDp = idDp,colsPlot = c('calibrated'), term = term)
  
  saveDrftCalPlotPrtName <-  base::paste0(plotSaveDir,"CalibDrift_",idDp,"_",term, ".html")
  htmlwidgets::saveWidget(widget = plotDrftAndNonPrt, file = saveDrftCalPlotPrtName)
  
  
  # Just TROLL Plots:
  plotDrftAndNonTrll <- def.plot.cal.drft(data = dataCoLoc, assetHist = asstHistCoLoc, idDp = idDpCoLoc, term = termCoLoc)
  plotCalOnlyTrll <-def.plot.cal.drft(data = dataCoLoc, assetHist = asstHistCoLoc, idDp = idDpCoLoc,colsPlot = c('calibrated'), term = termCoLoc)
  plotDrftOnlyTrll <- def.plot.cal.drft(data = dataCoLoc, assetHist = asstHistCoLoc, idDp = idDpCoLoc, colsPlot = c('driftCorrected'), term = termCoLoc) 
  
  saveDrftCalPlotTrllName <-  base::paste0(plotSaveDir,"DriftCorrected_",idDpCoLoc,"_",termCoLoc, ".html")
  htmlwidgets::saveWidget(widget = plotDrftAndNonTrll, file = saveDrftCalPlotTrllName)
  
  # ------------------------------------------------------------------------- #
  # Bringing it all together - all PRT/TROLL calibrated & drift
  plotDrftAndNonAll <- def.plot.cal.drft(data = dataCoLoc, p = plotDrftAndNonPrt, assetHist = asstHistCoLoc, idDp = base::c(idDp, idDpCoLoc), term = termCoLoc)
  
  # PRT/TROLL just calibrated:
  plotCalOnlyAll <-  def.plot.cal.drft(data = dataCoLoc, p = plotCalOnlyPrt, assetHist = asstHistCoLoc, idDp = base::c(idDp, idDpCoLoc), colsPlot = c('calibrated'), term = termCoLoc)
  
  # PRT/TROLL just drift-corrected:
  plotDrftOnlyAll <-  def.plot.cal.drft(data = dataCoLoc, p = plotDrftOnlyPrt, assetHist = asstHistCoLoc, idDp = base::c(idDp, idDpCoLoc), colsPlot = c('driftCorrected'), term = termCoLoc)
  bothDrftOnlyName <- base::paste0(plotSaveDir,"BothDriftOnly",idDp, "_vs_", idDpCoLoc,".html")
  htmlwidgets::saveWidget(widget = plotDrftOnlyAll, file = bothDrftOnlyName)
  
  
}

