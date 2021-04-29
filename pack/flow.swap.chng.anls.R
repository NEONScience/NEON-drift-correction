#' @title Assess reading changes during sensor swaps
#' @author Guy Litt
#' @description For a given sensor stream, identify periods
#' when sensor swaps are performed within a maxGapMins threshold.
#' Then calculate the difference from the post-swap reading to the 
#' pre-swap reading. Do this for both the calibrated readings
#' and the drift corrected readings. Generate a box plot of these 
#' differences for each data product.
#' @seealso flow.save.psto.s3.R

# Changelog / Contributions
#   2021-04-29 Guy Litt, originally created

library(aws.s3)
library(tidyverse)
library(stringr)
library(dplyr)
# Import functions
funcDir <- '~/R/NEON-drift-correction/pack/'
sapply(list.files(funcDir, pattern = ".R"), function(x) source(paste0(funcDir,x)))

# TODO Define these parameters
bucket <- "dev-is-drift"
drftCol <- "driftCorrected"
timeCol <- "time"
maxGapMins <- 60 # The max time gap in mins allowed between sensor swaps
otlrDiffThr <- 10 # The threshold for outlier exclusion in calibrated units (only if manlOtlrThr=TRUE)
manlOtlrThr <- FALSE # Should a manual outlier exclusion be performed? Set to FALSE for auto outlier removal
dpIdz <- c("DP0.00003","DP0.00004","DP0.00022","DP0.00024","DP0.00098","DP0.20016", "DP0.20053")
# =========================================================================== #
#                        SET S3 ENVIRONMENT/ACCESS
# =========================================================================== #

# Set the system environment for this bucket (assumes ~/.profile exists w/ secret key)
def.set.s3.env(bucket = bucket)

# Read in site-dp matrix
siteDpMat <- utils::read.csv("/scratch/SOM/dataProductInfo/SiteProductInstanceMatrixDp00.csv")
# Remove "MD00" column
siteDpMat <- siteDpMat %>% dplyr::select(-"MD00")


rowsDps <- base::unlist(base::lapply(dpIdz, function(dp) base::grep(dp,siteDpMat$idDp)))

colsSitz <- base::lapply(rowsDps, function(i) (which(siteDpMat[i,] != 0)))
colsSitz <- base::lapply(1:length(colsSitz), function(i) colsSitz[[i]][colsSitz[[i]]>6])# The 7th column is where site names begin                                                             
sitzDpz <- base::lapply(1:length(colsSitz), function(i) colnames(siteDpMat)[colsSitz[[i]]])
base::names(sitzDpz) <- dpIdz
# aws.s3::delete_object(object = "data/L1drift/DP1.20053/",bucket = bucket)

# siteNamzAIS <- c("ARIK","BIGC","BLDE","BLUE","CARI","CUPE","GUIL","HOPB","KING","LECO","LEWI","MART","MAYF","MCDI","MCRA","OKSR","POSE","PRIN","REDB","SYCA","TECR","WALK","WLOU")


allPlotGapDiff <- base::list()
for(dpId in base::names(sitzDpz)){
  message(paste0("Grabbing data from: ", aws.s3::get_bucketname(bucket = bucket, x = paste0('data/L1drift/',dpId,"/")) ))
  
  
  drftNamz <- aws.s3::get_bucket_df(bucket = bucket, prefix = paste0('data/L1drift/',dpId,"/"))$Key
  drftNamz <- drftNamz[base::grep(".rds",drftNamz)]
  
  plotTsGapLs <- base::list()
  swapGapLs <- base::list()
  siteNamz <- sitzDpz[[dpId]]
  for(site in siteNamz){
    dpSiteId <- paste0('data/L1drift/',dpId,"/",site,"/")
    drftNamz <- aws.s3::get_bucket_df(bucket = bucket, prefix = dpSiteId)$Key
    
    if(base::length(drftNamz) == 0){
      warning(paste0("No data for ", dpId, " at ", site))
      next()
    }
    
    # Split by HOR.VER:
    endIdxStr <- stringr::str_locate(pattern = dpSiteId, string = drftNamz)[,"end"]
    horVersMl <- base::substring(drftNamz,first = (endIdxStr + 35),last = (endIdxStr + 45))
    
    # simplify to HOR.VER if the HOR.VER.TMI has the same TMI throughout.
    if(length(base::unique(base::substring(horVersMl,first = 8, last = 11))) == 1){
      horVersMl <- base::substring(horVersMl, first = 1, last = 7)
    }
    
    for(horVer in base::unique(horVersMl)){
      drftNamzSub <- drftNamz[base::which(horVersMl==horVer)]
      
      lsDrftSiteLoc <- base::lapply(drftNamzSub, function(dn) aws.s3::s3readRDS(object = dn, bucket = bucket))
      
      dtDrft <- data.table::rbindlist(lsDrftSiteLoc) #%>% base::subset(exst == 1)
      
      if(base::nrow(dtDrft) == 0){
        next()
      }
      
      subDtDrft <- dtDrft %>% base::subset(exst == 1)
      
      
      
  
      # Identify the gaps between sensor swaps
      swapGaps <- def.id.swap.gap(subDtDrft)
      
      
      swapGaps$diffCalb <- NA
      swapGaps$diffDrft <- NA
      
      idxsSwapChck <- base::which(swapGaps$diffMins <= maxGapMins)
      if(base::length(idxsSwapChck)>0){
        
      
        idxsRealDataEnd <- base::unlist(base::lapply(1:length(idxsSwapChck),
                               function(i) base::which(dtDrft[[timeCol]] == swapGaps$dataEnd[idxsSwapChck[i]-1])))
        idxsRealDataBgn <- base::unlist(base::lapply(1:length(idxsSwapChck),
                               function(i) 
                                base::which(dtDrft[[timeCol]] == swapGaps$dataBgn[idxsSwapChck[i]]) ))
        
        if(length(idxsRealDataEnd) != length(idxsRealDataBgn)){
          stop("Problem with index matching to timestamps")
        }
        
        
        swapGaps$diffCalb[idxsSwapChck] <-  base::unlist(base::lapply(1:base::length(idxsRealDataBgn), function(i) 
            dtDrft[idxsRealDataBgn[i], "calibrated"] - dtDrft[idxsRealDataEnd[i],"calibrated"]) )
        
      
        swapGaps$diffDrft[idxsSwapChck] <- base::unlist(base::lapply(1:base::length(idxsRealDataBgn), function(i) 
          dtDrft[idxsRealDataBgn[i], "driftCorrected"] - dtDrft[idxsRealDataEnd[i],"driftCorrected"]) )
        
        # ------------------------------------------------------------------- #
        # TODO generate a plot of the data and driftCorrected around the time range of the swap:
        idxsSubBgn <- idxsRealDataBgn - 20*6
        idxsSubEnd <- idxsRealDataBgn + 20*6
        
        subData <- base::lapply(1:length(idxsSubBgn), function(i) dtDrft[idxsSubBgn[i]:idxsSubEnd[i],])
        
        for (idx in 1:length(subData)){
          
          subMlt <- subData[[idx]] %>% data.table::melt.data.table(id.vars = c("time"), measure.vars = c("calibrated","driftCorrected"))
          
          
          plotTsGap <- subMlt %>% ggplot2::ggplot(aes(x=time, y=value, color = variable)) + 
            geom_point() +
            ggtitle(paste(site,dpId,horVer,"sensor swap readings")) +
            ylab(paste0("calibrated and drift-corrected ", dpId))
          
          plotTsGapLs[[paste0(site,horVer,"_",base::as.Date(subData[[idx]]$time[1]))]] <- plotTsGap
        }
      }
      
      swapGaps$dpId <- dpId
      swapGaps$site <- site
      swapGaps$horVer <- horVer
      swapGapLs[[paste(site,dpId,horVer, sep = "_")]] <- swapGaps
          
    }
  }
  
  swapDt <- data.table::rbindlist(swapGapLs)
  if(base::nrow(swapDt) == 0){
    message(paste0("No swap gap data available to assess for ", dpId, ". Consider increasing maxGapMins."))
    next()
  }
  
  # ======================================
  # Remove outliers:
  if(!manlOtlrThr){
    # Auto detect outliers based on sd
    minSd <- min(sd(swapDt[['diffCalb']], na.rm = TRUE), sd(swapDt[['diffDrft']], na.rm = TRUE))
    oltrDiffThr <- minSd*5
  } # Otherwise otlrDiffThr should be pre-defined
  
  swapDtRmOtlr <- swapDt %>% subset(base::abs(diffCalb) < otlrDiffThr)
  swapDtRmOtlr <- swapDtRmOtlr %>% subset(base::abs(diffDrft) < otlrDiffThr)
  
  
  meltSwap <- swapDtRmOtlr %>% data.table::melt.data.table(id.vars = c("instDate","site"), measure.vars = c("diffCalb","diffDrft")) 
  
  plotdiffCalbVsDrft <- meltSwap %>% 
    ggplot(aes(x = variable, y = value, group = variable)) +
    geom_boxplot() +
    geom_line(aes(group=instDate), position = position_dodge(0.3)) +
    geom_point(aes(fill=variable,group=instDate),size=2,shape=21, position = position_dodge(0.3)) +
    theme(legend.position = "none") +
    ggtitle(paste0("Changes in data during sensor swaps performed within ", maxGapMins, " mins: ", dpId)) +
    ylab(paste0("Post-swap - Pre-swap reading, ", dpId)) + 
    theme(axis.text.x = element_text(size = 14)) +
    theme(axis.text.y = element_text(size = 14)) 
  
  allPlotGapDiff[[dpId]] <- plotdiffCalbVsDrft
  
  # write plots to bucket
  plotObjName <- base::paste0("analysis/sensorSwap/plots/",dpId,"_",maxGapMins,"GapMins_ReadingChangeFromSwapRawVsDrift.rds")
  aws.s3::s3saveRDS(x = plotdiffCalbVsDrft,bucket = bucket,object = plotObjName)

  plotNameTsGap <- base::paste0("analysis/sensorSwap/plots/",dpId,"_",maxGapMins,"GapMins_PrePostSwapTs.rds")
  aws.s3::s3saveRDS(x = plotTsGapLs,bucket = bucket,object = plotNameTsGap)
  
  # plotGapVsTime <- swapDtRmOtlr %>% 
  #   ggplot(aes(x = diffMins, y = diffCalb)) +
  #   geom_point()
  # 

}

