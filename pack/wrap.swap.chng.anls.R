#' @title Assess sensor reading changes during sensor swaps
#' @author Guy Litt
#' @description For a given sensor stream, identify periods
#' when sensor swaps are performed within a maxGapMins threshold.
#' Then calculate the difference from the post-swap reading to the 
#' pre-swap reading. Do this for both the calibrated readings
#' and the drift corrected readings. Generate a box plot of these 
#' differences for each data product. Save plot to bucket/analysis/sensorSwap/plots/
#' @details Outliers in the calculated difference between pre-swap and 
#' post-swap sensor readings are possible. The automated outlier removal process
#' finds the minimum in the sd of the calibrated and drift-corrected data value 
#' changes during sensor swap, and then multiplies the min sd by 5 to define the 
#' threshold. Otherwise, a manual threshold may be applied, but caution should
#' be exercised when considering multiple data products with different units.
#' @param idDpsVec A character-class vector of full dataproduct IDs of interest
#' @param bucket the s3 bucket name. Default 'dev-is-drift'
#' @param maxGapMins Numeric. The maximum allowable gap of missing data for a given sensor swap. Default 60.
#' @param timeCol The time column name in the data. Default "time"
#' @param manlOtlrThr Boolean. Should an outlier threshold be designated manually? Default FALSE.
#' @param otlrDiffThr Numeric. If \code{manOtlrThr=TRUE}, use this manually-defined threshold for determining outliers. Default 10. In units of the data stream of interest.
#' @seealso flow.drft.eval.R, flow.swap.chng.anls.R
#' @return \code{rslt}, a list of result objects:
#' \code{rslt$swapDtRmOtlrLs} a data.table of swap gap differences between both calibrated and drift-corrected within maxGapMins and with outliers removed, listed by dpId
#' \code{rslt$allPlotGapDiffLs} A list of plots depicting swap gap analysis for each data product of interest, listed by dpId
#' \code{rslt$swapDtLs} The comprehensive dataset of swap gap differences, regardless of whether a drift or calibration comparison can be made, listed by dpId
#' 
# Changelog / Contributions
#   2021-05-07 Guy Litt, originally created

library(aws.s3)
library(tidyverse)
library(stringr)
library(dplyr)

# TODO add term # to consideration

wrap.swap.chng.anls <- function(idDpsVec, bucket = 'dev-is-drift', maxGapMins=60, timeCol="time", manlOtlrThr = FALSE, otlrDiffThr = 10){
  if(!"character" %in% base::class(idDpsVec)){
    stop("idDpsVec should be character class.")
  }

  # Organize by unique main dpId & termId!
  dpIdsUniq <- base::unique(unlist(lapply(idDpsVec, function(idDp) base::substring(idDp, first = 15, last = 23+10))))
  
  swapDtLs <- base::list()
  swapDtRmOtlrLs <- base::list()
  allPlotGapDiff <- base::list()
  for(dpIdVerTerm in dpIdsUniq){
    
    dpIdMain <- substr(dpIdVerTerm,start=1, stop = 9)
    message(paste0("Grabbing all data from: ", aws.s3::get_bucketname(bucket = bucket, x = paste0('data/L1drift/',dpIdMain,"/")) ," to assess swap gap differences"))
    
    
    drftNamz <- aws.s3::get_bucket_df(bucket = bucket, prefix = paste0('data/L1drift/',dpIdMain,"/"), max = Inf)$Key
    drftNamz <- drftNamz[base::grep(".rds",drftNamz)]

    if(!base::grepl("TERM",dpIdVerTerm)){ # Further subset data by term if it was specified:
      drftNamz <- drftNamz[base::grep(dpIdVerTerm, drftNamz)]
      dpId <- dpIdVerTerm
    } else {
      dpId <- paste0(dpIdMain)
    }
    # Run a check in case drftNamz didn't drill down to a specific term number:
    uniqTrmz <- base::unique(unlist(base::lapply(drftNamz, function(x) substr(strsplit(x = x, split =dpIdMain)[[1]][3], start = 6, stop = 10))))
    if(base::length(uniqTrmz)>1){
      stop(paste0("Each idDpsVec supplied to wrap.swap.chng.anls must discern a unique term. This data product contains these terms: ", paste0(uniqTrmz, collapse = ", ")))
    }
    
    plotTsGapLs <- base::list()
    swapGapLs <- base::list()
    # siteNamz <- sitzDpz[[dpId]]
    
    siteNamz <- unique(unlist(lapply(drftNamz, function(x) strsplit(x, split = "/")[[1]][4]) ))
    
    for(site in siteNamz){
      dpSiteId <- paste0('data/L1drift/',dpIdMain,"/",site,"/")
      # drftNamz <- aws.s3::get_bucket_df(bucket = bucket, prefix = dpSiteId)$Key
      siteDrftNamz <- drftNamz[base::grep(site, drftNamz)]
      if(base::length(siteDrftNamz) == 0){
        warning(paste0("No data for ", dpId, " at ", site))
        next()
      }
      
      # Split by HOR.VER:
      endIdxStr <- stringr::str_locate(pattern = dpSiteId, string = siteDrftNamz)[,"end"]
      
      # termHorVersMl <- base::substring(siteDrftNamz,first = (endIdxStr + 30),last = (endIdxStr + 45))
      # # simplify to HOR.VER if the HOR.VER.TMI has the same TMI throughout.
      # if(length(base::unique(base::substring(termHorVersMl,first = 8, last = 11))) == 1){
      #   horVersMl <- base::substring(horVersMl, first = 1, last = 7)
      # }
      # 
      
      horVersMl <- base::substring(siteDrftNamz,first = (endIdxStr + 35),last = (endIdxStr + 45))

      # simplify to HOR.VER if the HOR.VER.TMI has the same TMI throughout.
      if(length(base::unique(base::substring(horVersMl,first = 8, last = 11))) == 1){
        horVersMl <- base::substring(horVersMl, first = 1, last = 7)
      }
      
      for(horVer in base::unique(horVersMl)){
        drftNamzSub <- siteDrftNamz[base::which(horVersMl==horVer)]
        
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
        
        swapGaps$dpId <- dpIdMain
        swapGaps$site <- site
        swapGaps$horVer <- horVer
        swapGapLs[[paste(site,dpIdMain,horVer, sep = "_")]] <- swapGaps
        
      }
    }
    
    swapDt <- data.table::rbindlist(swapGapLs)
    if(base::nrow(swapDt) == 0){
      message(paste0("No swap gap data available to assess for ", dpId, ". Consider increasing maxGapMins."))
      next()
    }
    
    # ======================================
    # Remove outliers:
    if(manlOtlrThr == FALSE){
      # Auto detect outliers based on sd
      minSd <- min(sd(swapDt[['diffCalb']], na.rm = TRUE), sd(swapDt[['diffDrft']], na.rm = TRUE), na.rm = TRUE)
      
      if(is.numeric(minSd)){
        otlrDiffThr <- minSd*5
      } else {
        stop("Could not determine a numeric outlier threshold value automatically.")

      }
    }  # Otherwise otlrDiffThr should be pre-defined
    
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
    
    message(paste0("wrote sensor swap plot in bucket to ",plotObjName, " and ", plotNameTsGap))
    
    swapDtRmOtlrLs[[dpId]] <- swapDtRmOtlr
    swapDtLs[[dpId]] <- swapDt
  }
  
  rslt <- base::list(swapDtRmOtlrLs = swapDtRmOtlrLs, allPlotGapDiff = allPlotGapDiff, swapDtLs = swapDtLs)
  
  return(rslt)
}