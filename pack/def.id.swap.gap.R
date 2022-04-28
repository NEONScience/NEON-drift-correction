#'@title identify the time gaps between sensor swaps
#' @author Guy Litt
#' @description Given a dataframe of sensor data with a numeric column as a sensor
#' identifier, identify sensor swaps.
#' 
#' @param dtDrft Data.table class time series data with an identifier corresponding to a sensor
#' @param instIdCol Column name in dtDrft corresponding to install date
#' @param swapIdCol
#' @param swapIdCol the identifier column name
#' @param maxGapDataPts - the max # of data points allowed to find gaps of interest. Default null returns all gaps.
#' @return a dataframe of sensor  gap begin and  end row indices corresponding to \code{dtDrft}
#' with column names \code{idxsBgnSwap} & \code{idxsEndSwap} & \code{swapLen}. The begin
#' index should contain the last index of a sensor stream, and the end index should contain
#' the first index of the new sensor stream.
#' 
# Changelog 
#   2021-04-23 Originally created, Guy Litt
#   2022-04-25 Update documentation, GL
#  TODO Search for a swap gap within a specified time range, srchTimeMins If gap identified, redefine the swap gap.
#   E.g. WALK.DP0.00004 200.000 - instDate defined as 2020-10-01 18:47:03, but data begin at 18:40:57 following a 5+ hour gap for sensor swap.
# instIdCol <- "instDate"
# dataCol <- "data"
# timeCol <- "time"
# def.id.swap.gap <- function(dtDrft, swapIdCol = "U_CVALE9",maxGapDataPts = NULL ){



def.id.swap.gap <- function(dtDrft, instIdCol = "instDate",  swapIdCol = "U_CVALE9", dataCol ="data", timeCol = "time", srchTimeMins = 60  ){  
  if(!"data.table" %in% base::class(dtDrft)){
    dtDrft <- data.table::as.data.table(dtDrft)
  }
  
  if(!base::is.numeric(dtDrft[[swapIdCol]])){
    warning(paste0("The swap ID col may need to be a numeric class: ", swapIdCol))
  }
  
  # sensor Times:
  sensTimz <- stats::na.omit(base::unique(dtDrft[[instIdCol]]))
  
  # dtDrft NA data remove:
  idxsNa <- base::which(base::is.na(dtDrft[[dataCol]])) 
  if(length(idxsNa)>0){
    dtDrftSub <- dtDrft[-idxsNa,]
  } else {
    dtDrftSub <- dtDrft
  }
  
  swapSumm <- dtDrftSub %>% dplyr::group_by(instDate) %>% 
                    dplyr::summarise(dataBgn = base::min(time),
                                     dataEnd = base::max(time))
  

  swapSumm <- swapSumm[base::which(!base::is.na(swapSumm[[instIdCol]])),]

  
  # Identify TRUE gaps in the data:
  if(base::nrow(swapSumm)>1){
    rsltNaGap <- def.na.run.idxs(dt = dtDrft, dataCol = dataCol)
    
    # Now work off of defined NA time ranges & find which ones are w/in threshold of the swap gaps of interest
    timeBgnsNaThr <- dtDrft[[timeCol]][rsltNaGap$idxsNaBgn]
    timeEndsNaThr <- dtDrft[[timeCol]][rsltNaGap$idxsNaEnd]
    
    # -------------
    endInstPrevThr <- swapSumm$dataEnd[1:(base::nrow(swapSumm)-1)] -  base::as.difftime(srchTimeMins, units = "mins")
    bgnInstNextThr <- swapSumm$dataBgn[2:base::nrow(swapSumm)] + base::as.difftime(srchTimeMins, units = "mins")
    endInstPrev <- swapSumm$dataEnd[1:(base::nrow(swapSumm)-1)] 
    bgnInstNext <- swapSumm$dataBgn[2:base::nrow(swapSumm)]
    swapSumm$diffMinsNonThr <- base::c(NA,base::difftime(bgnInstNext,endInstPrev, units = "mins"))
    # ------------
    
    # Interim fix for installs that don't line up with gaps... get rid of those that have no
    # NAs around the install
    freq <- median(diff(dtDrft$time))
    swapSumm$diffMinsNonThr[swapSumm$diffMinsNonThr <= freq] <- NA
    swapSumm$diffMins <- swapSumm$diffMinsNonThr
    
    # Turning this section off for now until the TODO can be completed. See interim fix above.
    if(FALSE){
      # The indices in dtDrft that correspond to time window around a swap: 
      idxsTimeRgn <- lapply(1:(nrow(swapSumm)-1), function(i) intersect(which(dtDrft[[timeCol]] >  endInstPrevThr[i]),
                                                     which(dtDrft[[timeCol]] < bgnInstNextThr[i]) ) )
      
      # The indices in rsltNaGap that align with time window around a swap:
      idxsNaGapWndo <- lapply(1:length(idxsTimeRgn), function(i) intersect(which(rsltNaGap$idxsNaBgn <= max(idxsTimeRgn[[i]])),
                                                          which(rsltNaGap$idxsNaEnd >= min(idxsTimeRgn[[i]])) ) )
      
      if(length(idxsNaGapWndo) > 0){
        diffTimz <- base::lapply(idxsNaGapWndo, function(j) 
                                              ifelse(!is.null(j), base::difftime(dtDrft[[timeCol]][rsltNaGap$idxsNaEnd[j]],
                                                             dtDrft[[timeCol]][rsltNaGap$idxsNaBgn[j]],
                                                             units = "mins"),NA ))
        
        #TODO match diffTimz w/ actual time in swapSumm
        swapSumm$diffMins <- c(NA, unlist(diffTimz))
      } else {
        swapSumm$diffMins <- NA
      }
    }
    
  } else {
    swapSumm$diffMinsNonThr <- NA
    swapSumm$diffMins <- NA
  }

  
  
  return(swapSumm)
}