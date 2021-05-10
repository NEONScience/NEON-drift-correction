#'@title identify the time gaps between sensor swaps
#' @author Guy Litt
#' @description Given a dataframe of sensor data with a numeric column as a sensor
#' identifier, identify sensor swaps.
#' 
#' @param dtDrft Data.table class time series data with an identifier corresponding to a sensor
#' @param swapIdCol the identifier column name
#' @param maxGapDataPts - the max # of data points allowed to find gaps of interest. Default null returns all gaps.
#' @return a dataframe of sensor  gap begin and  end row indices corresponding to \code{dtDrft}
#' with column names \code{idxsBgnSwap} & \code{idxsEndSwap} & \code{swapLen}. The begin
#' index should contain the last index of a sensor stream, and the end index should contain
#' the first index of the new sensor stream.
#' 
# Changelog 
#   2021-04-23 Originally created, Guy Litt
# instIdCol <- "instDate"
# dataCol <- "data"
# timeCol <- "time"
# def.id.swap.gap <- function(dtDrft, swapIdCol = "U_CVALE9",maxGapDataPts = NULL ){
def.id.swap.gap <- function(dtDrft, instIdCol = "instDate",  swapIdCol = "U_CVALE9", dataCol ="data", timeCol = "time" ){  
  if(!"data.table" %in% base::class(dtDrft)){
    dtDrft <- data.table::as.data.table(dtDrft)
  }
  
  if(!base::is.numeric(dtDrft[[swapIdCol]])){
    warning(paste0("The swap ID col may need to be a numeric class: ", swapIdCol))
  }
  
  # sensor Times:
  sensTimz <- stats::na.omit(base::unique(dtDrft[[instIdCol]]))
  
  idxsNa <- base::which(base::is.na(dtDrft[[dataCol]])) 
  if(length(idxsNa)>0){
    dtDrftSub <- dtDrft[-idxsNa,]
  } else {
    dtDrftSub <- dtDrft
  }
  
  # dtDrft NA data remove:
  swapSumm <- dtDrftSub %>% dplyr::group_by(instDate) %>% 
                    dplyr::summarise(dataBgn = base::min(time),
                                     dataEnd = base::max(time))
  

  swapSumm <- swapSumm[base::which(!base::is.na(swapSumm[[instIdCol]])),]
  
  if(base::nrow(swapSumm)>1){
    endInstPrev <- swapSumm$dataEnd[1:(base::nrow(swapSumm)-1)]
    bgnInstNext <- swapSumm$dataBgn[2:base::nrow(swapSumm)]
    swapSumm$diffMins <- base::c(NA,base::as.difftime(bgnInstNext - endInstPrev, units = "mins"))
    
  } else {
    swapSumm$diffMins <- NA
  }

  return(swapSumm)
  # # ============= RLE approach (not working well)
  # # Identify sensor swap dates:
  # swapRle <- base::rle((dtDrft[[swapIdCol]]))
  # # Find the begin/end indices of all sensor swaps
  # idxsSwap <- base::cumsum(swapRle$lengths)
  # 
  # idxSwapEnd <- idxsSwap[base::which(swapRle$lengths > 1)]
  # idxSwapBgn <- idxSwapEnd - swapRle$lengths[base::which(swapRle$lengths > 1)] + 1
  # 
  # # Limit to just the indices that have begin/end pairs (no edge cases)
  # idxsSwapBgnSub <- idxSwapBgn[2:length(idxSwapBgn)]
  # idxsSwapEndSub <- idxSwapEnd[1:(length(idxSwapEnd)-1)]
  # 
  # # Find the next swap begin:
  # gapPts <- idxsSwapBgnSub - idxsSwapEndSub
  # 
  # # Subset to gaps of interest (if there is a threshold)
  # if(!base::is.null(maxGapDataPts)){
  #   idxShrtGaps <- base::which(gapPts <= maxGapDataPts)
  # } else {
  #   idxShrtGaps <- gapPts
  # }
  # 
  # dfSwapGaps <- base::data.frame(idxsBgnSwap = base::integer(base::length(idxShrtGaps)),
  #                            idxsEndSwap = base::integer(base::length(idxShrtGaps)),
  #                            swapLen = base::integer(base::length(idxShrtGaps)),
  #                            stringsAsFactors = FALSE)
  # if(length(idxShrtGaps) == 0){
  #   message("No sensor swap time gaps found below the maxGapDataPts threshold.")
  # } else {
  #   dfSwapGaps$idxsBgnSwap <- idxsSwapEndSub[idxShrtGaps]
  #   dfSwapGaps$idxsEndSwap <- idxsSwapBgnSub[idxShrtGaps]
  #   dfSwapGaps$swapLen <- dfSwapGaps$idxsEndSwap - dfSwapGaps$idxsBgnSwap
  # }
  # 
  # return(dfSwapGaps)
}