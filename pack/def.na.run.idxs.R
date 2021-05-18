#' @title Identify NA runs, start and end indices
#' @author Guy Litt
#' @description Identify start/stop indices of NA & real number runs in a data column
#' 
#' @param dt A data.table or data.frame object
#' @param dataCol The single data column of interest for testing NA vs. non-NA
#' @seealso def.id.swap.gap
#
# Changelog/Contributions
#  2021-05-18 Originally created, GL

require(data.table)
def.na.run.idxs <- function(dt, dataCol){
  if("data.table" %in% base::class(dt)){
    dt <- data.table::as.data.table(dt)
  } 
  
  rleNa <- base::rle(base::is.na(dt[[dataCol]]))
  
  cumSumDrftNa <- base::cumsum(rleNa$lengths)
  
  idxsNaEnd <- cumSumDrftNa[base::which(rleNa$values==TRUE)]
  idxsRealEnd <- cumSumDrftNa[base::which(rleNa$values==FALSE)]
  idxsNaBgn <- idxsRealEnd + 1
  idxsRealBgn <- idxsNaEnd + 1
  
  # Define the actual begin time
  if(rleNa$values[1] == TRUE){
    idxsNaBgn <- base::c(1, idxsNaBgn)
  } else if (rleNa$values[1] == FALSE){
    # idxsRealBgnRedo <- c(1, head(idxsRealBgn, -1))
    idxsRealBgn <- base::c(1, idxsRealBgn)
  }
  # The End Idxs from cumsum are already correct...
  
  # Ensure all begin times valid:
  if(base::max(idxsRealBgn) > base::nrow(dt)){
    # Remove the incorrect begin time:
    idxsRealBgn <- head(idxsRealBgn, -1)
  } else if (base::max(idxsNaBgn) > base::nrow(dt)){
    # Remove the incorrect begin time:
    idxsNaBgn <- head(idxsNaBgn,-1)
  }
  
  rslt <- base::list(idxsNaBgn = idxsNaBgn,
                     idxsNaEnd = idxsNaEnd,
                     idxsRealBgn = idxsRealBgn,
                     idxsRealEnd = idxsRealEnd)
  
  return(rslt)
}


