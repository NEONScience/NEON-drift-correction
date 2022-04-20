#' @title Plot the calibrated and drift corrected data for two co-located DP IDs
#' @author Cove Sturtevant, Guy Litt
#' @description Plot the calibrated and drift corrected data for two co-located DP IDs
#' @param dataMain Main DP ID
#' @param dataCoLoc Co-located DP ID
#' @param idDpMain # Optional. Character string of data product identifier (title for the plot)
#' @param idDpCoLoc # Optional. Character string of data product identifier (title for the plot)
#' @return A plot of drift corrected data with marked installation dates
def.plot.drft.coLoc <- function(dataMain,
                                dataCoLoc,
                                idDpMain='main',
                                idDpCoLoc='co-located'
                          ){

  library(plotly)
  dataMain$sensor <- idDpMain
  dataCoLoc$sensor <- idDpCoLoc
  dataPlotCal <- rbind(dataMain[,names(dataMain) %in% c('time','calibrated','sensor')],
                       dataCoLoc[,names(dataCoLoc) %in% c('time','calibrated','sensor')],
                       stringsAsFactors=FALSE)
  dataPlotCorr <- rbind(dataMain[,names(dataMain) %in% c('time','driftCorrected','sensor')],
                       dataCoLoc[,names(dataCoLoc) %in% c('time','driftCorrected','sensor')],
                       stringsAsFactors=FALSE)
  # dataPlotCal <- reshape2::melt(dataPlotCal,id.vars=c('time'))
  # dataPlotCorr <- reshape2::melt(dataPlotCorr,id.vars=c('time'))
  
  # Plot calibrated comparison
  plotCal <- plotly::plot_ly(data=dataPlotCal, x=~time, y=~calibrated, split = ~sensor, type='scatter', mode='lines') %>%
    plotly::layout(margin = list(b = 50, t = 50, r=50),
                   title = 'Co-located comparison (calibrated)',
                   xaxis = list(title = base::paste0(c(rep("\n&nbsp;", 3),
                                                       rep("&nbsp;", 20),
                                                       paste0("Date-Time"),
                                                       rep("&nbsp;", 20)),
                                                     collapse = ""),
                                nticks=6,
                                range = dataPlotCal$time[c(1,base::length(dataPlotCal$time))],
                                zeroline=FALSE
                   )) 

  
  # Add in sensor swap points
  plotCal <- plotCal %>% 
    plotly::add_markers(x=unique(dataMain$instDate),y=min(dataPlotCal$calibrated,na.rm=TRUE),name=paste0('Sensor swap ',idDpMain),inherit=FALSE) %>%
    plotly::add_markers(x=unique(dataCoLoc$instDate),y=min(dataPlotCal$calibrated,na.rm=TRUE),name=paste0('Sensor swap ',idDpCoLoc),inherit=FALSE)
    
  print(plotCal)
  
  # Plot drift-corrected comparison
  plotCorr <- plotly::plot_ly(data=dataPlotCorr, x=~time, y=~driftCorrected, split = ~sensor, type='scatter', mode='lines') %>%
    plotly::layout(margin = list(b = 50, t = 50, r=50),
                   title = 'Co-located comparison (drift-corrected)',
                   xaxis = list(title = base::paste0(c(rep("\n&nbsp;", 3),
                                                       rep("&nbsp;", 20),
                                                       paste0("Date-Time"),
                                                       rep("&nbsp;", 20)),
                                                     collapse = ""),
                                nticks=6,
                                range = dataPlotCorr$time[c(1,base::length(dataPlotCorr$time))],
                                zeroline=FALSE
                   )) 
  
  
  # Add in sensor swap points
  plotCorr <- plotCorr %>% 
    plotly::add_markers(x=unique(dataMain$instDate),y=min(dataPlotCorr$driftCorrected,na.rm=TRUE),name=paste0('Sensor swap ',idDpMain),inherit=FALSE) %>%
    plotly::add_markers(x=unique(dataCoLoc$instDate),y=min(dataPlotCorr$driftCorrected,na.rm=TRUE),name=paste0('Sensor swap ',idDpCoLoc),inherit=FALSE)
  
  print(plotCorr)
  
}