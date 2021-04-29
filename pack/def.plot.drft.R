#' @title Plot the calibrated and drift corrected data for a single DP ID
#' @author Cove Sturtevant, Guy Litt
#' @description Plot the calibrated and drift corrected data for a single DP ID
#' @param data
#' @param idDp # Optional. Character string of data product identifier (title for the plot)
#' @return A plot of drift corrected data with marked installation dates
def.plot.drft <- function(data,
                          idDp=NULL
                          ){

  library(plotly)
  dataPlot <- data[,names(data) %in% c('time','calibrated','driftCorrected')]
  dataPlot <- reshape2::melt(dataPlot,id.vars=c('time'))
  
  plot <- plotly::plot_ly(data=dataPlot, x=~time, y=~value, split = ~variable, type='scatter', mode='lines') %>%
    plotly::layout(margin = list(b = 50, t = 50, r=50),
                   title = idDp,
                   xaxis = list(title = base::paste0(c(rep("\n&nbsp;", 3),
                                                       rep("&nbsp;", 20),
                                                       paste0("Date-Time"),
                                                       rep("&nbsp;", 20)),
                                                     collapse = ""),
                                nticks=6,
                                range = dataPlot$time[c(1,base::length(dataPlot$time))],
                                zeroline=FALSE
                   )) 

  
  # Add in sensor swap points
  plot <- plot %>% 
    plotly::add_markers(x=unique(data$instDate),y=min(dataPlot$value,na.rm=TRUE),name='Sensor swap',inherit=FALSE) %>%
    
    print(plot)
}