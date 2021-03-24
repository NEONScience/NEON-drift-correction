#' @title plot calibrated and drift-corrected data together
#' @author Cove Sturtevant
#' @description given a dataset with calibrated and drift-corrected data columns
#' @seealso def.read.asst.xml.neon
#' @seealso wrap.cal.corr.drft
#' @seealso wrap.load.data.psto.drft
#' @param data Dataframe of data
#' @param assetHist Dataframe of asset history. See wrap.cal.corr.drft.R
#' @param idDp Data product ID for plot title
#' @param p Optional. Another plotly plot to pass in and build upon
#' @param timeCol time col of \code{data}. Default 'time'
#' @param colsPlot column names of data of interest in \code{data}. Default \code{c('calibrated','driftCorrected')}
#' @param term Optional. Term name or some identifier to add to the items in a legend. Useful when passing in another plotly plot as \code{p}

# Changelog/Contributions
# Originally created by Cove, adapted to function and multi-plotting capability by GL 2021-03-03
def.plot.cal.drft <- function(data, assetHist, idDp, p = NULL, timeCol = 'time', colsPlot = c('calibrated','driftCorrected'), term = NULL){
  

  
  dataPlot <- data[,names(data) %in% c(timeCol,colsPlot)]
  if(!base::is.null(term)){
    base::colnames(dataPlot) <- c(timeCol, paste0(colsPlot,term))
  }
  
  dataPlot <- reshape2::melt(dataPlot,id.vars=c(timeCol))
  base::colnames(dataPlot) <- base::c("time","variable","value")
  # plot<-ggplot(dataPlot,aes(time,value,colour=variable)) +
  #   geom_line() + labs(title=idDp)
  
  
  if(base::is.null(p)){
    plot <- plotly::plot_ly(data=dataPlot, x=~time, y=~value, split = ~variable, type='scatter', mode='lines', alpha = 0.3) %>%
      plotly::add_markers(x=assetHist$installDate,y=min(dataPlot$value,na.rm=TRUE),name=paste(term,'Sensor swap'),inherit=FALSE) %>%
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
  } else {
    
    plot <- p %>% plotly::add_trace(data=dataPlot,  x=~time, y=~value, split = ~variable, type='scatter', mode='lines', alpha = 0.3) %>%
      plotly::add_markers(x=assetHist$installDate,y=min(dataPlot$value,na.rm=TRUE),name=paste(term,'Sensor swap'),inherit=FALSE) %>%
      plotly::layout(margin = list(b = 50, t = 50, r=50),
                     title = paste0(idDp, collapse = " & \n"),
                     xaxis = list(title = base::paste0(c(rep("\n&nbsp;", 3),
                                                         rep("&nbsp;", 20),
                                                         paste0("Date-Time"),
                                                         rep("&nbsp;", 20)),
                                                       collapse = ""),
                                  nticks=6,
                                  range = dataPlot$time[c(1,base::length(dataPlot$time))],
                                  zeroline=FALSE
                     )) 
  
  } 
  
  return(plot)
}