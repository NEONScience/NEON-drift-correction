#' @title wrapper function to download data from presto for drift assessemnt
#' @author Cove Sturtevant
#' @description Given data product ID, time range, and file source directory, load already-saved data as .Rds or download from presto if specified.
#' @param fileData The filepath where .RData should live
#' @param idDp Full NEON dp ID
#' @param yearMnthBgn Begin year-month character class, formatted as 'YYYY-MM'
#' @param yearMnthEnd End year-month character class, formatted as 'YYYY-MM'
#' @param timeAgr Time aggregation value, in minutes
#' @param dnld Logical. Should data download? Default FALSE.
# idDp <- "NEON.D13.WLOU.DP0.20016.001.01378.102.100.000"
# yearMnthBgn <- '2018-01' # Format YYYY-DD as a string
# yearMnthEnd <- '2021-01' # Format YYYY-DD as a string
# timeAgr <- 5 # minutes
#'  
# Changelog / Contributions
# Created by Cove, adapted to wrapper by Guy on 2021-03-03

wrap.load.data.psto.drft <- function(fileData,idDp, yearMnthBgn = '2018-01' , yearMnthEnd = '2021-01', timeAgr = 5, dnld = FALSE){
  if(dnld == TRUE){
    timeDist <- base::as.difftime(timeAgr,units="mins") # Time aggregation interval in minutes
    
    yearEnd <- base::as.numeric(base::substr(yearMnthEnd,start=1,stop=4))
    mnthEnd <- base::as.numeric(base::substr(yearMnthEnd,start=6,stop=7))
    
    if(mnthEnd == 12){
      mnthEnd <- '01'
      yearEnd <- base::as.character(yearEnd + 1)
    } else {
      mnthEnd <- base::as.character(mnthEnd + 1)
    }
    
    # Set up the output time sequence
    timeBgn <- base::strptime(base::paste0(yearMnthBgn,'-01'),format="%Y-%m-%d",tz='GMT') # Start time
    timeEnd <- base::strptime(base::paste0(yearEnd,'-',mnthEnd,'-01'),format="%Y-%m-%d",tz='GMT')-timeDist
    timeBgn <- base::as.POSIXct(base::seq.POSIXt(from=timeBgn,to=timeEnd,by=timeDist,tz="GMT"))
    timeEnd <- timeBgn + timeDist
    numData <- base::length(timeBgn)
    
    # We're going to grab data for each month and string it together
    yearMnth <- base::unique(base::format(timeBgn,format='%Y-%m'))
    data <- vector(mode='list',length=length(yearMnth)) # Initialize
    names(data) <- yearMnth
    
    for(idxMnth in yearMnth){
      print(idxMnth)
      
      # Get the end time for the data this month
      yearEndIdx <- base::as.numeric(base::substr(idxMnth,start=1,stop=4))
      mnthEndIdx <- base::as.numeric(base::substr(idxMnth,start=6,stop=7))
      
      if(mnthEndIdx == 12){
        mnthEndIdx <- '01'
        yearEndIdx <- base::as.character(yearEndIdx + 1)
      } else {
        mnthEndIdx <- base::as.character(mnthEndIdx + 1)
      }
      
      timeBgnIdx <- base::strptime(base::paste0(idxMnth,'-01'),format="%Y-%m-%d",tz='GMT') # Start time
      timeEndIdx <- base::strptime(base::paste0(yearEndIdx,'-',mnthEndIdx,'-01'),format="%Y-%m-%d",tz='GMT')
      
      # Grab the data
      data[[idxMnth]] <- som::wrap.extr.neon.dp.psto(idDp=list(idDp),
                                                     timeBgn=timeBgnIdx,
                                                     timeEnd=timeEndIdx,
                                                     MethRglr= c("none","CybiEc")[2],
                                                     Freq=1/60/timeAgr,
                                                     WndwRglr = c("Cntr", "Lead", "Trlg")[3],
                                                     IdxWndw = c("Clst","IdxWndwMin","IdxWndwMax")[2],
                                                     Srvr=c("https://den-prodpresto-1.ci.neoninternal.org:8443"), 
                                                     Type=c('numc','str')[1],
                                                     CredPsto=NULL,
                                                     PrcsSec = 3
      )[[1]] # Just doing one data stream for now, but could do multiple
      
    } # end loop around months
    
    # Combine all the data into a single data frame and save it
    data <- do.call(rbind,data)
    save(data,file=fileData)
    
  } else {
    
    # Just load it in!
    load(file=fileData)
  }
  
  
  return(data)
}