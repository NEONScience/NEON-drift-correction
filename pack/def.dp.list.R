##############################################################################################
#' @title Gather all possible 45-character data product IDs for a data product, term, and (optionally) site combo

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. gather all possible 45-character data product IDs for a data product, 
#' term, and (optionally) site combo. Currently requires running on the 
#' den-devissom-1 server unless you have a local stored copy of Blizzard's data product info
#' and credentials for the terms database stored as an environment variable on your system. 

#' @param idDpMain Character string. The main DP ID consisting of the 3-character data product level, 
#' the 5-digit data product ID, and the data product revision, separted by periods (e.g. "DP1.00001.001")
#' @param idTerm Character string. The 5-digit term ID (e.g. "00340"). 
#' @param site Character vector. Optional. NEON site codes (e.g. "CPER"). Default is NULL, which will construct
#' the data product IDs for all locations across NEON. 
#' @param dirInfoDp the full or relative path to the directory containing data product info used for Blizzard. 
#' Default is "/scratch/SOM/dataProductInfo" assuming you are running this function on den-devissom-1
#' 
#' @return A character vector of the 45-character full data product ID of all possible instances 
#' of the specified data product, term, and site(s). 

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' # Example 1. L0. Run from den-devissom-1
#' idDpFull <- def.dp.list(idDpMain="DP0.00001.001",idTerm="01306",site=c("CPER","JORN"))
#' 
#' # OUTPUT
#' > idDpFull
#' [1] "NEON.D10.CPER.DP0.00001.001.01306.000.010.000" "NEON.D10.CPER.DP0.00001.001.01306.000.020.000"
#' [3] "NEON.D10.CPER.DP0.00001.001.01306.000.030.000" "NEON.D14.JORN.DP0.00001.001.01306.000.010.000"
#' [5] "NEON.D14.JORN.DP0.00001.001.01306.000.020.000" "NEON.D14.JORN.DP0.00001.001.01306.000.030.000"
#' 
#' # Example 2. L1. Run from den-devissom-1
#' idDpFull <- def.dp.list(idDpMain="DP1.00001.001",idTerm="00340",site=c("CPER","JORN"))
#' 
#' # OUTPUT
#' > idDpFull
#' [1]  "NEON.D10.CPER.DP1.00001.001.00340.000.010.002" "NEON.D10.CPER.DP1.00001.001.00340.000.020.002"
#' [3]  "NEON.D10.CPER.DP1.00001.001.00340.000.030.002" "NEON.D10.CPER.DP1.00001.001.00340.000.010.030"
#' [5]  "NEON.D10.CPER.DP1.00001.001.00340.000.020.030" "NEON.D10.CPER.DP1.00001.001.00340.000.030.030"
#' [7]  "NEON.D14.JORN.DP1.00001.001.00340.000.010.002" "NEON.D14.JORN.DP1.00001.001.00340.000.020.002"
#' [9]  "NEON.D14.JORN.DP1.00001.001.00340.000.030.002" "NEON.D14.JORN.DP1.00001.001.00340.000.010.030"
#' [11] "NEON.D14.JORN.DP1.00001.001.00340.000.020.030" "NEON.D14.JORN.DP1.00001.001.00340.000.030.030"

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-03-17)
#     original creation
##############################################################################################
def.dp.list <- function(idDpMain,
                        idTerm,
                        site=NULL,
                        dirInfoDp="/scratch/SOM/dataProductInfo"
){

  # Load terms database (requires NEON internal access and credentials)
  infoTermDb <- som::def.neon.dp.info()
  
  # Check for valid DP ID and term combo
  termVali <- som::def.neon.term(termUse=infoTermDb$termUse,idDpMain=idDpMain)$idTerm
  
  if(!(idTerm %in% termVali)){
    stop('Invalid data product and term combo. Try again.')
  }
  
  # File names for Blizzard's data product information  
  fileDomnSite <- "DomainSiteList.csv" # List of domains and associated sites (domain, state, etc.)
  fileDpSiteRep <- "SiteProductInstanceMatrix.csv" # List of data products and associated sites and location indices
  fileDpSiteRepDp00 <- "SiteProductInstanceMatrixDp00.csv" # List of data products and associated sites and location indices
  
  # Intialize
  Info=base::vector('list',length=0)
  
  # Domain-Site information
  dirDomnSite <- base::paste0(dirInfoDp,'/',fileDomnSite)
  Info$DomnSite <- utils::read.csv(file=dirDomnSite,header=TRUE,stringsAsFactors=FALSE)
  
  # Data Product - Site-Location information (DP0)
  pathDpSiteRepDp00 <- base::paste0(dirInfoDp,'/',fileDpSiteRepDp00)
  Info$DpSiteRepDp00 <- utils::read.csv(file=pathDpSiteRepDp00,header=TRUE,stringsAsFactors=FALSE)
  Info$DpSiteRepDp00$DPN <- base::substr(Info$DpSiteRepDp00$idDp,start=1,stop=3)
  Info$DpSiteRepDp00$versDp <- base::substr(Info$DpSiteRepDp00$idDp,start=11,stop=13)
  Info$DpSiteRepDp00$id <- base::substr(Info$DpSiteRepDp00$idDp,start=5,stop=9)
  colSiteBgn <- 7 # Column that the site codes begin - applies to both DP00 and DP01 tables.
  colSiteEnd <- 88
    
  # Data Product - Site-Location information (DP1+)
  pathDpSiteRep <- base::paste0(dirInfoDp,'/',fileDpSiteRep)
  Info$DpSiteRep <- utils::read.csv(file=pathDpSiteRep,header=TRUE,stringsAsFactors=FALSE)
  Info$DpSiteRep$DPN <- base::substr(Info$DpSiteRep$idDp,start=1,stop=3)
  Info$DpSiteRep$versDp <- base::substr(Info$DpSiteRep$idDp,start=11,stop=13)
  Info$DpSiteRep$id <- base::substr(Info$DpSiteRep$idDp,start=5,stop=9)
  Info$DpSiteRep$idName <- base::paste(Info$DpSiteRep$id,'-',Info$DpSiteRep$NameDp)
  
  # Select the DpSiteRep table (DP0 or DP1)
  DPN <- base::substr(idDpMain,start=1,stop=3)
  DpSiteRep <- switch(DPN,
                      DP0=Info$DpSiteRepDp00,
                      DP1=Info$DpSiteRep)
  
  idDpFull <- c() # Intitialize
  
  # Get the sites that have this product-term combo
  nameSiteAll <- names(DpSiteRep)[colSiteBgn:colSiteEnd]
  
  setIdDp <- which(DpSiteRep$idDp == idDpMain) # Row of this DP ID in the site-prod-instance matrix
  
  if(base::length(setIdDp) > 1){
    # There are term-specific sites or HOR.VER. Make sure we have the right sites
    termDp <- Info$DpSiteRepDp00$ApplicableTerms[setIdDp] # Terms applicable to this DP
    termDp <- lapply(termDp,som::wrap.splt.sub,dlmt='|',ptrn="[\"]",sub="") # Partition into a vector
    setIdDp <- setIdDp[
                      unlist(
                        lapply(termDp,FUN=function(idxTermDp){
                          idTerm %in% idxTermDp
                        })
                      )
                    ]
  } 
  
  siteAvail <- unique(
                      unlist(
                             lapply(setIdDp,FUN=function(idxRow){
                               nameSiteAll[DpSiteRep[idxRow,colSiteBgn:colSiteEnd] != "0"]
                               }
                              )
                            )
                      )
  
  if(is.null(site)){
    # Set sites to all available for this product
    site <- siteAvail
    
  } else {
    # Verify that the product is produced as the indicated sites. Remove any site that don't apply.
    site <- intersect(site,siteAvail)
  }
  
  # Get tmi codes for this dp-term combo
  tmi <- som::def.neon.tmi(dpSiteRep = DpSiteRep,idDpMain = idDpMain, term = idTerm)
  
  # Get the domain, hor.ver codes for this dp-term combo and return the full DP ID
  for(idxSite in site){
   
    # Get the domain code for each site
    dom <- Info$DomnSite$DomainCode[Info$DomnSite$SiteCode==idxSite]
    
    # Get hor.ver
    horVer <- som::def.neon.hor.ver(dpSiteRep = DpSiteRep,
                                    site = idxSite,
                                    idDpMain = idDpMain,
                                    term = idTerm)  
    for(idxTmi in tmi){
      idDpFull <- c(idDpFull,paste0('NEON.',dom,'.',idxSite,'.',idDpMain,'.',idTerm,'.',horVer,'.',idxTmi))
    } # End loop around tmi
    
  } # End loop around sites


  return(idDpFull)
}
