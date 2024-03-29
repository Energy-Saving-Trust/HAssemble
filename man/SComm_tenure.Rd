% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/EPC_Cleaning_SComm.R
\name{scomm_tenure}
\alias{scomm_tenure}
\title{Property tenure cleaning for Scotland EPCs}
\usage{
scomm_tenure(data, tenure = TENURE, mute = NULL, ...)
}
\arguments{
\item{data}{The dataframe with data to be cleaned}

\item{tenure}{The column in the dataframe to be cleaned}

\item{mute}{option to mute some print statements. If set to TRUE then only prints when processing starts and ends. Default is NULL, but FALSE is also valid.}
}
\value{
A dataframe with the UPRN, cleaned data and original EPC data for onward processing
}
\description{
Standarised cleaning procedure for Home Analytics updates
}
\examples{
scomm_tenure(data)
scomm_tenure(data, PROP_TENURE)
}
