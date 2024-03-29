% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/EPC_Cleaning_SComm.R
\name{epc_improves_scomm}
\alias{epc_improves_scomm}
\title{EPC Improvements data collection for Scotland EPCs}
\usage{
epc_improves_scomm(data, imp = IMPROVEMENTS)
}
\arguments{
\item{data}{The dataframe with data to be cleaned}

\item{imp}{The column in the dataframe with retrofit improvements. Expecting a column with long strings with measures seperated by a pipe (|) delimiter}
}
\value{
A dataframe with the UPRN, cleaned data and original EPC data for onward processing
}
\description{
Generates a list of unique EPC improvements from the Scottish EPC register
}
\examples{
scomm_epc_improves(data)
scomm_epc_improves(data, "IMPROVEMENTS_EPC")
}
