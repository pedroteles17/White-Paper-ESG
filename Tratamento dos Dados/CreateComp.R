library(tidyverse)
library(readxl)
library(xts)
library(lubridate)
library(furrr)
library(progressr)
library(parallel)
library(lubridate)

"%ni%" <- Negate("%in%")

plan(multisession, workers = parallel::detectCores())

source("Funcoes.R")

path_dado <- paste0(getwd(), '\\Brasil')

# Importa os dados com o retorno dos ativos
dbRet_ativos <- read_csv(paste(path_dado, "ativos.csv", sep = "\\"))
dbRet_ativos <- dbRet_ativos %>% dplyr::filter(Data > "2009-12-31")

# Transforma os dados de diario para mensal para fazermos a matriz de composicao
dbRet_ativos <- xts(dbRet_ativos[,-1], dbRet_ativos$Data)

dbRet_ativos_mes <- list()
for (i in 1:ncol(dbRet_ativos)) {
  dbRet_ativos_mes[[i]] <- apply.monthly(dbRet_ativos[,i], function(x) sum(!is.na(x), na.rm = TRUE))
}

dbRet_ativos_mes <- do.call("cbind.xts", dbRet_ativos_mes)

dbRet_ativos_mes <- data.frame(dbRet_ativos_mes)

# Se temos apenas NAs (if é verdade) 0; c.c 1
for (i in 1:nrow(dbRet_ativos_mes)) {
  for (j in 1:ncol(dbRet_ativos_mes)) {
    if(dbRet_ativos_mes[i,j] != 0){
      dbRet_ativos_mes[i,j] <- 1
    }
  }
}

comp <- as.data.frame(t(dbRet_ativos_mes))

colnames(comp) <- as.Date(colnames(comp)) %m-% months(1)

write.csv(comp, "Brasil\\comp.csv", row.names = TRUE)
