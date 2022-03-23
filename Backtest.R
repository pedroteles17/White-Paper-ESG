library(tidyverse)
library(readxl)
library(xts)
library(lubridate)
library(furrr)
library(progressr)
library(parallel)
library(PerformanceAnalytics)

"%ni%" <- Negate("%in%")

plan(multisession, workers = parallel::detectCores())

source("Funcoes.R")

path_dado <- paste0(getwd(), '\\Brasil')

# Importa a composicao do indice
dbComp <- read_csv(paste(path_dado, "comp.csv", sep = "\\"), col_types = paste0("c", paste(rep("n", 144), collapse = ""), collapse = ""))
colnames(dbComp) <- c('Ativos', as.character(ymd(20091231) %m+% months(0:144)))

# Alteramos os nomes para ficar em concordância com a base de ESG
dbComp$Ativos <- do.call("c", lapply(strsplit(dbComp$Ativos, "[.]"), function(x) x[1]))

# Importa o retorno diario do indice
dbRet_ind <- read_csv(paste(path_dado, "indice.csv", sep = "\\"), col_types = "Dn")

# Importa o retorno diario da taxa livre de risco
dbRisk_free <- read_csv(paste(path_dado, "rf.csv", sep = "\\"), col_types = "Dn")

# Importa o retorno diário dos ativos que compoe o indice
dbRet_ativos <- read_csv(paste(path_dado, "ativos.csv", sep = "\\"), col_types = paste0("D", paste(rep("n", 304), collapse = ""), collapse = ""))
colnames(dbRet_ativos) <- do.call("c", lapply(strsplit(colnames(dbRet_ativos), " "), function(x) x[1]))

# Importa os dados de ESG
dbESG <- read_excel(paste(path_dado, "ESG.xlsx", sep = "\\"))

# Datas para ficarmos apenas com os dados anuais
datas_filtro <- ymd(20101231) %m+% years(0:10)

## ESG
esg_score <- dbESG %>% dplyr::select(date, ticker, esgscore) %>% 
  spread(ticker, esgscore) %>% 
  dplyr::filter(date >= '2009-12-31')
colnames(esg_score)[1] <- "Data"

esg_score <- xts(esg_score[,-1], esg_score$Data)

esg_score <- data.frame(Data = as.Date(index(esg_score)), lapply(esg_score, na.locf))

esg_score <- esg_score %>% filter(Data %in% datas_filtro)

## Governance
governance <- dbESG %>% dplyr::select(date, ticker, governancepillarscore) %>% 
  spread(ticker, governancepillarscore) %>% 
  dplyr::filter(date >= '2009-12-31')
colnames(governance)[1] <- "Data"

governance <- xts(governance[,-1], governance$Data)

governance <- data.frame(Data = as.Date(index(governance)), lapply(governance, na.locf))

governance <- governance %>% filter(Data %in% datas_filtro)

## Environment
environment <- dbESG %>% dplyr::select(date, ticker, environmentalpillarscore) %>% 
  spread(ticker, environmentalpillarscore) %>% 
  dplyr::filter(date >= '2009-12-31')
colnames(environment)[1] <- "Data"

environment <- xts(environment[,-1], environment$Data)

environment <- data.frame(Data = as.Date(index(environment)), lapply(environment, na.locf))

environment <- environment %>% filter(Data %in% datas_filtro)

## Social
social <- dbESG %>% dplyr::select(date, ticker, socialpillarscore) %>% 
  spread(ticker, socialpillarscore) %>% 
  dplyr::filter(date >= '2009-12-31')
colnames(social)[1] <- "Data"

social <- xts(social[,-1], social$Data)

social <- data.frame(Data = as.Date(index(social)), lapply(social, na.locf))

social <- social %>% filter(Data %in% datas_filtro)

indicadores <- list(esg_score, governance, environment, social)

# Adotamos uma metodologia de aplicar a mesma funcao as nossas 4 bases de dados
# Primeiro, ordenamos nossos dados cada ano (lista_indic)
lista_indic <- lapply(indicadores, function(x) params_estim(20101231, 20211231, c(12), c(1), x, "ESG", "asc", 0))

# Em seguida, fazemos o backtest para cada decil da amostra e para cada uma das 4 bases de dados
lista_ret_port <- lapply(lista_indic, function(x) backtest_decil(x[[1]], 20101231, 20211231, 10, "EW"))

# O retorno da linha acima são listas contendo o retorno 
lista_ret_port <- lapply(lista_ret_port, function(y) do.call("cbind.xts", lapply(y, function(x) do.call("rbind.xts", x))))

# Nomeamos cada coluna de cada df
for (i in seq_along(lista_ret_port)) {
  colnames(lista_ret_port[[i]]) <- paste0("D", seq(1,10))
}

# Geramos PDFs com plots de cada portfolio e cada indicador
pdf(file = "Portfolios/Plot_ports.pdf")

titulos <- c('ESG Score', 'Gov Score', 'Env Score', 'Social Score')
for (i in seq_along(lista_ret_port)) {
  charts.PerformanceSummary(lista_ret_port[[i]], main = titulos[i])
}

dev.off()

# Transformamos em Data Frame
esg_port <- data.frame(Data = index(lista_ret_port[[1]]), lista_ret_port[[1]])
gov_port <- data.frame(Data = index(lista_ret_port[[2]]), lista_ret_port[[2]])
env_port <-  data.frame(Data = index(lista_ret_port[[3]]), lista_ret_port[[3]])
social_port <- data.frame(Data = index(lista_ret_port[[4]]), lista_ret_port[[4]])

write.csv(esg_port, paste0(getwd(), '\\Portfolios\\port_esg.csv'), row.names = FALSE)
write.csv(gov_port, paste0(getwd(), '\\Portfolios\\port_gov.csv'), row.names = FALSE)
write.csv(env_port, paste0(getwd(), '\\Portfolios\\port_env.csv'), row.names = FALSE)
write.csv(social_port, paste0(getwd(), '\\Portfolios\\port_social.csv'), row.names = FALSE)


