library(tidyverse)
library(readxl)
library(xts)
library(lubridate)
library(writexl)

choque <- read_xlsx('Tratamento dos Dados\\RawPrices.xlsx', sheet = 1)
  
choque1 <- choque[-1, ]
colnames(choque1)[1] <- "Data"

choque1$Data <- as.Date(as.numeric(choque1$Data), origin="1899-12-30")

precos <- data.frame(Data = choque1$Data, sapply(choque1[,-1], as.numeric))
  
colnames(precos) <- colnames(choque1)
  
ind <- read_csv('indice.csv')
ind <- ind %>% dplyr::filter(Data > "2001-12-31" & Data <= "2021-12-31")
  
###############################################################################
  
classe_precos <- merge(ind[,-2], precos, by = "Data", all.x = TRUE)
classe_precos <- classe_precos[ , colSums(is.na(classe_precos)) != nrow(classe_precos)]
  
# Realiamos iterações para preencher os NAs no meio da amostra
classe_precos_locf <- data.frame(matrix(ncol = ncol(classe_precos) - 1, nrow = nrow(classe_precos)))
for(i in 2:ncol(classe_precos)){
  ind_data <- ind[,1]
  # Selecionamos a data e a coluna do fundo para iterações ("i")
  suporte1 <- classe_precos[,c(1,i)]
  # Criamos um vetor que informa a posição das observações que não são NA
  NonNAindex <- which(!is.na(suporte1[,2]))
  # Filtramos para podermos trabalhar apenas com as datas antes de um fundo, possivelmente, fechar. Isso evitará problemas na hora de utilizar a função na.locf() 
  suporte2 <- suporte1[1:max(NonNAindex),]
  # Usamos a funçãona.locf para substituir os as observações faltantes pela observação anterior
  suporte3 <- na.locf(suporte2)
  classe_precos_locf[[i]] <- merge(ind_data, suporte3, by = "Data", all.x = TRUE)
  classe_precos_locf[[i]] <- classe_precos_locf[[i]][,2]
}
classe_precos_locf <- classe_precos_locf %>% select(!X1)
classe_precos_locf$Data <- classe_precos$Data
classe_precos_locf <- classe_precos_locf %>% select(Data, everything())
colnames(classe_precos_locf) <- colnames(classe_precos)
  
# Transforma os preços diários em retornos diários
classe_retornos <- data.frame(matrix(ncol = ncol(classe_precos_locf) - 1, nrow = nrow(classe_precos_locf)))
for(i in 2:ncol(classe_precos_locf)){
  # Transformamos a base de dados de preços em retornos
  classe_retornos[[i]] <- append(diff(classe_precos_locf[[i]])/classe_precos_locf[[i]][-length(classe_precos_locf[[i]])], NA, after = 0)
}
classe_retornos <- classe_retornos %>% select(!X1)
classe_retornos$Data <- classe_precos_locf$Data
classe_retornos <- classe_retornos %>% select(Data, everything())
colnames(classe_retornos) <- colnames(classe_precos_locf)
# Eliminamos a primeira linha, dado que ela só possui NAs (oriundos da função append)
classe_retornos <- classe_retornos[-1,]
  
write.csv(classe_retornos, "ativos.csv", row.names=FALSE)




