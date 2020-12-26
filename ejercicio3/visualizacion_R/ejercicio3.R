library(nlme)
library(ggplot2)

theme_set(theme_bw())

datos_reales_entrenamiento <- read.csv("../../ficheros/2a-training_set.csv",header=TRUE, sep=",")
datos_reales_test <- read.csv("../../ficheros/2a-test_set.csv",header=TRUE, sep=",")
resultados_reales_entrenamiento = datos_reales_entrenamiento[,146]
resultados_reales_test = datos_reales_test[,146]

# ------------------------- Graficas Ejercicio 2a NNET -------------------------

datos_prediccion_entrenamiento_nnet <- read.csv("../../ficheros/2a-prediction_training_set.csv",header=TRUE, sep=",")
datos_prediccion_test_nnet <- read.csv("../../ficheros/2a-prediction_test_set.csv",header=TRUE, sep=",")

datos_entrenamiento_nnet <- datos_prediccion_entrenamiento_nnet
datos_entrenamiento_nnet$reales <- resultados_reales_entrenamiento

datos_test_nnet <- datos_prediccion_test_nnet
datos_test_nnet$reales <- resultados_reales_test

jpeg("../Graficas/grafica_2a_entrenamiento_nnet.jpeg")
ggplot(datos_entrenamiento_nnet, aes(x = reales, y = x) ) +
  labs(x = "Datos Reales",y = "Datos Predicciones")+
  geom_point() +
  geom_abline(intercept=0, slope=1)
dev.off()

jpeg("../Graficas/grafica_2a_test_nnet.jpeg")
ggplot(datos_test_nnet, aes(x = reales, y = x) ) +
  labs(x = "Datos Reales",y = "Datos Predicciones")+
  geom_point() +
  geom_abline(intercept=0, slope=1)
dev.off()

# -------------------- Graficas Ejercicio 2b Random Forest ---------------------

datos_prediccion_entrenamiento_rf <- read.csv("../../ficheros/2b-training_prediction.csv",header=FALSE, sep=",")
datos_prediccion_test_rf <- read.csv("../../ficheros/2b-test_prediction.csv",header=FALSE, sep=",")

datos_entrenamiento_rf <- datos_prediccion_entrenamiento_rf
datos_entrenamiento_rf$reales <- resultados_reales_entrenamiento

datos_test_rf <- datos_prediccion_test_rf
datos_test_rf$reales <- resultados_reales_test

jpeg("../Graficas/grafica_2b_entrenamiento_rf.jpeg")
ggplot(datos_entrenamiento_rf, aes(x = reales, y = V2) ) +
  labs(x = "Datos Reales",y = "Datos Predicciones")+
  geom_point() +
  geom_abline(intercept=0, slope=1)
dev.off()

jpeg("../Graficas/grafica_2b_test_rf.jpeg")
ggplot(datos_test_rf, aes(x = reales, y = V2) ) +
  labs(x = "Datos Reales",y = "Datos Predicciones")+
  geom_point() +
  geom_abline(intercept=0, slope=1)
dev.off()

# ------------------------- Graficas Ejercicio 2c Knn --------------------------

datos_clusters <- read.csv("../../ficheros/2c-clusters-medias.csv",header=TRUE, sep=",")
centros_clusters <- read.csv("../../ficheros/2c-clusters-centros.csv",header=FALSE, sep=",")


datos_clusters_cortados = datos_clusters[1:400,]


jpeg("../Graficas/grafica_2c_knn.jpeg")
ggplot(datos_clusters_cortados, aes(x = Sensor, y = Media) ) +
  theme(axis.text.x  = element_text(angle = 90, vjust = 0.5, hjust=1))+
  geom_point(aes(color=factor(Label))) + 
  geom_hline(yintercept=centros_clusters[1, 1])+
geom_hline(yintercept=centros_clusters[2, 1]) +
geom_hline(yintercept=centros_clusters[3, 1]) +
geom_hline(yintercept=centros_clusters[4, 1]) +
geom_hline(yintercept=centros_clusters[5, 1])
dev.off()

datos_clusters_DG1000420 = datos_clusters[datos_clusters$Sensor == "DG1000420",]
jpeg("../Graficas/grafica_2c_knn_DG1000420.jpeg")
ggplot(datos_clusters_DG1000420, aes(x = Fecha, y = Media) ) +
  theme(axis.text.x  = element_text(angle = 90, vjust = 0.5, hjust=1))+
  geom_point(aes(color=factor(Label))) + 
  geom_hline(yintercept=centros_clusters[1, 1])+
  geom_hline(yintercept=centros_clusters[2, 1]) +
  geom_hline(yintercept=centros_clusters[3, 1]) +
  geom_hline(yintercept=centros_clusters[4, 1]) +
  geom_hline(yintercept=centros_clusters[5, 1])
dev.off()


















