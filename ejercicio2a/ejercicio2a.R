library(ggplot2)
library(caret)
library(mlbench)
#load the data
measures_file <- read.csv("../ficheros/prediccion.csv",header=FALSE, sep=",")

#Creamos los set de datos para el entrenamiento y el test
training_index <- createDataPartition(y = measures_file$V146, p = .70, list = FALSE)
training_set <- measures_file[training_index,2:146]
test_set<- measures_file[-training_index,2:146]
write.csv(training_set,file="../ficheros/2a-training_set.csv")
write.csv(test_set,file="../ficheros/2a-test_set.csv")
# Cojemos el valor maximo de las filas de los datos que vamos a tener dentro del fichero, par
# asi poder normalizar y extrapolar los datos despues
max_value <- max(measures_file$V146)

# Mandamos una seed, para que las estadisticas no cambien con cada analisis
set.seed(2020)

# Definicion de las variables del tamaño de las capas ocultas y el decay
tune_grid_neural <- expand.grid(size = seq(from = 1, to = 6, by = 1),
                                           decay = seq(from = 0.1, to = 0.5, by = 0.1))


# part b: set some other consrains to be imposed on network (to keep computation manageable)
# see: p. 361 of Kuhn & Johnson (2013, 
#max_size_neaural <- max(tune_grid_neural$size)
#max_weights_neural <- max_size_neaural*(nrow(training_set) + 1) + max_size_neaural + 1


# Definicion de k-fold Cross-Validation
train_control_neural <- trainControl(method="repeatedcv",
                                     number=10,
                                     repeats =5)


# Entrenamiento del modelo, con las especificaciones ya definidas anteriormente
model <- train(V146/72.25762 ~ .,
        data = training_set,
        method = "nnet",
        tuneGrid = tune_grid_neural,
        trControl = train_control_neural,
        trace = TRUE,
        maxit = 1000,
        linout = 1)

#Mostramos los datos del modelo, y los valores para size y decay que dan un menos RMSE
model

# Multiplicamos la predccion por la escala para volver a tener los valores con los valores originales
nnet.predict <- predict(model)*72.25762
write.csv(nnet.predict,file="../ficheros/2a-prediction_training_set.csv")
# Error cuadratico medio
mean((nnet.predict - training_set$V146)^2)

# Realizamos una prediccion sobre los datos de test
predict_test <- predict(model, newdata=test_set)*72.25762
write.csv(predict_test,file="../ficheros/2a-prediction_test_set.csv")


