import java.io.File
import java.io.PrintWriter

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest



object Ejercicio2b {
     def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.WARN)
     Logger.getLogger("akka").setLevel(Level.WARN)
    
    //Configuraciones
     val conf = new SparkConf().setAppName("Ejercicio2").setMaster("local")
     val sc = new SparkContext(conf)
     val fs = FileSystem.get(sc.hadoopConfiguration)
     
     //Carga de datos
     val dataTrainingRaw = sc.textFile("../../ficheros/2a-training_set.csv").cache()
     val dataTestRaw = sc.textFile("../../ficheros/2a-test_set.csv").cache()
     
     val dataTrainingHeader = dataTrainingRaw.first()
     val dataTestHeader = dataTestRaw.first()
     
     val dataTraining = dataTrainingRaw.filter(_!= dataTrainingHeader)
     val dataTest = dataTestRaw.filter(_!= dataTestHeader)
          
     val dataTraining_no_index_split = dataTraining.map(_.split(",")).map(_.drop(1))
     val dataTest_no_index_split = dataTest.map(_.split(",")).map(_.drop(1))

      val predictionNNETRaw = sc.textFile("../../ficheros/2a-prediction_test_set.csv").cache()
      val predictionNNETRawHeader = predictionNNETRaw.first()
      
     //Transformacion de los datos en LabeledPoing para usar el modelo
     val trainingSet = dataTraining_no_index_split.map(row => new LabeledPoint(row.last.toDouble, Vectors.dense(row.take(row.length - 1)
         .map(string => string.toDouble))))
     val testSet = dataTest_no_index_split.map(row => new LabeledPoint(row.last.toDouble, Vectors.dense(row.take(row.length - 1)
         .map(string => string.toDouble))))

     // Parametros de configuracion para el modelo RandomForest
     val categoricalFeaturesInfo = Map[Int, Int]()
     val maxDepth = 5
     val numTrees = 100
     val impurity = "variance"
     val featureSubsetStrategy = "auto"
     val maxBins = 32
    
     // Entrenamiento del modelo RandomForest
     val model = RandomForest.trainRegressor(trainingSet, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
     
     // Guardamos la estructura de los arboles para la visualizacion
     val pw = new PrintWriter(new File("../../ficheros/2b-random_forest.txt" ))
     pw.write(model.toDebugString)
     pw.close
     
     
     
     val labelsAndPredictions  = testSet.map { point =>
       val prediction = model.predict(point.features)
        (point.label, prediction)
      }
     // Metricas del modelo a partir del set de test
      // Instanciacion
      val metrics = new RegressionMetrics(labelsAndPredictions)
      // Mean Absolute Error (Error absoluto medio)
      println("\nMetricas")
      println(s"MAE = ${metrics.meanAbsoluteError}")
      // Squared error
      println(s"MSE = ${metrics.meanSquaredError}")
      println(s"RMSE = ${metrics.rootMeanSquaredError}")
      
      
      // Hacemos la prediccion para el conjunto de tests y lo guardamos
      val testFeaturesSet = testSet.map(_.features)
      val prediction = model.predict(testFeaturesSet).zipWithIndex.map(x => (x._2 + "," +x._1))
      prediction.foreach(println)
      prediction.saveAsTextFile("./output_tmp")
      //val prediction = labelsAndPredictions.map(_._2)
      fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/2b-test_prediction.csv"))
      fs.delete(new Path("./output_tmp"), true)
      
      // Creamos los rdd con indice para los resultados obtenidos en la prediccion, y para los cargados del obtenido con nnet
      val prediction_index = prediction.zipWithIndex.map(row => (row._2, row._1))
      val predictionRandomTree = prediction_index.map(_._2).zipWithIndex.map(x => (x._2, x._1))
      val predictionNNET = predictionNNETRaw.filter(_!= predictionNNETRawHeader)
      .map(_.split(",").drop(1)).zipWithIndex.map(x => (x._2, x._1.mkString(", ")))
      // Unimos a pares las comparaciones con los dos modelos
      val predictioncomparation = predictionRandomTree.join(predictionNNET).sortBy(_._1).map(_._2)
      val predictioncomparationString = predictioncomparation.map(x => (x._1 + ", " + x._2))
      // Creamos la cabecera para añadir al csv de la comparacion
      val rddheadercomparation = sc.parallelize(List("RandomTree, NNET"))
      //Unimos la cabecera y escribimos el fichero
      val predictioncomparationHeader = rddheadercomparation.union(predictioncomparationString)
      predictioncomparationHeader.repartition(1).saveAsTextFile("./output_tmp")
      fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/2b-comparativa_RandomTree_NNET.csv"))
      fs.delete(new Path("./output_tmp"), true)
      
      // Mostramos las comparaciones     
      println("\n\nRandomTree prediction")
      predictionRandomTree.foreach(println)
      
      println("\n\nNeural Network prediction")
      predictionNNET.foreach(println)
      
      // Para la grafica un fichero con las prediccionesn del conjunto de entrenamiento 
      val training_features_set = trainingSet.map(_.features)
      val prediction_training = model.predict(training_features_set).zipWithIndex.map(x => (x._2 + "," +x._1))
      prediction_training.foreach(println)
      prediction_training.saveAsTextFile("./output_tmp")
      fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/2b-training_prediction.csv"))
      fs.delete(new Path("./output_tmp"), true)

     
     }
}