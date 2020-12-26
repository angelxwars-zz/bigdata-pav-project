import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.hadoop.fs.{FileSystem, Path}


object Ejercicio2c {
     def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.WARN)
     Logger.getLogger("akka").setLevel(Level.WARN)
    
     val conf = new SparkConf().setAppName("Ejercicio3").setMaster("local")
     val sc = new SparkContext(conf)
     val fs = FileSystem.get(sc.hadoopConfiguration)

     //Vamos a coger todos los datos, porque para clastering no necesitamos particion
     val rddFromFile = sc.textFile("../../ficheros/consumo.csv").cache()
     val rddHeader = rddFromFile.first()

     val rddSplit = rddFromFile.filter(_ != rddHeader).map(_.split(","))
     val rddMedidas = rddSplit.map(_.drop(1)).map(_.drop(2)).map(_.map(_.toDouble))
      
     //Cojemos los sesores y las fechas para asi poder guardarlas luego con las labels
     val rddSensorFechaIndex = rddSplit.zipWithIndex.map(x => (x._2, x._1(0) + "," +x._1(1)))
     
     //Vector de medidas
     val dataSet = rddMedidas.map(row => Vectors.dense(row.take(row.length - 1)))
     // Cluster the data into two classes using KMeans
     val numClusters = 5
     val numIterations = 20
     // Entrenamos el modelo de datos
     val clusters = KMeans.train(dataSet, numClusters, numIterations)
     //Obtencion de las labels de los clusters
     val clusterLabels = clusters.predict(dataSet)
     // añadimos a las labels el indice para poder ejecutar el join posteriormente
     val clusterLabelsIndex = clusterLabels.zipWithIndex.map(x => (x._2, x._1))
     
     //Ordenamos por el indice de la union, lo eliminamos y convertimos la label en un string
     val sensorFechaLabel = rddSensorFechaIndex.join(clusterLabelsIndex).sortBy(_._1).map(_._2).map(x => x._1 + "," + x._2.toString)
     //Unimos la cabecera y escribimos el fichero
     val sensorFechaLabelHeader = sc.parallelize(List("Sensor, Fecha, Label"))
     
     val dataUnion = sensorFechaLabelHeader.union(sensorFechaLabel)
     
     dataUnion.repartition(1).saveAsTextFile("./output_tmp")

     fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/2c-clusters.csv"))
     fs.delete(new Path("./output_tmp"), true)

     
     // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(dataSet)
      println(s"Within Set Sum of Squared Errors = $WSSSE")
      
      
      // Añadimos a la agrupacion de cluster por dias las fechas medias para las graficas.
      val numeroMedidas = rddMedidas.first().length
      //Media de los elementos con los indices para poder hacer el join
      val rddMediaElementos = rddMedidas.map(_.reduce(_+_)/numeroMedidas).zipWithIndex.map(x => (x._2, x._1))
      
      val rddSFIMedias = sensorFechaLabel.zipWithIndex.map(x => (x._2, x._1)).join(rddMediaElementos).sortBy(_._1).map(_._2).map(x => x._1 + "," + x._2.toString)


      val rddSFIMediasHeader = sc.parallelize(List("Sensor, Fecha, Label, Media"))
      
      //val SFIMedias = .join(rddSFIMedias).sortBy(_._1).map(_._2).map(x => x._1 + "," + x._2.toString)

      val dataUnionSFIM = rddSFIMediasHeader.union(rddSFIMedias)
      dataUnionSFIM.repartition(1).saveAsTextFile("./output_tmp")

      fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/2c-clusters-medias.csv"))
      fs.delete(new Path("./output_tmp"), true)
      
      // Puntos medios de los centroides
      val mediaCentroides = clusters.clusterCenters.map(x => x.toArray).map(_.reduce(_+_)/numeroMedidas).map(_.toString)
      sc.parallelize(mediaCentroides).repartition(1).saveAsTextFile("./output_tmp")
      mediaCentroides.foreach(println)
      fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/2c-clusters-centros.csv"))
      fs.delete(new Path("./output_tmp"), true)
      
     }
}