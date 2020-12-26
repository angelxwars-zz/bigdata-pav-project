import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.StructType
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Ejercicio1 {
   def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
     
      val conf = new SparkConf().setAppName("Ejercio 1").setMaster("local")
      val sc = new SparkContext(conf)
      val rddFromFile = sc.textFile("../../ficheros/consumo.csv").cache()
      rddFromFile.count()
      
      val rddCompleto = rddFromFile.filter(_.contains("DG1000420")).map(_.split(","))
      // Eliminamos el campo del nombre del sensor
      val rddFechaMedidas = rddCompleto.map(_.drop(1))
      // Eliminamos tanto el campo del nombre del sensor como del tiempo
      val rddMedidas = rddCompleto.map(_.drop(1)).map(_.drop(2)).map(_.map(_.toDouble))

      //El zipWithIndex crea a cada elemento un indice, al cual accedemos con el x_2, y el x._1 es el valor del elemento
      //sumamos uno porque con este indice vamos a hacer el join, y para hacerlo lo hacemos con el indice, el cual es la media del dia siguiente
      val rddCompletoDictionary = rddFechaMedidas.zipWithIndex.map(x => (x._2+1, x._1.mkString(", ")))
      // Numero de medidas para hacer la media
      val numeroMedidas = rddMedidas.first().length
      //Media de los elementos con los indices para poder hacer el join
      val rddMediaElementos = rddMedidas.map(_.reduce(_+_)/numeroMedidas).zipWithIndex.map(x => (x._2, x._1))

      // DataSet final, con las medidas y la media, quitamos el primer elemento que es el indice y exportamos
      val finalDataset = rddCompletoDictionary.join(rddMediaElementos).sortBy(_._1).map(_._2)
      finalDataset.map(x => (x._1 + ", " + x._2)).saveAsTextFile("./output_tmp")
      val fs = FileSystem.get(sc.hadoopConfiguration)
      fs.rename(new Path("./output_tmp/part-00000"), new Path("../../ficheros/prediccion.csv"))

      // Remove output directory
      fs.delete(new Path("./output_tmp"), true)
      
      println("Proceso finalizado con exito")
   }


}