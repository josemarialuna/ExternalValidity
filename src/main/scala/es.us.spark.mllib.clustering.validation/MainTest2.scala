package es.us.spark.mllib.clustering.validation

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
  * Created by Josem on 26/09/2017.
  */
object MainTest2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()


    val susyFile = "C:\\datasets\\Validation\\SUSY.csv"

    val destino: String = Utils.whatTimeIsIt() + "susyFile"
    val numIterations = 1000
    val minClusters = 2
    val maxClusters = 10
    val origen = susyFile
    val idIndex = -1
    val classIndex = 0
    val delimiter = ","

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", delimiter)
      .csv(origen)

    dataRead.printSchema()


    println("Saving results..")
    dataRead.limit(100)
      .write
      .option("header", "false")
      .option("delimiter", delimiter)
      .csv(s"SUSY_limited2")

  }

}
