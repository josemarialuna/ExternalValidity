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
    val higgsFile = "C:\\datasets\\Validation\\HIGGS\\HIGGS.csv"

    val destino: String = Utils.whatTimeIsIt() + "-higgs200k"
    val origen = higgsFile
    val delimiter = ","

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(origen)

    dataRead.printSchema()


    println("Saving results..")
    dataRead.limit(200000)
      .write
      .option("header", "false")
      .option("delimiter", delimiter)
      .csv(s"$destino")

  }

}
