package es.us.spark.mllib.clustering.validation

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Josem on 26/09/2017.
  */
object MainTestRiquelme {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()

    val file = "C:\\datasets\\toy\\6.csv"

    var numClusters = 6

    var origen = file
    var destino: String = Utils.whatTimeIsIt() + "-columnFile"
    var delimiter = "\t"

    val dataRead = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(origen)


    val res = FeatureStatistics.getTotalChi(List("class"), "", dataRead, dataRead, numClusters, destino)

    println(s"$res")
    //    spark.sparkContext.parallelize(resultados)
    //      .map(x => x._1 + "\t" + x._2.split(",").mkString("\t").replace("(", "").replace(")", "\t"))
    //      .repartition(1)
    //      .saveAsTextFile(s"$destino-loopingChi")

  }

}
