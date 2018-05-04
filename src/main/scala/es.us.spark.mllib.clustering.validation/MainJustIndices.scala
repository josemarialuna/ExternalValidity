package es.us.spark.mllib.clustering.validation

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Josem on 26/09/2017.
  */
object MainJustIndices {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()

    val default = "C:\\datasets\\Validation\\Synthetics\\Default.csv"
    val randomPath = "C:\\datasets\\Validation\\Synthetics\\"
    val numClusters = 3
    val salto = 40
    var destino: String = Utils.whatTimeIsIt() //+ "-step" + salto


//    val resultados = for (salto <- 25 to 75 by 25) yield {
//      var numClusters = 3
      val fileName = "Incorrect" + salto + ".csv"
      var origen = randomPath + "Incorrect" + salto + ".csv"
      //var origen = default
      val delimiter = ";"

//      if (salto >= 100) numClusters = 2
//      if (salto >= 200) numClusters = 1

      if (args.length > 4) {
        origen = args(2)
        destino = args(3)

      }

      println("Loading file..")

      val data = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .csv(origen)

      //      println("data")
      //      data.printSchema()
      //      data.show()

      val contingencyTable = data.stat.crosstab("prediction", "class")
      println("contingencyTable")
      contingencyTable.printSchema()
      contingencyTable.show()
      //      contingencyTable.repartition(1).write
      //        .option("header", "true")
      //        .option("delimiter", "\t")
      //        .csv(s"$destino-contingencyTable-$numClusters")

      val literatureIndices = ExternalValidation.calculateExternalIndices(contingencyTable.drop("prediction_class"))

      val riquelme = FeatureStatistics.getTotalChi(List("class"), "", data, data, numClusters, destino + fileName).split(",").mkString("\t").replace("(", "").replace(")", "\t")

      println(literatureIndices.toString)

    println(riquelme)

//      (numClusters, literatureIndices.toString, riquelme)
//    }


    def cleanString(palabro: String) = {
      palabro.split(",").mkString("\t").replace("(", "").replace(")", "\t")
    }

    println("Saving results..")
    spark.sparkContext.parallelize(Seq(literatureIndices.toString, riquelme))
//    spark.sparkContext.parallelize(resultados)
//      .map(x => x._1 + "\t" + cleanString(x._2) + "\t" + cleanString(x._3))
      .repartition(1)
      .saveAsTextFile(destino + "-ExternalIndices")

  }
}
