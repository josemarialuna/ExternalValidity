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
    val numClusters = 1
    val salto = 200
    var destino: String = Utils.whatTimeIsIt() + "-step" + salto


    //val resultados = for (salto <- 50 to 200 by 50) yield {
    //      val numClusters = 1
    var origen = randomPath + "Step" + salto + ".csv"
    val delimiter = ";"

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

    data.printSchema()


    val contingencyTable = ExternalValidation.getContingencyMatrix(data, numClusters)
    println("contingencyTable")
    //      contingencyTable.printSchema()
    contingencyTable.show()
    //      contingencyTable.repartition(1).write
    //        .option("header", "true")
    //        .option("delimiter", "\t")
    //        .csv(s"$destino-contingencyTable-$numClusters")

    val literatureIndices = ExternalValidation.calculateExternalIndices(contingencyTable.drop("prediction"))
    val riquelme = FeatureStatistics.getTotalChi(List("class"), "", data, data, numClusters, destino).split(",").mkString("\t").replace("(", "").replace(")", "\t")

    println(literatureIndices.toString)

    //      (numClusters, literatureIndices.toString, riquelme)
    //    }


    println("Saving results..")
    spark.sparkContext.parallelize(Seq(literatureIndices.toString, riquelme))
      .repartition(1)
      .saveAsTextFile(destino + "-ExternalIndices")

  }
}
