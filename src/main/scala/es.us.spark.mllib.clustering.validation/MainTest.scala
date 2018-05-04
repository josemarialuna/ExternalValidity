package es.us.spark.mllib.clustering.validation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Josem on 26/09/2017.
  */
object MainTest {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()

    var featuresFile = "C:\\Users\\Josem\\Dropbox\\PHD\\Proyectos\\2017-08 - Clustering Trabajadores BACK\\Caracteristicas Demandas.csv"
    var clusterResultFile = "C:\\Users\\Josem\\Dropbox\\PHD\\Proyectos\\2017-08 - Clustering Trabajadores BACK\\Resultados Clusters\\KM200"
    var numClusters = 2
    var threshold = 20f


    if (args.length > 3) {
      featuresFile = args(0)
      clusterResultFile = args(1)
      numClusters = args(2).toInt
      threshold = args(3).toFloat
    }

    println("*******************************")
    println("*******FEATURING CLUSTER*******")
    println("*******************************")
    println("Configuration:")
    println("\tCLUSTERS: " + numClusters)
    println("\tFeatures File: " + featuresFile)
    println("\tCluster Result file: " + clusterResultFile)
    println("Running...\n")
    println("Loading files..")

    import spark.implicits._
    //val df = spark.sparkContext.parallelize(Seq((80, 0, 20), (10, 80, 10), (0, 0, 100))).toDF("A", "B", "C")
    //val df = spark.sparkContext.parallelize(Seq((60, 54, 46, 41), (40, 44, 53, 57))).toDF("A", "B", "C", "D")

    //val df = spark.sparkContext.parallelize(Seq((10L, 0L, 0L), (0L, 10L, 0L), (0L, 0L, 10L))).toDF("A", "B", "C")
    //val df = spark.sparkContext.parallelize(Seq((33.0, 33.0, 33.0), (33.0, 33.0, 33.0), (33.0, 33.0, 33.0))).toDF("A", "B", "C")

    //val df = spark.sparkContext.parallelize(Seq(("A", 0),("A", 0),("A", 0),("B",0),("B",0),("B",0),("C",0),("C",0),("C",0),("C",0),("A",1),("B",1),("B",1))).toDF("class", "prediction")
    val df = spark.sparkContext.parallelize(Seq((1.0, 0),(1.0, 0),(1.0, 0),(2.0,0),(2.0,0),(2.0,0),(3.0,0),(3.0,0),(3.0,0),(3.0,0),(1.0,1),(2.0,1),(2.0,1))).toDF("class", "prediction")

    /*
        println("ENTROPY:")
        val entropy = ExternalValidation.getEntropy(df)
        println(entropy)

        println("PURITY:")
        val purity = ExternalValidation.getPurity(df)
        println(purity)

        println("MUTUAL INFORMATION:")
        val mutualInformation = ExternalValidation.getMutualInformation(df)
        println(mutualInformation)

        println("F-MEASURE:")
        val fmeasure = ExternalValidation.getFMeasure(df)
        println(fmeasure)

        println("VARIATION OF INFORMATION:")
        val variationOfInformation = ExternalValidation.getVariationOfInformation(df)
        println(variationOfInformation)

        println("GOODMAN-KRUSKAL:")
        val goodmanKruskal = ExternalValidation.getGoodmanKruskal(df)
        println(goodmanKruskal)

        println("RAND INDEX:")
        val randIndex = ExternalValidation.getRandIndex(df)
        println(randIndex)

        println("ADJUSTED RAND INDEX:")
        val adjustedRandIndex = ExternalValidation.getAdjustedRandIndex(df)
        println(adjustedRandIndex)

        println("JACCARD:")
        val jaccard = ExternalValidation.getJaccard(df)
        println(jaccard)

        println("FOWLKES-MALLOWS:")
        val fowlkesMallows = ExternalValidation.getFowlkesMallows(df)
        println(fowlkesMallows)

        println("HUBERT:")
        val hubert = ExternalValidation.getHubert(df)
        println(hubert)

        println("MINKOWSKI:")
        val minkowski = ExternalValidation.getMinkowski(df)
        println(minkowski)
    */


    val res = FeatureStatistics.getTotalChiCross(List("class"), df, numClusters, "")
    //val res = FeatureStatistics.getTotalChi(List("class"), "",df,df, numClusters, "")

  }

}
