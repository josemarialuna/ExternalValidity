package es.us.spark.mllib.clustering.validation.featuring

import es.us.spark.mllib.Utils
import es.us.spark.mllib.clustering.validation.FeatureStatistics
import es.us.spark.mllib.clustering.validation.kmeans.KMeansCustom
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
  * Created by Josem on 26/09/2017.
  */
object MainLoopFeatures {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()


    val fileOriginal = "C:\\Users\\Josem\\Dropbox\\PHD\\Proyectos\\2017-08 - Clustering Trabajadores BACK\\New Dataset por Provincias\\2014-16\\matrizRelativaPor1"
    var destino: String = Utils.whatTimeIsIt()
    var numIterations = 1000
    val minClusters = 51
    val maxClusters = 80

    val resultados = for {numClusters <- minClusters to maxClusters} yield {

      val kmeansResult = KMeansCustom.calculateKMeans(fileOriginal, numClusters, numIterations)


      var featuresFile = "C:\\Users\\Josem\\Dropbox\\PHD\\Proyectos\\2017-08 - Clustering Trabajadores BACK\\New Dataset por Provincias\\2014-16\\Features_Demanda.csv"

      println("*******************************")
      println("*******FEATURING CLUSTER*******")
      println("*******************************")
      println("Configuration:")
      println("\tCLUSTERS: " + numClusters)
      println("\tFeatures File: " + featuresFile)
      println("Running...\n")
      println("Loading files..")

      import spark.implicits._
      val dfAux = kmeansResult.toDF()

      //Casting
      val dfResults = dfAux.select(
        dfAux("_1").cast(StringType).as("id"),
        dfAux("_2").cast(IntegerType).as("prediction")
      )
//      dfResults.printSchema()
//      dfResults.show()
      val dfAux2 = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("delimiter", ";")
        .csv(featuresFile)

      //Casting
      val dfFeatures = dfAux2.select(
        dfAux2("id_dem").cast(StringType).as("id"),
        dfAux2("ccaa_trab1").cast(StringType).as("ca"),
        dfAux2("prov_trab1").cast(StringType).as("provincia"),
        dfAux2("ocup_trab").cast(StringType).as("ocupacion"),
        dfAux2("act_trab1").cast(StringType).as("actividad")
      )
//      dfFeatures.printSchema()
//      dfFeatures.show()
      println("Matching Data..")
      //      df.join(df2, "id")
      //        .write.bucketBy(numClusters, "cluster")
      //        .saveAsTable("dfJoin")


      val dfJoin = dfResults.join(dfFeatures, "id").cache()

//      dfJoin.printSchema()
//      dfJoin.show()


      val listaColumnas = List("provincia")
      //val listaColumnas = List("ca", "ocupacion", "actividad", "provincia")

      //val totalChi = FeatureStatistics.getTotalChi(listaColumnas, "\t", df2, dfJoin, numClusters)
      //(numClusters, totalChi)
      val res = FeatureStatistics.getTotalChiCross(listaColumnas, dfJoin, numClusters, destino)
      (numClusters, res)


    }

    println("Saving global results..")
    spark.sparkContext.parallelize(resultados)
      .map(x => x._1 + "\t" + x._2.split(",").mkString("\t").replace("(", "").replace(")", "\t"))
      .repartition(1)
      .saveAsTextFile(s"$destino-loopingChi")

  }

}
