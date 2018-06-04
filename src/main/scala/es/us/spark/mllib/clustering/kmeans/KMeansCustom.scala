package es.us.spark.mllib.clustering.validation.kmeans

import es.us.spark.mllib.Utils
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeansEmpleo}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object KMeansCustom {
  def calculateKMeans(fileOriginal: String, numClusters: Int, numIterations: Int): RDD[(String, Int)] = {

    val spark = SparkSession.builder().getOrCreate()

    var origen: String = fileOriginal
    var destino: String = Utils.whatTimeIsIt()


    println("*******************************")
    println("*********CLUSTER SPARK*********")
    println("*******************************")
    println("Configuration:")
    println("\tCLUSTERS: " + numClusters)
    println("\tIterations: " + numIterations)
    println("\tInput file: " + origen)
    println("\tOutput File: " + destino)
    println("Running...\n")
    println("Loading file..")

    val data = spark.sparkContext.textFile(origen)
    //It skips the first line
    val skippedData = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

    //val dataRDD = parsedData.map(s => Vectors.dense(s.split(';').map(_.toDouble))).cache()
    val dataRDD = data
      .map(s => s.split("\t"))
      .keyBy(_.apply(0))
      .mapValues(_.drop(2))
      .mapValues(_.map(_.toDouble))
    //.mapPartitions(new scala.util.Random().shuffle(_))


    val parsedData = dataRDD.map(s => Vectors.dense(s._2)).cache()

    val start = System.nanoTime()

    val clusters = KMeansEmpleo.train(parsedData, numClusters, numIterations, "k-means||", Utils.whatTimeIsIt().toLong)

    val elapsed = (System.nanoTime() - start) / 1000000000.0
    println("TIEMPO TOTAL: " + elapsed)


    println("Cluster DONE!")
    dataRDD.mapValues(arr => clusters.predict(Vectors.dense(arr)))

  }

  def calculateBKM(fileOriginal: String, numClusters: Int, minDisibleSize: Double, numIterations: Int): RDD[(Double, Int)] = {

    val spark = SparkSession.builder().getOrCreate()

    val origen: String = fileOriginal
    val destino: String = Utils.whatTimeIsIt()


    println("*******************************")
    println("*********CLUSTER SPARK*********")
    println("*******************************")
    println("Configuration:")
    println("\tCLUSTERS: " + numClusters)
    println("\tIterations: " + numIterations)
    println("\tminDisibleSize: " + minDisibleSize)
    println("\tInput file: " + origen)
    println("\tOutput File: " + destino)
    println("Running...\n")
    println("Loading file..")

    val data = spark.sparkContext.textFile(origen)

    val dataRDD = data
      .map(s => s.split(",")
        .map(_.toDouble))
      .keyBy(_.apply(0))
      .mapValues(x => x.drop(2))
    //.mapPartitions(new scala.util.Random().shuffle(_))


    val parsedData = dataRDD.map(s => Vectors.dense(s._2)).cache()

    val start = System.nanoTime()

    //val clusters = KMeans.train(parsedData, numClusters, numIterations)
    val clusters = new BisectingKMeans()
      .setK(numClusters)
      .setMinDivisibleClusterSize(minDisibleSize)
      .run(parsedData)
    //val clusters = new GaussianMixture().setK(numClusters).setMaxIterations(numIterations).run(parsedData)
    var elapsed = (System.nanoTime() - start) / 1000000000.0
    println("TIEMPO TOTAL: " + elapsed)


    println("Cluster DONE!")
    dataRDD.mapValues(arr => clusters.predict(Vectors.dense(arr)))

  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    return if (s.isEmpty) 0 else s.toDouble
  }

}

