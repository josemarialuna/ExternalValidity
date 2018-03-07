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
object MainRiquelme {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()

    val irisFile = "C:\\datasets\\Validation\\iris.data"
    val wineFile = "C:\\datasets\\Validation\\wine.data"
    val yeastFile = "C:\\datasets\\Validation\\yeast.data"
    val wineQualityFile = "C:\\datasets\\Validation\\winequality-white.csv"
    val bankNoteFile = "C:\\datasets\\Validation\\data_banknote_authentication.txt"
    val breastFile = "C:\\datasets\\Validation\\breast-cancer-wisconsin.data"
    val columnFile = "C:\\datasets\\Validation\\vertebral_column_data\\column_3C.dat"
    val habermanFile = "C:\\datasets\\Validation\\haberman.data"
    val seedFile = "C:\\datasets\\Validation\\seeds_dataset.txt"
    val vehiclesFile = "C:\\datasets\\Validation\\vehicles.dat"
    val data_UserFile = "C:\\datasets\\Validation\\Data_User.csv"
    val waveformFile = "C:\\datasets\\Validation\\waveform.data"
    val relaxFile = "C:\\datasets\\Validation\\planing_relax.txt"
    val susyFile = "C:\\datasets\\Validation\\SUSY.csv"

    val synthetic = "C:\\datasets\\Validation\\Synthetics\\C3-D5-I1000"
    val susy_limited = "C:\\Users\\Josem\\Documents\\ExternalValidity\\SUSY_limited2"

    val emoticonsFile = "C:\\datasets\\Validation\\multilabel\\emotions\\emotions.dat"
    val sceneFile = "C:\\datasets\\Validation\\multilabel\\scene\\scene.dat"

    val numIterations = 1000
    var minClusters = 4
    var maxClusters = 5
    var origen = columnFile
    var destino: String = Utils.whatTimeIsIt() + "-columnFile"
    var idIndex = -1
    var classIndex = 6
    var delimiter = " "

    if (args.length > 3) {
      minClusters = args(0).toInt
      maxClusters = args(1).toInt
      origen = args(2)
      destino = args(3)
      classIndex = args(4).toInt
      delimiter = args(5)
      idIndex = args(6).toInt
    }

    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(origen)

    dataRead.printSchema()

    //Si el fichero tiene indice, se le dropea, si no símplemente cambiamos el nombre a la columna
    val data = if (idIndex != -1) {
      dataRead.drop(s"_c$idIndex")
        .withColumnRenamed(dataRead.columns(classIndex), "class")
    } else {
      dataRead.withColumnRenamed(dataRead.columns(classIndex), "class")
      //.drop("_c72", "c_74", "c_75", "c_76", "c_77")
    }

    //data = data.withColumn("class", when(col("class") > "0", "1").otherwise("0")).cache

    //data.show()
    data.printSchema()

    val resultados = for {numClusters <- minClusters to maxClusters} yield {

      val start = System.nanoTime()
      println("*******************************")
      println("*********CLUSTER SPARK*********")
      println("*******************************")
      println("Configuration:")
      println("\tCLUSTERS: " + numClusters)
      println("Running...\n")
      println("Loading file..")

      val featureColumns = data.drop("class").columns
      val featureAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
      val df_kmeans = featureAssembler.transform(data).select("class", "features")
      //df_kmeans.show()

      val kmeans = new KMeans().setK(numClusters)
        .setSeed(1L)
        .setMaxIter(numIterations)
        .setFeaturesCol("features")
      val model = kmeans.fit(df_kmeans)

      var predictionResult = model.transform(df_kmeans)
        .select("class", "prediction")

      predictionResult = predictionResult.withColumn("prediction", predictionResult("prediction").cast(DoubleType))


      val elapsed = (System.nanoTime() - start) / 1000000000.0
      println("TIEMPO TOTAL: " + elapsed)
      println("Cluster DONE!")

      //Featuring
      predictionResult.show()
      predictionResult.repartition(1).write
        .option("header", "true")
        .option("delimiter", "\t")
        .csv(s"$destino-kmeansRes-$numClusters")


      val res = FeatureStatistics.getTotalChi(List("class"), "", predictionResult, predictionResult, numClusters, destino)
      (numClusters, res)

    }
    println("Saving results..")
    spark.sparkContext.parallelize(resultados)
      .map(x => x._1 + "\t" + x._2.split(",").mkString("\t").replace("(", "").replace(")", "\t"))
      .repartition(1)
      .saveAsTextFile(s"$destino-loopingChi")

  }

}
