import es.us.spark.mllib.Utils
import es.us.spark.mllib.clustering.validation.FeatureStatistics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
  * Created by Josem on 26/09/2017.
  */
object MainExternalTest {

  val RIQUELME = 1
  val LITERATURE = 2
  val ALL = 0

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Featuring Clusters")
      .master("local[*]")
      .getOrCreate()

    val irisFile = "iris.data"

    val numIterations = 1000
    var minClusters = 2
    var maxClusters = 10
    val origen = irisFile
    val destino: String = Utils.whatTimeIsIt()
    var idIndex = "-1"
    val classIndex = 4
    val delimiter = ","

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
      println(s"\tFile: $origen")
      println("Running...\n")
      println("Loading file..")

      val featureColumns = data.drop("class").columns
      val featureAssembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
      val df_kmeans = featureAssembler.transform(data).select("class", "features")
      //df_kmeans.show()

      //val clusteringResult = new KMeans()
      //      val clusteringResult = new GaussianMixture()
      //val clusteringResult = new LDA()
      val clusteringResult = new BisectingKMeans()
        .setK(numClusters)
        .setSeed(1L)
        .setMaxIter(numIterations)
        .setFeaturesCol("features")
      val model = clusteringResult.fit(df_kmeans)

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


      val res = FeatureStatistics.getTotalChiCross(List("class"), predictionResult, numClusters, destino)
      (numClusters, res)

    }
    println("Saving results..")
    spark.sparkContext.parallelize(resultados)
      .map(x => x._1 + "\t" + x._2.split(",").mkString("\t").replace("(", "").replace(")", "\t"))
      .repartition(1)
      .saveAsTextFile(s"$destino-loopingChi")


  }

}
