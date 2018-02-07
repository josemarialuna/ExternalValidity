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
object MainLoop {
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

    val destino: String = Utils.whatTimeIsIt()
    val numIterations = 1000
    val minClusters = 2
    val maxClusters = 15
    val origen = yeastFile
    val classIndex = 9


    val dataRead = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(origen)

    dataRead.printSchema()

    //val data = dataRead.withColumnRenamed(dataRead.columns(dataRead.columns.length - 1), "class")
    val data = dataRead.drop("_c0").withColumnRenamed(dataRead.columns(classIndex), "class")
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

      val contingencyTable = ExternalValidation.getContingencyMatrix(predictionResult, numClusters)
      println("contingencyTable")
      //      contingencyTable.printSchema()
      contingencyTable.show()

      val res = ExternalValidation.calculateExternalIndices(contingencyTable.drop("prediction"))
      (numClusters, res)
      //println(res.toString)

      /*
            var featuresFile = "C:\\Users\\Josem\\CloudStation\\Agrupados 96\\Features.csv"

            println("*******************************")
            println("*******FEATURING CLUSTER*******")
            println("*******************************")
            println("Configuration:")
            println("\tCLUSTERS: " + numClusters)
            println("\tFeatures File: " + featuresFile)
            println("Running...\n")
            println("Loading files..")
            val dfAux = kmeansResult.toDF()

            //      val dfAux = spark.read
            //        .option("header", "false")
            //        .option("inferSchema", "false")
            //        .option("delimiter", "\t")
            //        .csv(clusterResultFile)

            //Casting
            val df = dfAux.select(
              dfAux("_1").cast(StringType).as("id"),
              dfAux("_2").cast(IntegerType).as("cluster")
            )


            val dfAux2 = spark.read
              .option("header", "false")
              .option("inferSchema", "false")
              .option("delimiter", ";")
              .csv(featuresFile)

            //Casting
            val df2 = dfAux2.select(
              dfAux2("_c0").cast(StringType).as("id"),
              dfAux2("_c1").cast(StringType).as("edificio"),
              dfAux2("_c2").cast(StringType).as("estacion"),
              dfAux2("_c3").cast(StringType).as("mes"),
              dfAux2("_c4").cast(StringType).as("dia")
            )

            println("Matching Data..")
            //      df.join(df2, "id")
            //        .write.bucketBy(numClusters, "cluster")
            //        .saveAsTable("dfJoin")


            val dfJoin = df.join(df2, "id").cache()


            val listaColumnas = List("edificio", "estacion", "mes", "dia")

            //val totalChi = FeatureStatistics.getTotalChi(listaColumnas, "\t", df2, dfJoin, numClusters)
            //(numClusters, totalChi)
            val matrixChi = FeatureStatistics.getMatrixChi(listaColumnas, "\t", df2, dfJoin, numClusters)

            println(s"$numClusters")
            //matrixChi.show(numClusters)

            (numClusters, matrixChi)

          }

          resultados.map(x => x._2.repartition(1).write
            .option("header", "true")
            .option("delimiter", "\t")
            .csv(s"$destino-Energies-C${x._1}-Result"))

          //    val rddResult = spark.sparkContext.parallelize(resultados)

          //rddResult.repartition(1).map(x => x._1 + "\t" + x._2).saveAsTextFile("Resultss")
      */


    }

    spark.sparkContext.parallelize(resultados)
      .repartition(1)
      .mapValues(_.toString().replace("(", "").replace(")", ""))
      .map(x => x._1.toInt + "\t" + x._2)
      .saveAsTextFile(Utils.whatTimeIsIt() + "ExternalIndices")

  }
}
