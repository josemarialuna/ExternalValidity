package es.us.spark.mllib.clustering.validation

import java.util.NoSuchElementException

import es.us.spark.mllib.Utils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by Josem on 27/09/2017.
  */
object Feature extends Logging {

  /**
    * Returns a dataframe with the relative frequency by cluster
    *
    * @param featureName  Name of the column
    * @param list_feature List of the names of the distinct features of a column
    * @param dfJoin       Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters  The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def calculateRatioByCluster(featureName: String, list_feature: Array[String], dfJoin: DataFrame, numClusters: Int): DataFrame = {

    //require(numClusters > 1, "The number of cluster should be more than 1.")

    logInfo(s"Processing ratio by cluster: $featureName..")

    val dfTotalClusters = dfJoin.groupBy("prediction")
      .count()
      .withColumnRenamed("count", "totalCluster")
      .cache()

    val rows = list_feature.map { feature =>

      val dfFeatures = dfJoin.groupBy("prediction", s"$featureName")
        .count()
        .withColumnRenamed("count", "totalFeatureCluster")

      val dfDefinitivo = dfTotalClusters.join(dfFeatures, "prediction")
        .withColumn("abs", col("totalFeatureCluster") / col("totalCluster") * 100)
        .select("prediction", s"$featureName", "abs")
        .sort("prediction", s"$featureName")
        .repartition(col("prediction"))
      //dfDefinitivo.show(20)


      val columna = for (cluster <- 0 until numClusters) yield {

        val clusterRatio = try {
          dfDefinitivo.select("abs")
            .where(s"prediction == '$cluster'")
            .where(s"$featureName == '$feature'")
            .first()
            .getDouble(0)
        } catch {
          case ex: NoSuchElementException => {
            0.0
          }
        }

        (cluster, clusterRatio)
      }
      //
      //      dfDefinitivo.unpersist()

      val spark = SparkSession.builder().getOrCreate()


      val schema = Array(
        StructField("prediction", IntegerType, true),
        StructField(s"$feature", DoubleType, true))

      val customSchema = StructType(schema)

      val columnaRDD = spark.sparkContext.parallelize(columna)
      val filas = columnaRDD.map(Row.fromTuple(_))

      logInfo(s"\tValue: $feature DONE!")


      spark.createDataFrame(filas, customSchema)

    }

    dfTotalClusters.unpersist()

    logInfo(s"$featureName finished!\n")
    rows.reduce(_.join(_, "prediction")).sort("prediction")
  }

  /**
    * Returns a dataframe with the relative frequency by cluster
    *
    * @param featureName  Name of the column
    * @param list_feature List of the names of the distinct features of a column
    * @param dfJoin       Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters  The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example calculateColocationsRatioByCluster(list_cities, dfResults, 6, 20)
    */
  def calculateColocationsRatioByCluster(featureName: String, list_feature: Array[String], dfJoin: DataFrame, numClusters: Int): DataFrame = {

    require(numClusters > 1, "The number of cluster should be more than 1.")

    logInfo(s"Processing colocation ratio by cluster: $featureName..")

    //DF: cluster number | total of colocations by cluster
    val dfTotalClusters = dfJoin.groupBy("prediction")
      .agg(sum("coloc"))
      .withColumnRenamed("sum(coloc)", "totalCluster")
      .cache()
    //    dfTotalClusters.printSchema()
    //dfTotalClusters.show(20)

    val rows = list_feature.map { feature =>

      //DF: cluster number | feature | total of colocations by cluster and feature
      val dfFeatures = dfJoin.groupBy("prediction", s"$featureName")
        .agg(sum("coloc"))
        .withColumnRenamed("sum(coloc)", "totalFeatureCluster")
      //      dfFeatures.printSchema()
      //dfFeatures.show(20)


      val dfDefinitivo = dfTotalClusters.join(dfFeatures, "prediction")
        .withColumn("abs", col("totalFeatureCluster") / col("totalCluster") * 100)
        .select("prediction", s"$featureName", "abs")
        .sort("prediction", s"$featureName")
        .repartition(col("prediction"))
        .cache()
      //dfDefinitivo.show(50)


      val columna = for (cluster <- 0 until numClusters) yield {

        val clusterRatio = try {
          dfDefinitivo.select("abs")
            .where(s"prediction == '$cluster'")
            .where(s"$featureName == '$feature'")
            .first()
            .getDouble(0)
        } catch {
          case ex: NoSuchElementException => {
            0.0
          }
        }

        (cluster, clusterRatio)
      }

      dfDefinitivo.unpersist()

      val spark = SparkSession.builder().getOrCreate()


      val schema = Array(
        StructField("prediction", IntegerType, true),
        StructField(s"$feature", DoubleType, true))

      val customSchema = StructType(schema)

      val columnaRDD = spark.sparkContext.parallelize(columna)
      val filas = columnaRDD.map(Row.fromTuple(_))

      logInfo(s"\tValue: $feature DONE!")


      spark.createDataFrame(filas, customSchema)

    }

    dfTotalClusters.unpersist()

    logInfo(s"$featureName finished!\n")
    rows.reduce(_.join(_, "prediction")).sort("prediction")
  }

  /**
    * Calculate and save the dataframe by cluster
    *
    * @param featureName Name of the column
    * @param dfFeatures  Full Dataframe that includes the cluster results and the features of the original dataset
    * @param dfJoin      Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def getResultsByCluster(featureName: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val list_feature = dfFeatures.select(featureName).distinct().rdd.map(r => r(0).toString).collect()
    val dfResult = calculateRatioByCluster(featureName, list_feature, dfJoin, numClusters)

    //dfResult.show(numClusters)

    dfResult

  }


  def getContingencies(df: DataFrame): (DataFrame, DataFrame) = {

    //begin by clusters//////////////////
    val dfCrossCluster = df.stat.crosstab("class", "prediction").sort("class_prediction")


    //gipsy fabs method
    var dfRenamed = dfCrossCluster.columns.foldLeft(dfCrossCluster)((curr, n) => curr.withColumnRenamed(n, n.replaceAll("\\.", "_")))
    val dfValues2 = dfCrossCluster.drop("class_prediction")

    val columnsModify2 = dfValues2.columns.map(col).map(colName => {
      val total = dfValues2.select(sum(colName)).first().get(0)
      (colName / total) * 100 as (s"${colName}")
    })

    val dfByClusters = dfRenamed.select(col("class_prediction") +: columnsModify2: _*)

    //End by clusters//////////////////


    //begin by class//////////////////
    val dfCrossClass = df.stat.crosstab("prediction", "class").sort("prediction_class")

    val dfValues = dfCrossClass.drop("prediction_class")

    val columnsModify = dfValues.columns.map(col).map(colName => {
      val total = dfValues.select(sum(colName)).first().get(0)
      (colName / total) * 100 as (s"${colName}")
    })


    val dfByColumns = dfCrossClass.select(col("prediction_class") +: columnsModify: _*)

    //End by columns//////////////////


    (dfByClusters, dfByColumns)

  }


  /**
    * Calculate and save the dataframe by cluster
    *
    * @param dfClusteringResult Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters        The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def getResultsByCluster(dfClusteringResult: DataFrame, numClusters: Int): DataFrame = {

    val list_feature = dfClusteringResult.select("class").distinct().rdd.map(r => r(0).toString).collect()
    val dfResult = calculateRatioByCluster("class", list_feature, dfClusteringResult, numClusters)

    //dfResult.show(numClusters)

    dfResult

  }

  /**
    * Calculate and save the dataframe by cluster
    *
    * @param featureName Name of the column
    * @param dfFeatures  Full Dataframe that includes the cluster results and the features of the original dataset
    * @param dfJoin      Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def saveResultsByCluster(featureName: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val momentum = Utils.whatTimeIsIt()

    val list_feature = dfFeatures.select(featureName).distinct().rdd.map(r => r(0).toString).collect()
    //val dfFeatureResult = calculateColocationsRatioByCluster(featureName, list_feature, dfJoin, numClusters)
    val dfFeatureResult = calculateRatioByCluster(featureName, list_feature, dfJoin, numClusters)


    logInfo("Saving results into a file..")
    //dfFeatureResult.show(numClusters)

    dfFeatureResult.repartition(1).write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"$momentum C$numClusters $featureName byCluster")

    logInfo(s"Saved data into folder: $momentum C$numClusters $featureName byCluster")
    dfFeatureResult
  }


  def saveColocationsResultsByCluster(featureName: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val momentum = Utils.whatTimeIsIt()

    val list_feature = dfFeatures.select(featureName).distinct().rdd.map(r => r(0).toString).collect()
    val dfFeatureResult = calculateColocationsRatioByCluster(featureName, list_feature, dfJoin, numClusters)


    logInfo("Saving results into a file..")
    //dfFeatureResult.show(numClusters)

    dfFeatureResult.repartition(1).write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"$momentum C$numClusters $featureName byCluster")

    logInfo(s"Saved data into folder: $momentum C$numClusters $featureName byCluster")
    dfFeatureResult
  }

  /**
    * Calculate and save the dataframe by cluster
    *
    * @param featureNameList List with the names of the columns
    * @param dfFeatures      Full Dataframe that includes the cluster results and the features of the original dataset
    * @param dfJoin          Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters     The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def saveResultsByCluster(featureNameList: List[String], dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): Unit = {

    featureNameList.map(saveResultsByCluster(_, dfFeatures, dfJoin, numClusters))

  }

  /**
    * Returns a dataframe with the relative frequency by feature
    *
    * @param featureName  Name of the column
    * @param list_feature List of the names of the distinct features of a column
    * @param dfJoin       Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters  The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def calculateRatioByFeature(featureName: String, list_feature: Array[String], dfJoin: DataFrame, numClusters: Int): DataFrame = {

    logInfo(s"Processing ratio by feature: $featureName..")

    val dfTotalClusters = dfJoin.groupBy(s"$featureName")
      .count()
      .withColumnRenamed("count", "totalFeature")
      .cache()

    val rows = list_feature.map {
      feature =>

        val dfFeatures = dfJoin.groupBy("prediction", s"$featureName")
          .count()
          .withColumnRenamed("count", "totalFeatureCluster")

        val dfDefinitivo = dfTotalClusters.join(dfFeatures, s"$featureName")
          .withColumn("abs", col("totalFeatureCluster") / col("totalFeature") * 100)
          .select("prediction", s"$featureName", "abs")
          .sort("prediction", s"$featureName")
          .repartition(col("prediction"))
          .cache()
        //dfDefinitivo.show(20)


        val columna = for (cluster <- 0 until numClusters) yield {

          val featureRatio = try {
            dfDefinitivo.select("abs")
              .where(s"prediction == '$cluster'")
              .where(s"$featureName == '$feature'")
              .first()
              .getDouble(0)
          } catch {
            case ex: NoSuchElementException => {
              0.0
            }
          }

          (cluster, featureRatio)
        }

        dfDefinitivo.unpersist()

        val spark = SparkSession.builder().getOrCreate()


        val schema = Array(
          StructField("prediction", IntegerType, true),
          StructField(s"$feature", DoubleType, true))

        val customSchema = StructType(schema)

        val columnaRDD = spark.sparkContext.parallelize(columna)
        val filas = columnaRDD.map(Row.fromTuple(_))

        logInfo(s"\tValue: $feature DONE!")


        spark.createDataFrame(filas, customSchema)

    }

    dfTotalClusters.unpersist()

    logInfo(s"$featureName finished!\n")
    rows.reduce(_.join(_, "prediction")).sort("prediction")
  }

  /**
    * Returns a dataframe with the relative frequency by feature
    *
    * @param featureName  Name of the column
    * @param list_feature List of the names of the distinct features of a column
    * @param dfJoin       Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters  The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def calculateColocationsRatioByFeature(featureName: String, list_feature: Array[String], dfJoin: DataFrame, numClusters: Int): DataFrame = {

    logInfo(s"Processing ratio by feature: $featureName..")

    val dfTotalClusters = dfJoin
      .groupBy(s"$featureName")
      .agg(sum("coloc"))
      .withColumnRenamed("sum(coloc)", "totalFeature")
      .cache()

    val rows = list_feature.map {
      feature =>

        val dfFeatures = dfJoin.groupBy("prediction", s"$featureName")
          .agg(sum("coloc"))
          .withColumnRenamed("sum(coloc)", "totalFeatureCluster")

        val dfDefinitivo = dfTotalClusters.join(dfFeatures, s"$featureName")
          .withColumn("abs", col("totalFeatureCluster") / col("totalFeature") * 100)
          .select("prediction", s"$featureName", "abs")
          .sort("prediction", s"$featureName")
          .repartition(col("prediction"))
          .cache()
        //dfDefinitivo.show(20)


        val columna = for (cluster <- 0 until numClusters) yield {

          val featureRatio = try {
            dfDefinitivo.select("abs")
              .where(s"prediction == '$cluster'")
              .where(s"$featureName == '$feature'")
              .first()
              .getDouble(0)
          } catch {
            case ex: NoSuchElementException => {
              0.0
            }
          }

          (cluster, featureRatio)
        }

        dfDefinitivo.unpersist()

        val spark = SparkSession.builder().getOrCreate()


        val schema = Array(
          StructField("prediction", IntegerType, true),
          StructField(s"$feature", DoubleType, true))

        val customSchema = StructType(schema)

        val columnaRDD = spark.sparkContext.parallelize(columna)
        val filas = columnaRDD.map(Row.fromTuple(_))

        logInfo(s"\tValue: $feature DONE!")


        spark.createDataFrame(filas, customSchema)

    }

    dfTotalClusters.unpersist()

    logInfo(s"$featureName finished!\n")
    rows.reduce(_.join(_, "prediction")).sort("prediction")
  }


  /**
    * Return the dataframe by cluster
    *
    * @param featureName Name of the column
    * @param dfFeatures  Full Dataframe that includes the cluster results and the features of the original dataset
    * @param dfJoin      Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def getResultsByFeature(featureName: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val list_feature = dfFeatures.select(featureName).distinct().rdd.map(r => r(0).toString).collect()
    val dfResult = calculateRatioByFeature(featureName, list_feature, dfJoin, numClusters)

    //dfResult.show(numClusters)

    dfResult

  }

  /**
    * Return the dataframe by cluster
    *
    * @param numClusters The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def getResultsByFeature(dfClusteringResult: DataFrame, numClusters: Int): DataFrame = {

    val list_feature = dfClusteringResult.select("class").distinct().rdd.map(r => r(0).toString).collect()
    val dfResult = calculateRatioByFeature("class", list_feature, dfClusteringResult, numClusters)

    //dfResult.show(numClusters)

    dfResult

  }


  /**
    * Calculate and save the dataframe by cluster
    *
    * @param featureName Name of the column
    * @param dfFeatures  Full Dataframe that includes the cluster results and the features of the original dataset
    * @param dfJoin      Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def saveResultsByFeature(featureName: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val momentum = Utils.whatTimeIsIt()

    val list_feature = dfFeatures.select(featureName).distinct().rdd.map(r => r(0).toString).collect()
    //val dfFeatureResult = calculateColocationsRatioByFeature(featureName, list_feature, dfJoin, numClusters)
    val dfFeatureResult = calculateRatioByFeature(featureName, list_feature, dfJoin, numClusters)


    logInfo("Saving results into a file..")
    //dfFeatureResult.show(numClusters)

    val fileName = s"$momentum C$numClusters $featureName byFeature"
    dfFeatureResult.repartition(1).write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(fileName)

    logInfo(s"Saved data into folder: $fileName")
    dfFeatureResult
  }

  def saveColocationsResultsByFeature(featureName: String, dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): DataFrame = {

    val momentum = Utils.whatTimeIsIt()

    val list_feature = dfFeatures.select(featureName).distinct().rdd.map(r => r(0).toString).collect()
    val dfFeatureResult = calculateColocationsRatioByFeature(featureName, list_feature, dfJoin, numClusters)


    logInfo("Saving results into a file..")
    //dfFeatureResult.show(numClusters)

    val fileName = s"$momentum C$numClusters $featureName byFeature"
    dfFeatureResult.repartition(1).write
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(fileName)

    logInfo(s"Saved data into folder: $fileName")
    dfFeatureResult
  }

  /**
    * Calculate and save the dataframe by cluster
    *
    * @param featureNameList List with the names of the columns
    * @param dfFeatures      Full Dataframe that includes the cluster results and the features of the original dataset
    * @param dfJoin          Full Dataframe that includes the cluster results and the features of the original dataset
    * @param numClusters     The number of clusters
    * @return The total number of cells that exceed the threshold
    * @example getRatio(list_cities, dfResults, 6, 20)
    */
  def saveResultsByFeature(featureNameList: List[String], dfFeatures: DataFrame, dfJoin: DataFrame, numClusters: Int): Unit = {

    featureNameList.map(saveResultsByFeature(_, dfFeatures, dfJoin, numClusters))

  }

  //
  //  def getReport(dfOrigen: DataFrame, dfDestino: DataFrame): Dataset[String] = {
  //
  //    val spark = dfOrigen.sparkSession
  //
  //    val bcOrigenColums = spark.sparkContext.broadcast(dfOrigen.columns)
  //
  //    val origen = dfOrigen.map {
  //      thisRow =>
  //
  //        getMaxOfRows(thisRow, bcOrigenColums.value)
  //    }
  //
  //    //    origen.show(5)
  //
  //    val bcDestinoColums = spark.sparkContext.broadcast(dfDestino.columns)
  //
  //    val destino = dfDestino.map(x2 =>
  //      getMaxOfRows(x2, bcDestinoColums.value)
  //    )
  //
  //    //    destino.show(5)
  //
  //    val dfFullText = origen.join(destino, "_1")
  //
  //    //    dfFullText.show(100)
  //
  //    val fullText = dfFullText.map {
  //      thisRow =>
  //
  //        var stringAux = ""
  //
  //        for (i <- 0 until thisRow.size) {
  //          stringAux += thisRow.get(i) + "\t"
  //        }
  //        stringAux
  //    }
  //
  //    fullText.rdd
  //      .repartition(1)
  //      .saveAsTextFile("Report-" + Utils.whatTimeIsIt())
  //
  //    fullText
  //
  //  }


  def getMaxOfRows(thisRow: Row, nameColums: Array[String]): (Int, String, Double, String, Double, String, Double, String, Double, String, Double, String, Double) = {

    val cluster = thisRow.getInt(0)
    var uno = new MaxRow
    var dos = new MaxRow
    var tres = new MaxRow
    var cuatro = new MaxRow
    var cinco = new MaxRow
    var seis = new MaxRow

    for (i <- 1 until thisRow.size) {
      val aux = new MaxRow(nameColums(i), thisRow.getDouble(i))
      if (aux.value >= uno.value) {
        seis = cinco
        cinco = cuatro
        cuatro = tres
        tres = dos
        dos = uno
        uno = aux
      } else if (aux.value >= dos.value) {
        seis = cinco
        cinco = cuatro
        cuatro = tres
        tres = dos
        dos = aux
      } else if (aux.value >= tres.value) {
        seis = cinco
        cinco = cuatro
        cuatro = tres
        tres = aux
      } else if (aux.value >= cuatro.value) {
        seis = cinco
        cinco = cuatro
        cuatro = aux
      } else if (aux.value >= cinco.value) {
        seis = cinco
        cinco = aux
      } else if (aux.value >= seis.value) {
        seis = aux
      }

    }

    (cluster, uno.name, uno.value, dos.name, dos.value, tres.name, tres.value, cuatro.name, cuatro.value, cinco.name, cinco.value, seis.name, seis.value)

  }


}