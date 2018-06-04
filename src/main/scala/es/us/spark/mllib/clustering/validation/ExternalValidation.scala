package es.us.spark.mllib.clustering.validation

import java.util.NoSuchElementException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ExternalValidation(_entropy: Double, _purity: Double, _mutualInformation: Double, _fMeasure: Double, _variationOfInformation: Double, _goodmanKruskal: Double, _randIndex: Double, _adjustedRandIndex: Double, _jaccard: Double, _fowlkesMallows: Double, _hubert: Double, _minkoski: Double) extends Serializable {
  def entropy: Double = _entropy

  def purity: Double = _purity

  def mutualInformation: Double = _mutualInformation

  def fMeasure: Double = _fMeasure

  def variationOfInformation: Double = _variationOfInformation

  def goodmanKruskal: Double = _goodmanKruskal

  def randIndex: Double = _randIndex

  def adjustedRandIndex: Double = _adjustedRandIndex

  def jaccard: Double = _jaccard

  def fowlkesMallows: Double = _fowlkesMallows

  def hubert: Double = _hubert

  def minkoski: Double = _minkoski


  override def toString: String = s"$entropy\t$purity\t$mutualInformation\t$fMeasure\t$variationOfInformation\t$goodmanKruskal\t$randIndex\t$adjustedRandIndex\t$jaccard\t$fowlkesMallows\t$hubert\t$minkoski"
}

/**
  * Created by Josem on 27/09/2017.
  */
object ExternalValidation extends Logging {

  /**
    * Calculate all the external validation indices for the given dataframe and returns an ExternalValidation object with its values
    *
    * @param dfClusteringResult Clustering Result
    * @example calculateExternalIndices(kmeansResults)
    */
  def calculateExternalIndices(dfClusteringResult: DataFrame): ExternalValidation = {
    new ExternalValidation(getEntropy(dfClusteringResult),
      getPurity(dfClusteringResult),
      getMutualInformation(dfClusteringResult),
      getFMeasure(dfClusteringResult),
      getVariationOfInformation(dfClusteringResult),
      getGoodmanKruskal(dfClusteringResult),
      getRandIndex(dfClusteringResult),
      getAdjustedRandIndex(dfClusteringResult),
      getJaccard(dfClusteringResult),
      getFowlkesMallows(dfClusteringResult),
      getHubert(dfClusteringResult),
      getMinkowski(dfClusteringResult))
  }


  /**
    * Returns the entropy of a clustering results given the clustering result and the number of clusters
    *
    * @param dfClusteringResult Clustering Result
    * @example getEntropy(kmeansResults)
    */
  def getEntropy(dfClusteringResult: DataFrame): Double = {


    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Entropy")

    //    dfClusteringResult.printSchema()
    //    dfClusteringResult.show()

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements.doubleValue())

    val entropy = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)
      val pi = totalRow.doubleValue() / bcTotalElements.value

      val rowEntropySeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i) / bcTotalElements.value.doubleValue()

        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) (cellValue / pi) * log2(cellValue / pi) else 0.0
      }

      val rowEntropy = -rowEntropySeq.sum

      pi * rowEntropy

    }.reduce(_ + _)

    -entropy

  }

  /**
    * Returns the purity of a clustering results given the clustering result and the number of clusters
    *
    * @param dfClusteringResult Clustering Result DataFrame
    * @example getPurity(kmeansResults)
    */
  def getPurity(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Purity")

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements.doubleValue())

    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val rowPuritySeq = for (i <- 0 until row.size) yield {
        row.getLong(i) / totalRow.doubleValue()
      }

      val rowPurity = rowPuritySeq.max

      (totalRow / bcTotalElements.value) * rowPurity

    }.reduce(_ + _)

  }

  /**
    * Returns the mutual information coeficient of a clustering results given the clustering result and the number of clusters
    *
    * @param dfClusteringResult Clustering Result Dataframe
    * @example getMutualInformation(kmeansResults)
    */
  def getMutualInformation(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Mutual Information")

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements.doubleValue())

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i) / bcTotalElements.value
        val pi = totalRow / bcTotalElements.value
        val pj = bcColumnsSum.value.apply(i) / bcTotalElements.value

        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) cellValue * log2(cellValue / (pi * pj)) else 0.0
      }

      variationOfInformationSeq.sum

    }.reduce(_ + _)

  }

  /**
    * Returns the F-Measure of a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getFMeasure(kmeansResults)
    */
  def getFMeasure(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating F-Measure")

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements.doubleValue())

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      val pj = bcColumnsSum.value.apply(i) / bcTotalElements.value
      val bc_pj = spark.sparkContext.broadcast(pj)

      val maxValue = dfClusteringResult.map { row =>
        val totalRow = getSumRow(row)
        val pi = totalRow / bcTotalElements.value
        val pij = row.getLong(i) / bcTotalElements.value

        //If cellValue is zero log(0) is set to zero
        if (pij != 0) 2 * ((pij / pi) * (pij / bc_pj.value)) / ((pij / pi) + (pij / bc_pj.value)) else 0.0
      }.collect().max

      pj * maxValue

    }
    val fmeasure = secondValueSeq.sum

    fmeasure
  }

  /**
    * Returns the Variation Of Information of a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getVariationOfInformation(kmeansResults)
    */
  def getVariationOfInformation(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Variation of Information")

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements.doubleValue())

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)


    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)
      val pi = totalRow / bcTotalElements.value

      pi * log2(pi)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      val pj = bcColumnsSum.value.apply(i) / bcTotalElements.value

      pj * log2(pj)

    }
    val secondValue = secondValueSeq.sum

    val thirdValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i) / bcTotalElements.value
        val pi = totalRow / bcTotalElements.value
        val pj = bcColumnsSum.value.apply(i) / bcTotalElements.value

        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) cellValue * log2(cellValue / (pi * pj)) else 0.0
      }

      variationOfInformationSeq.sum

    }.reduce(_ + _)

    -firstValue - secondValue - (2 * thirdValue)

  }

  /**
    * Returns the Goodman-Kruscal of a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getFMeasure(kmeansResults)
    */
  def getGoodmanKruskal(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Goodman-Kruskal")

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements.doubleValue())

    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row).doubleValue()

      val rowPuritySeq = for (i <- 0 until row.size) yield {
        row.getLong(i) / totalRow
      }

      val rowPurity = rowPuritySeq.max

      (totalRow / bcTotalElements.value) * (1 - rowPurity)

    }.reduce(_ + _)

  }

  /**
    * Returns the Rand Index of a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getRandIndex(kmeansResults)
    */
  def getRandIndex(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Rand Index")

    val totalElements = getTotalElements(dfClusteringResult)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)


    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      combina2(totalRow)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      combina2(bcColumnsSum.value.apply(i))

    }
    val secondValue = secondValueSeq.sum

    val thirdValue = dfClusteringResult.map { row =>

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i)

        combina2(cellValue)
      }

      variationOfInformationSeq.sum

    }.reduce(_ + _)

    (combina2(totalElements) - firstValue - secondValue + (2 * thirdValue)) / combina2(totalElements)

  }

  /**
    * Returns the Adjusted Rand Index from a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getAdjustedRandIndex(kmeansResults)
    */
  def getAdjustedRandIndex(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Adjusted Rand index")

    val totalElements = getTotalElements(dfClusteringResult)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i)
        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) combina2(cellValue) else 0.0
      }

      variationOfInformationSeq.sum

    }.reduce(_ + _)

    val secondValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      if (totalRow != 0) combina2(totalRow) else 0.0

    }.reduce(_ + _)

    val thirdValueSeq = for (i <- dfClusteringResult.columns.indices) yield {
      val pj = bcColumnsSum.value.apply(i)
      if (pj != 0) combina2(pj) else 0.0

    }
    val thirdValue = thirdValueSeq.sum
    val combina2Total = combina2(totalElements)
    //println(s"($firstValue - (($secondValue * $thirdValue) / $combina2Total)) / ((0.5) * (($secondValue + $thirdValue) - ($secondValue * $thirdValue)) / $combina2Total)")

    if (combina2Total != 0) (firstValue - ((secondValue * thirdValue) / combina2Total)) / (0.5 * (secondValue + thirdValue) - (secondValue * thirdValue) / combina2Total) else 0.0

  }

  /**
    * Returns Jaccard Index from a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getJaccard(kmeansResults)
    */
  def getJaccard(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Jaccard")

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>

      val jaccardSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i)

        combina2(cellValue)
      }

      jaccardSeq.sum

    }.reduce(_ + _)

    val secondValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      combina2(totalRow)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      combina2(bcColumnsSum.value.apply(i))

    }
    val thirdValue = secondValueSeq.sum

    firstValue / (secondValue + thirdValue - firstValue)

  }


  /**
    * Returns Fowlkes and Mallows Index from a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getFowlkesMallows(kmeansResults)
    */
  def getFowlkesMallows(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Fowlkes Mallows")

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>

      val fowlkesSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i)

        combina2(cellValue)
      }

      fowlkesSeq.sum

    }.reduce(_ + _)

    val secondValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      combina2(totalRow)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      combina2(bcColumnsSum.value.apply(i))

    }
    val thirdValue = secondValueSeq.sum

    firstValue / math.sqrt(secondValue * thirdValue)

  }

  /**
    * Returns Hubert Statistic Index from a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getHubert(kmeansResults)
    */
  def getHubert(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Hubert")

    val totalElements = getTotalElements(dfClusteringResult)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i)

        combina2(cellValue)
      }

      variationOfInformationSeq.sum

    }.reduce(_ + _)

    val secondValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      combina2(totalRow)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      combina2(bcColumnsSum.value.apply(i))

    }
    val thirdValue = secondValueSeq.sum

    (combina2(totalElements) * firstValue - (secondValue * thirdValue)) / math.sqrt(secondValue * thirdValue * (combina2(totalElements) - secondValue) * (combina2(totalElements) - thirdValue))

  }

  /**
    * Returns Minkowski score Index from a clustering results
    *
    * @param dfClusteringResult Clustering Result dataframe
    * @example getMinkowski(kmeansResults)
    */
  def getMinkowski(dfClusteringResult: DataFrame): Double = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    logInfo("Calculating Minkowski")

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>

      val minkowskiSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getLong(i)

        combina2(cellValue)
      }

      minkowskiSeq.sum

    }.reduce(_ + _)

    val secondValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      combina2(totalRow)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      combina2(bcColumnsSum.value.apply(i))

    }
    val thirdValue = secondValueSeq.sum

    math.sqrt(secondValue + thirdValue - 2 * firstValue) / math.sqrt(thirdValue)

  }


  /**
    * Returns the sum of all the values of a row
    *
    * @param row The Row which values are going to be sum
    * @example getRowSum(rowExample)
    */

  def getSumRow(row: Row): Long = {

    val a = for (i <- 0 until row.size) yield {
      row.getLong(i)
    }

    a.sum
  }

  /**
    * Returns an array with the sum of all the elements of each column
    *
    * @param data The Row which values are going to be summed
    * @example getSumColumns(rowExample)
    */
  def getSumColumns(data: DataFrame): Array[Long] = {

    val columnNames = data.columns

    columnNames.map { colName =>
      data.select(sum(cleanColumnName(colName))).first().getLong(0)
    }

  }

  // Back ticks can't exist in DataFrame column names, therefore drop them. To be able to accept
  // special keywords and `.`, wrap the column names in ``.
  def cleanColumnName(name: String): String = {
    s"`$name`"
  }

  /**
    * Returns the max of all the values of a row
    *
    * @param row The Row which values are going to be summed
    * @example getMaxRow(rowExample)
    */
  def getMaxRow(row: Row): Double = {

    val a = for (i <- 0 until row.size) yield {
      row.getLong(i)
    }

    a.sum
  }


  /**
    * Returns the sum of all the values of the dataframe
    *
    * @param data Clustering Result RDD with
    * @example getTotalElements(dfExample)
    */
  def getTotalElements(data: DataFrame): Long = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    data.map { row =>
      getSumRow(row)
    }.reduce(_ + _)

  }

  /**
    * Returns result of the log base 2 of the given number
    *
    * @param number Number to be log
    * @example log2(4)
    */
  def log2(number: Double): Double = {
    math.log10(number) / math.log10(2)
  }

  /**
    * Returns combinatorium base 2
    *
    * @param number Number to be combined
    * @example combina2(10)
    */
  def combina2(number: Long): Double = {
    (number * (number - 1)) / 2
  }

  /**
    * Returns contingency matrix
    *
    * @param dfClusteringResult Dataframe with the assigned cluster and the class to which belongs
    * @example getContingencyMatrix(kmeansResult)
    * @deprecated ("This method was replaced by DataFrame.stat.crosstab")
    */
  def getContingencyMatrix(dfClusteringResult: DataFrame, numClusters: Int): DataFrame = {
    val list_feature = dfClusteringResult.select("class")
      .distinct()
      .rdd.map(r => r(0).toString).collect()

    val dfTotal = dfClusteringResult.groupBy("prediction", "class")
      .count()
    val dfTotalClusters = dfTotal
      .withColumn("count", dfTotal("count").cast(LongType))
      .cache()

    //    println("dfTotalClusters")
    //    dfTotalClusters.show()

    val rows = list_feature.map { feature =>

      val columna = for (cluster <- 0 until numClusters) yield {

        val clusterRatio = try {
          dfTotalClusters.select("count")
            .where(s"prediction == '$cluster'")
            .where(s"class == '$feature'")
            .first()
            .getLong(0)
        } catch {
          case ex: NoSuchElementException =>
            0.0
        }

        (cluster, clusterRatio)
      }

      dfTotalClusters.unpersist()

      val spark = SparkSession.builder().getOrCreate()

      val schema = Array(
        StructField("prediction", IntegerType, nullable = true),
        StructField(s"$feature", LongType, nullable = true))

      val customSchema = StructType(schema)

      val columnaRDD = spark.sparkContext.parallelize(columna)
      val filas = columnaRDD.map(Row.fromTuple(_))

      logInfo(s"\tValue: $feature DONE!")

      spark.createDataFrame(filas, customSchema)

    }

    dfTotalClusters.unpersist()

    rows.reduce(_.join(_, "prediction")).sort("prediction")
  }


}


