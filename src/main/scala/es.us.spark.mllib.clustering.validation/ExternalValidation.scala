package es.us.spark.mllib.clustering.validation

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * Created by Josem on 27/09/2017.
  */
object ExternalValidation extends Logging {

  /**
    * Returns the entropy of a clustering results given the clustering result and the number of clusters
    *
    * @param dfClusteringResult Clustering Result
    * @example getEntropy(kmeansResults)
    */
  def getEntropy(dfClusteringResult: DataFrame): Double = {


    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements)

    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val rowEntropySeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow
        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) cellValue * log2(cellValue) else 0.0
      }

      val rowEntropy = -rowEntropySeq.sum

      (totalRow / bcTotalElements.value) * rowEntropy

    }.reduce(_ + _)

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

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements)

    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val rowPuritySeq = for (i <- 0 until row.size) yield {
        row.getDouble(i) / totalRow
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

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)


    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val mutualInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow
        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) cellValue * math.log(cellValue / ((totalRow / bcTotalElements.value) * (bcColumnsSum.value.apply(i) / bcTotalElements.value))) else 0.0
      }

      mutualInformationSeq.sum

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

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)


    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val mutualInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

        (2 * (cellValue / totalRow) * (cellValue / bcColumnsSum.value.apply(i))) / ((cellValue / totalRow) + (cellValue / bcColumnsSum.value.apply(i)))
      }

      mutualInformationSeq.max

    }.reduce(_ + _)

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

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)


    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      (totalRow / bcTotalElements.value) * math.log(totalRow / bcTotalElements.value)

    }.reduce(_ + _)

    val secondValueSeq = for (i <- dfClusteringResult.columns.indices) yield {

      (bcColumnsSum.value.apply(i) / bcTotalElements.value) * math.log(bcColumnsSum.value.apply(i) / bcTotalElements.value)

    }
    val secondValue = secondValueSeq.sum

    val thirdValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow
        //If cellValue is zero log(0) is set to zero
        if (cellValue != 0) cellValue * math.log(cellValue / ((totalRow / bcTotalElements.value) * (bcColumnsSum.value.apply(i) / bcTotalElements.value))) else 0.0
      }

      variationOfInformationSeq.sum

    }.reduce(_ + _)

    -firstValue - secondValue - 2 * thirdValue

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

    val totalElements = getTotalElements(dfClusteringResult)
    val bcTotalElements = spark.sparkContext.broadcast(totalElements)

    dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val rowPuritySeq = for (i <- 0 until row.size) yield {
        row.getDouble(i) / totalRow
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
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

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

    val totalElements = getTotalElements(dfClusteringResult)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

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

    (firstValue - (secondValue * thirdValue) / combina2(totalElements)) / ((1 / 2) * (secondValue + thirdValue) - (secondValue * thirdValue) / combina2(totalElements))


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

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

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

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

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

    val totalElements = getTotalElements(dfClusteringResult)

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

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

    val columnsSum = getSumColumns(dfClusteringResult)
    val bcColumnsSum = spark.sparkContext.broadcast(columnsSum)

    val firstValue = dfClusteringResult.map { row =>
      val totalRow = getSumRow(row)

      val variationOfInformationSeq = for (i <- 0 until row.size) yield {
        val cellValue = row.getDouble(i) / totalRow

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

    math.sqrt(secondValue + thirdValue - 2 * firstValue) / math.sqrt(thirdValue)

  }


  /**
    * Returns the sum of all the values of a row
    *
    * @param row The Row which values are going to be sum
    * @example getRowSum(rowExample)
    */
  def getSumRow(row: Row): Double = {

    val a = for (i <- 0 until row.size) yield {
      row.getDouble(i)
    }

    a.sum
  }

  /**
    * Returns an array with the sum of all the elements of each column
    *
    * @param data The Row which values are going to be summed
    * @example getSumColumns(rowExample)
    */
  def getSumColumns(data: DataFrame): Array[Double] = {

    val columnNames = data.columns

    columnNames.map { colName =>
      data.select(sum(colName)).first().getDouble(0)
    }

  }

  /**
    * Returns the max of all the values of a row
    *
    * @param row The Row which values are going to be summed
    * @example getMaxRow(rowExample)
    */
  def getMaxRow(row: Row): Double = {

    val a = for (i <- 0 until row.size) yield {
      row.getDouble(i)
    }

    a.sum
  }


  /**
    * Returns the sum of all the values of the dataframe
    *
    * @param data Clustering Result RDD with
    * @example getTotalElements(dfExample)
    */
  def getTotalElements(data: DataFrame): Double = {
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
  def combina2(number: Double): Double = {
    (number * (number - 1)) / 2
  }


}
