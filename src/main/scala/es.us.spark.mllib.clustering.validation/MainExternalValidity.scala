package es.us.spark.mllib.clustering.validation

import es.us.spark.mllib.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Josem on 26/09/2017.
  */
object MainExternalValidity {

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


    val minClusters = 2
    val maxClusters = 10
    val origen = columnFile
    val destino: String = Utils.whatTimeIsIt()
    var idIndex = "-1"
    val classIndex = 6
    val delimiter = " "

    val modality = RIQUELME

    val argumentsList = List(Array[String](minClusters.toString, maxClusters.toString, columnFile, destino + "-columnFile", "6", " ", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, habermanFile, destino + "-habermanFile", "3", ",", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, irisFile, destino + "-irisFile", "4", ",", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, seedFile, destino + "-seedFile", "7", "\t", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, vehiclesFile, destino + "-vehiclesFile", "18", "\t", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, wineFile, destino + "-wineFile", "0", ",", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, yeastFile, destino + "-yeastFile", "9", "\t", "0"),
      Array[String](minClusters.toString, maxClusters.toString, data_UserFile, destino + "-data_UserFile", "5", ";", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, waveformFile, destino + "-waveformFile", "21", ",", idIndex),
      Array[String](minClusters.toString, maxClusters.toString, relaxFile, destino + "-relaxFile", "12", "\t", idIndex))

    //val arguments = Array[String](minClusters.toString, maxClusters.toString, origen, destino, classIndex.toString, delimiter)
    argumentsList.foreach {
      modality match {
        case RIQUELME =>
          MainRiquelme.main(_)

        case LITERATURE =>
          MainLiterature.main(_)

        case ALL =>
          MainRiquelme.main(_)
          MainLiterature.main(_)

        case _ => throw new IllegalArgumentException
      }
    }


  }

}
