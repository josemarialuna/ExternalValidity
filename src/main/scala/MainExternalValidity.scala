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
    val column3CFile = "C:\\datasets\\Validation\\vertebral_column_data\\column_3C.dat"
    val column2CFile = "C:\\datasets\\Validation\\vertebral_column_data\\column_2C.dat"
    val habermanFile = "C:\\datasets\\Validation\\haberman.data"
    val seedFile = "C:\\datasets\\Validation\\seeds_dataset.txt"
    val vehiclesFile = "C:\\datasets\\Validation\\vehicles.dat"
    val data_UserFile = "C:\\datasets\\Validation\\Data_User.csv"
    val waveformFile = "C:\\datasets\\Validation\\waveform.data"
    val relaxFile = "C:\\datasets\\Validation\\planing_relax.txt"
    val zooFile = "C:\\datasets\\Validation\\zoo.data"
    val wineQualityWhite = "C:\\datasets\\Validation\\winequality-white.csv"
    val wineQualityRed = "C:\\datasets\\Validation\\winequality-red.csv"
    val biodegFile = "C:\\datasets\\Validation\\biodeg.csv"
    val breastTissueFile = "C:\\datasets\\Validation\\BreastTissue.csv"
    val cloudFile = "C:\\datasets\\Validation\\cloud.data"
    val diabetesFile = "C:\\datasets\\Validation\\diabetes.arff"
    val ecoliFile = "C:\\datasets\\Validation\\ecoli.data"
    val faultsFile = "C:\\datasets\\Validation\\Faults.data"
    val airlinesFile = "C:\\datasets\\Validation\\datasets_arff\\airlinesnorm.arff"
    val bankmarketingFile = "C:\\datasets\\Validation\\datasets_arff\\bankmarketingnorm.arff"
    val carFile = "C:\\datasets\\Validation\\datasets_arff\\carnorm.arff"
    val electricityFile = "C:\\datasets\\Validation\\datasets_arff\\electricity.arff"
    val kddFile = "C:\\datasets\\Validation\\datasets_arff\\kddcup99norm.arff"
    val ozoneFile = "C:\\datasets\\Validation\\datasets_arff\\ozone.arff"
    val pendigitsFile = "C:\\datasets\\Validation\\datasets_arff\\pendigits.arff"
    val pokerhandFile = "C:\\datasets\\Validation\\datasets_arff\\pokerhand.arff"
    val spambaseFile = "C:\\datasets\\Validation\\datasets_arff\\spambase.arff"
    val wholesaleFile = "C:\\datasets\\Validation\\wholesale.csv"
    val vowelFile = "C:\\datasets\\Validation\\vowel.csv"
    val urbanFile = "C:\\datasets\\Validation\\urban.csv"
    val spamFile = "C:\\datasets\\Validation\\spambase.data.txt"
    val spectometerFile = "C:\\datasets\\Validation\\spectrometer.txt"
    val segmentFile = "C:\\datasets\\Validation\\segment.dat"
    val satimageFile = "C:\\datasets\\Validation\\sat.txt"
    val optdigits = "C:\\datasets\\Validation\\optdigits.tra"
    val movementFile = "C:\\datasets\\Validation\\movement_libras.data.txt"
    val letterFile = "C:\\datasets\\Validation\\letter-recognition.data"
    val leafFile = "C:\\datasets\\Validation\\leaf.csv"
    val glassFile = "C:\\datasets\\Validation\\glass.data"
    val forestFile = "C:\\datasets\\Validation\\forest.csv"
    val gestureFile = "C:\\datasets\\Validation\\gesture.csv"


    val susyFile = "C:\\datasets\\Validation\\SUSY.csv"

    val synthetic = "C:\\datasets\\Validation\\Synthetics\\C3-D5-I1000"
    val susy_limited = "C:\\Users\\Josem\\Documents\\ExternalValidity\\SUSY_limited2"

    val emoticonsFile = "C:\\datasets\\Validation\\multilabel\\emotions\\emotions.dat"
    val sceneFile = "C:\\datasets\\Validation\\multilabel\\scene\\scene.dat"


    val recordFile = "C:\\datasets\\Validation\\caepia\\RecordLinkage\\block_1\\block_1.csv"
    val heteroFile = "C:\\datasets\\Validation\\caepia\\RecordLinkage\\block_1\\block_1.csv"
    val hepFile = "C:\\datasets\\Validation\\caepia\\RecordLinkage\\block_1\\block_1.csv"
    val gasFile = "C:\\datasets\\Validation\\caepia\\RecordLinkage\\block_1\\block_1.csv"

    val empleo1 = "C:\\Users\\Josem\\Dropbox\\PHD\\Proyectos\\2017-08 - Clustering Trabajadores BACK\\New Dataset por Provincias\\2014-16\\matrizRelativaPor1"

    val minClusters = 2
    val maxClusters = 10
    val origen = empleo1
    val destino: String = Utils.whatTimeIsIt()
    var idIndex = "-1"
    val classIndex = 0
    val delimiter = " "

    val modality = LITERATURE
//
//    val argumentsList = List(
//      Array[String](minClusters.toString, maxClusters.toString, recordFile, destino + "-recordFile", "7", ";", idIndex),
//      Array[String]("10", "20", heteroFile, destino + "-heteroFile", "17", ",", idIndex),
//      Array[String](minClusters.toString, maxClusters.toString, hepFile, destino + "-hepFile", "17", ",", idIndex),
//      Array[String](minClusters.toString, maxClusters.toString, gasFile, destino + "-gasFile", "17", ",", idIndex)
//    )

        val argumentsList = List(
//          Array[String](minClusters.toString, maxClusters.toString, zooFile, destino + "-zooFile", "17", ",", "0"),
//          Array[String](minClusters.toString, maxClusters.toString, bankNoteFile, destino + "-bankNoteFile", "4", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, breastFile, destino + "-breastFile", "10", ",", "0"),
//          Array[String](minClusters.toString, maxClusters.toString, column3CFile, destino + "-column3CFile", "6", " ", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, column2CFile, destino + "-column2CFile", "6", " ", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, habermanFile, destino + "-habermanFile", "3", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, irisFile, destino + "-irisFile", "4", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, seedFile, destino + "-seedFile", "7", "\t", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, vehiclesFile, destino + "-vehiclesFile", "18", "\t", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, wineFile, destino + "-wineFile", "0", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, yeastFile, destino + "-yeastFile", "9", "\t", "0"),
//          Array[String](minClusters.toString, maxClusters.toString, data_UserFile, destino + "-data_UserFile", "5", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, waveformFile, destino + "-waveformFile", "21", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, relaxFile, destino + "-relaxFile", "12", "\t", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, wineQualityWhite, destino + "-wineQualityWhite", "11", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, wineQualityRed, destino + "-wineQualityRed", "11", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, biodegFile, destino + "-biodegFile", "41", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, breastTissueFile, destino + "-breastTissueFile", "0", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, cloudFile, destino + "-cloudFile", "0", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, diabetesFile, destino + "-diabetesFile", "19", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, ecoliFile, destino + "-ecoliFile", "8", ";", "0"),
//          Array[String](minClusters.toString, maxClusters.toString, faultsFile, destino + "-faultsFile", "11", ";", idIndex),
//          Array[String]("2", "4", airlinesFile, destino + "-airlinesFile", "7", ",", idIndex),
//          Array[String]("2", "4", bankmarketingFile, destino + "-bankmarketingFile", "16", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, carFile, destino + "-carFile", "1", ",", idIndex),
//          Array[String]("2", "4", electricityFile, destino + "-electricityFile", "8", ",", idIndex),
//          //Array[String](minClusters.toString, maxClusters.toString, kddFile, destino + "-kddFile", "0", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, ozoneFile, destino + "-ozoneFile", "72", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, pendigitsFile, destino + "-pendigitsgFile", "16", ",", idIndex),
////          Array[String](minClusters.toString, maxClusters.toString, pokerhandFile, destino + "-pokerhandFile", "10", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, spambaseFile, destino + "-spambaseFile", "57", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, wholesaleFile, destino + "-wholesaleFile", "0", ",", idIndex),
//          Array[String]("10", "20", vowelFile, destino + "-vowelFile", "13", ";", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, urbanFile, destino + "-urbanFile", "0", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, spamFile, destino + "-spamFile", "57", ",", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, spectometerFile, destino + "-spectometerFile", "1", "\t", "0"),
//          Array[String](minClusters.toString, maxClusters.toString, segmentFile, destino + "-segmentFile", "19", " ", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, satimageFile, destino + "-satimageFile", "36", " ", idIndex),
//          Array[String](minClusters.toString, maxClusters.toString, optdigits, destino + "-optdigits", "64", ",", idIndex),
//          Array[String]("10", "20", movementFile, destino + "-movementFile", "90", ",", idIndex),
          Array[String]("20", "30", letterFile, destino + "-letterFile", "0", ",", idIndex),
          Array[String]("30", "40", leafFile, destino + "-leafFile", "0", ",", idIndex),
          Array[String](minClusters.toString, maxClusters.toString, gestureFile, destino + "-gestureFile", "19", ",", "0"),
          Array[String](minClusters.toString, maxClusters.toString, glassFile, destino + "-glassFile", "10", ",", "0"),
          Array[String](minClusters.toString, maxClusters.toString, forestFile, destino + "-forestFile", "0", ",", idIndex)
        )
//            val argumentsList = List(
//              Array[String]("12", "15", wineQuality, destino + "-winequality", "11", ";", idIndex))

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
