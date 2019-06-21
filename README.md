# External Clustering Validity Indices
This package contains the code for executing 15 external clustering validity indices in Spark. The package includes the following indices:
* **Chi Index**
* Entropy
* Purity
* Mutual-Information
* F-Measure
* Variation Of Information
* Goodman-Kruskal
* Rand Index
* Adjusted Rand Index
* Jaccard
* Fowlkes-Mallows
* Hubert
* Minkowski
* Criterion H
* CSI
* PSI

Chi Index takes a value in [0, 2], where 0 is given by the worst clustering solution, and 2 is the best value that Chi Index can achieve. 

The cluster indices can be executed using K-means and Bisecting K-Means from Spark MLlib, and Linkage method.

Please, cite as: Luna-Romera JM, Martínez-Ballesteros M, García-Gutiérrez J, Riquelme JC. External clustering validity index based on chi-squared statistical test. Information Sciences (2019) 487: 1-17. https://doi.org/10.1016/j.ins.2019.02.046. (http://www.sciencedirect.com/science/article/pii/S0020025519301550)

## Getting Started

### Prerequisites

The package is ready to be used. You only have to download it and import it into your workspace. The package includes iris dataset as example and it can be used with the main test class.

## Running the tests
MainExternalTest class has been configured for being executed in a laptop. You can execute this class directly but there are some variables that you should be noticed:
* val numIterations = Maximum number of iterations for the clustering algorithm.
* var minClusters = Minimum number of clusters to begin the execution.
* var maxClusters = Maximum number of clusters to finish the execution.
* val origen = The path of the dataset.
* val destino: String = Utils.whatTimeIsIt() This is the name of the output file.
* var idIndex = The index of the instances' ID. If there is no id, set it as "-1".
* val classIndex = The index of the class, 0 is the first element.
* val delimiter = The delimiter between the columns.

```
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
```

This application can be used with KMeans, GaussianMixture, LDA and BisectingKMeans from SPARK ML. Just comment the methods that are not going to be used:

```
  val clusteringResult = new KMeans()
  //val clusteringResult = new GaussianMixture()
  //val clusteringResult = new LDA()
  //val clusteringResult = new BisectingKMeans()
    .setK(numClusters)
    .setSeed(1L)
    .setMaxIter(numIterations)
    .setFeaturesCol("features")
```

## Results
By default, the results are saved in the same a folder than the dataset. The results are saved in several folders:
* [DATETIME]-[DATESETNAME]-kmeansRes-[K]Results: It's the folder that contains the results of the clustering result as "class	prediction".
* [DATETIME]-[DATESETNAME]-[K]-class-DFClusters: This folder contains the results of the contingency table with the relative values that sums 100% by rows.
* [DATETIME]-[DATESETNAME]-[K]-class-DFFeatures: This folder contains the results of the contingency table with the relative values that sums 100% by columns.
* [DATETIME]-[DATESETNAME]-loopingchi: This folder contains the results of the chi values for each K in 3 columns: Chi-squared value by row; Chi-squared value by column; and the Chi Index value.

This data can be copy-pasted directly into an spreadsheet to be visualized.

For the iris dataset, the results are following:
![Chi index results for Iris for k=[2,10]](https://github.com/josemarialuna/ExternalValidity/blob/master/ChiRes.PNG)

![Contingency tables for k=3](https://github.com/josemarialuna/ExternalValidity/blob/master/Chitables.PNG)




## Contributors

* José María Luna-Romera - (main contributor and maintainer).
* María Martínez-Ballesteros
* Jorge García-Gutiérrez
* José C. Riquelme Santos

## References

[1] Luna-Romera JM, Martínez-Ballesteros M, García-Gutiérrez J, Riquelme JC. External clustering validity index based on chi-squared statistical test. Information Sciences (2019) 487: 1-17. https://doi.org/10.1016/j.ins.2019.02.046. (http://www.sciencedirect.com/science/article/pii/S0020025519301550)

