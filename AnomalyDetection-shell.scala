/**
 * @author Keira Zhou
 * @date 12/21/15
 *
 * Training code
 * <p>
 * This anomaly detection code mainly follows the tutorial from Sean Owen, Cloudera.
 * Couple of modifcations have been made to fit personal interest:
 * 		- Instead of training multiple clusters, the code only trains on "normal" data points
 * 		- Only one cluster center is recorded and threshold is set to the last of 
 *		  the furthest 2000 data points
 *		- During later validating stage, all points that are further than the threshold
 *		  is labeled as "anomaly"
 *
 * Video: https://www.youtube.com/watch?v=TC5cKYBZAeI
 * Slides-1: http://www.slideshare.net/CIGTR/anomaly-detection-with-apache-spark
 * Slides-2: http://www.slideshare.net/CIGTR/anomaly-detection-with-apache-spark-2
 */ 


//************************************
//         Training
//************************************

// train on the "normal" data points
val rawData = sc.textFile("dataset/ad.train.csv", 120)
rawData.count


// parse data, only include numerical attributes
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val dataAndLabel = rawData.map { line =>
	val buffer = ArrayBuffer[String]()
	buffer.appendAll(line.split(","))
	buffer.remove(1, 3) // remove categorial attributes
	val label = buffer.remove(buffer.length-1)
	val vector = Vectors.dense(buffer.map(_.toDouble).toArray) 
	(vector, label)
}


// checkout how data look like
dataAndLabel.take(1)
dataAndLabel.map(_._1).take(1)
dataAndLabel.map(_._2).take(1)


// training data
val data = dataAndLabel.map(_._1).cache()


// normalize training data
val dataArray = data.map(_.toArray)
val numCols = dataArray.first().length
val n = dataArray.count()

val sums = dataArray.reduce((a, b) => a.zip(b).map(t => t._1 + t._2))
val sumSquares = dataArray.fold(new Array[Double](numCols)) (
	(a,b) => a.zip(b).map(t => t._1 + t._2 * t._2)
	)
val stdevs = sumSquares.zip(sums).map { case
	(sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
}
val means = sums.map(_ / n)

def normalize(v: Vector) = {
	val normed = (v.toArray, means, stdevs).zipped.map { 
		case (value, mean, 0) => (value - mean) / 1 // if stdev is 0
		case (value, mean, stdev) => (value - mean) / stdev
		}
	Vectors.dense(normed)
}

val normalizedData = data.map(normalize(_))

val normalizedDataAndLabel = normalizedData.zip(dataAndLabel.values)


// train the model
import org.apache.spark.mllib.clustering._
val kmeans = new KMeans()
kmeans.setK(1) // find that one center
kmeans.setRuns(10)
val model = kmeans.run(normalizedData)


// see the centroid
model.clusterCenters.foreach(centroid =>
	println(centroid.toString))

val clusterAndLabel = dataAndLabel.map { case
	(data, label) => (model.predict(data), label)
}
val clusterLabelCount = clusterAndLabel.countByValue 

clusterLabelCount.toList.sorted.foreach { case
	((cluster, label), count) =>
		println(f"$cluster%1s$label%18s$count%8s")
}


// calculate distance between data point to centroid
def distToCentroid(datum: Vector, model: KMeansModel) = {
	val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
	Vectors.sqdist(datum, centroid)
}

// decide threshold for anormalies
val distances = normalizedData.map(d => distToCentroid(d, model))
val threshold = distances.top(2000).last



//************************************
//         Testing
//************************************

// load data
val rawTestdata = sc.textFile("dataset/ad.all.csv", 120)
rawTestdata.count


// parse input
val testdataAndLabel = rawTestdata.map { line =>
	val buffer = ArrayBuffer[String]()
	buffer.appendAll(line.split(","))
	buffer.remove(1, 3) // remove categorial attributes
	val label = buffer.remove(buffer.length-1)
	val vector = Vectors.dense(buffer.map(_.toDouble).toArray) 
	(vector, label)
}


// validation data
val testdata = testdataAndLabel.map(_._1).cache()

// normalize data
val normalizedTestData = testdata.map(normalize(_))
val normalizedTestDataAndLabel = normalizedTestData.zip(testdataAndLabel.values) // put label back


// define distance measure
def distToCentroid(datum: Vector, model: KMeansModel) = {
	val centroid = model.clusterCenters(0)
	Vectors.sqdist(datum, centroid)
}


// calculate distance of validation data points
val testDistances = normalizedTestData.map(d => distToCentroid(d, model))


// get the anomalies
val anomalies = normalizedTestDataAndLabel.filter(
	d => distToCentroid(d._1, model) > threshold  // threshold is calculated during training
	)

anomalies.count()
anomalies.filter(x => x._2 != "normal.").count // count true "anomalies"