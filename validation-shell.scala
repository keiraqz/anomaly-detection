/**
 * @author Keira Zhou
 * @date 12/21/15
 *
 * Validation code
 * <p>
 * Use the model from the training result and test on all data points
 */

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


// normalize validation data
import scala.math._
import org.apache.spark.rdd._

val testDataArray = testdata.map(_.toArray)
val numCols = testDataArray.first().length
val n = testDataArray.count()
val sums = testDataArray.reduce((a, b) => a.zip(b).map(t => t._1 + t._2))
val sumSquares = testDataArray.fold(new Array[Double](numCols)) (
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

val normalizedTestData = testdata.map(normalize(_))
val normalizedTestDataAndLabel = normalizedTestData.zip(testdataAndLabel.values) // put label back


// define distance measure
def distToCentroid(datum: Vector, model: KMeansModel) = {
	val centroid = model.clusterCenters(model.predict(datum))
	Vectors.sqdist(datum, centroid)
}


// calculate distance of validation data points
val testDistances = normalizedTestData.map(d => distToCentroid(d, model))


// get the anomalies
val anomalies = normalizedTestDataAndLabel.filter(
	d => distToCentroid(d._1, model) > threshold  // threshold is calculated during training
	)

anomalies.count()
anomalies.toArray.foreach(x => println(x._2))
anomalies.filter(x => x._2 != "normal.").count // count true "anomalies"
