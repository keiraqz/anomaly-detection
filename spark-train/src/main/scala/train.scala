/**
 * @author Keira Zhou
 * @date 12/21/15
 *
 * Similar training code as in Spark Shell, but in OOP and with modification.
 * Use "sbt assambly", "sbt package" to compile.
 * Use "spark-submit" to submit the job.
 * The job save the "normal" centroid and threshold into an output file. 
 * (may try use Redis later)
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering._
import java.io._

object AnomalyDetection {
  def main(args: Array[String]) {

    // val sparkConf = new SparkConf().setAppName("geo_data").set("spark.cassandra.connection.host", "172.31.11.232")
    val sparkConf = new SparkConf().setAppName("AnomalyDetection")
    val sc = new SparkContext(sparkConf)
    val normalizedData = loadData(sc)
    val model = trainModel(normalizedData)
    val file = new File("../dataset/trainOutput.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val centroid = model.clusterCenters(0).toString // save centroid to file
  	bw.write(centroid)
  	bw.write(",")
    // decide threshold for anormalies
    val distances = normalizedData.map(d => distToCentroid(d, model))
    val threshold = distances.top(2000).last // set the last of the furthest 2000 data points as the threshold
    bw.write(threshold.toString) // last item is the threshold
    bw.close()
  }

  /**
   * Load data from file, parse the data and normalize the data.
   */
  def loadData(sc: SparkContext) : RDD[Vector] = {
    val rawData = sc.textFile("../dataset/ad.train.csv", 120)
    // parse data file
    val dataAndLabel = rawData.map { line =>
      val buffer = ArrayBuffer[String]()
      buffer.appendAll(line.split(","))
      buffer.remove(1, 3) // remove categorial attributes
      val label = buffer.remove(buffer.length-1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray) 
      (vector, label)
    }

    val data = dataAndLabel.map(_._1).cache()
    val normalizedData = normalization(data)
    normalizedData
    // val normalizedDataAndLabel = normalizedData.zip(dataAndLabel.values) // put back the label
  }

  /**
   * Normalization function. 
   * Normalize the training data. 
   */
  def normalization(data: RDD[Vector]): RDD[Vector] = {
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

    def normalize(v: Vector): Vector = {
      val normed = (v.toArray, means, stdevs).zipped.map { 
        case (value, mean, 0) => (value - mean) / 1 // if stdev is 0
        case (value, mean, stdev) => (value - mean) / stdev
        }
      Vectors.dense(normed)
    }

    val normalizedData = data.map(normalize(_)) // do nomalization
    normalizedData
  }

  /**
   * Train a KMean model using normalized data.
   */
  def trainModel(normalizedData: RDD[Vector]): KMeansModel = {
    val kmeans = new KMeans()
    kmeans.setK(1) // find that one center
    kmeans.setRuns(10) // run 10 iterations
    val model = kmeans.run(normalizedData)
    model
  }

  /**
   * Calculate distance between data point to centroid.
   */
  def distToCentroid(datum: Vector, model: KMeansModel) : Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

}


// submit
// spark-submit --class AnomalyDetection target/scala-2.11/anomalydetection_2.11-1.0.jar

