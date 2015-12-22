/**
 * @author Keira Zhou
 * @date 12/22/15
 *
 * Validation in OOP.
 * Use "sbt assambly", "sbt package" to compile.
 * Use "spark-submit" to submit the job.
 * The job loads the "normal" centroid and threshold from the output file,
 * and streaming new inputs and tag them. 
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering._
import java.io._
import org.apache.spark.streaming._
import scala.collection.mutable
// import java.util.Date
// import org.joda.time.DateTime
// import org.joda.time.format.DateTimeFormat

object AnomalyDetectionTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("AnomalyDetectionTest")
    val sc = new SparkContext(sparkConf)
    val model = loadCentroidAndThreshold(sc).cache()

    // load model info
    val centroid = model.first()._1
    val threshold = model.first()._2

    // load data
    val normalizedTestDataAndLabel = loadData(sc)

    val ssc = new StreamingContext(sc, Seconds(3))
    // put data into a queue
    val lines = mutable.Queue[RDD[(Vector, String)]]()
    val messages = ssc.queueStream(lines)
    
    messages.foreachRDD { rdd => 

    // get the anomalies
      val anomalies = rdd.filter(
          d => Vectors.sqdist(d._1, centroid) > threshold  // threshold is calculated during training
          )
      println(anomalies.count)

      // rdd.foreach(println)
    }

    
    ssc.start() // Start the computation
    lines += normalizedTestDataAndLabel // add data to the stream
    // ssc.stop()
    ssc.awaitTermination()
  }

  def loadCentroidAndThreshold(sc: SparkContext) : RDD[(Vector,Double)] = {
    val modelInfo = sc.textFile("/Users/Shanghai/Developer/Spark/AnomalyDetection/dataset/trainOutput.txt", 120)
    // val modelInfo = sc.textFile("../dataset/trainOutput.txt", 120)
    // parse data file
    val centroidAndThreshold = modelInfo.map { line =>
      val buffer = ArrayBuffer[String]()
      buffer.appendAll(line.split(","))
      val threshold = buffer.remove(buffer.length-1)
      val centroid = Vectors.dense(buffer.map(_.toDouble).toArray)
      (centroid, threshold.toDouble)
    }
    centroidAndThreshold
  }

  /**
   * Load data from file, parse the data and normalize the data.
   */
  def loadData(sc: SparkContext) : RDD[(Vector, String)] = {
    val rawData = sc.textFile("/Users/Shanghai/Developer/Spark/AnomalyDetection/dataset/ad.train.csv", 120)
    // val rawData = sc.textFile("../dataset/ad.train.csv", 120)

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
    val normalizedTestDataAndLabel = normalizedData.zip(dataAndLabel.values) // put label back
    normalizedTestDataAndLabel
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

}



object TimestampFormatter {

  private val TimestampPattern = "yyyy-MM-dd'T'HH:mm:ssZ"

  def format(date: Date): String =
    DateTimeFormat.forPattern(TimestampPattern).print(new DateTime(date.getTime))
}