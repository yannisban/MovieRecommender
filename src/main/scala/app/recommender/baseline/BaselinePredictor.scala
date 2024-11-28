package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  private var prepared_ratings: RDD[(Int, (Double, Double))] = _
  private var global_deviation: RDD[(Int, Double)] = _
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val avgratings = ratingsRDD.map(x=> x._1-> (x._4,1))
      .reduceByKey {
      case ((sumL, countL), (sumR, countR)) =>
        (sumL + sumR, countL + countR)
    }
      .mapValues(x => x._1 / x._2.toDouble)

    val ratingsjoined = ratingsRDD.map(x=> x._1 -> (x._2,x._4)).join(avgratings)
      .map(x => (x._1,x._2._1._1,x._2._1._2,x._2._2))

    val ratings_scaled = ratingsjoined.map(x => (x._1,x._2,(x._3 - x._4)/scale(x._3,x._4),x._4))

   global_deviation = ratings_scaled.map(x => x._2 -> (x._3, 1)).reduceByKey {
      case ((sumL, countL), (sumR, countR)) =>
        (sumL + sumR, countL + countR)
    }
      .mapValues(x => x._1 / x._2.toDouble).persist()

   prepared_ratings = ratings_scaled.map(x=> x._1 -> (x._3,x._4)).persist()
  }

  def scale(rating: Double, average: Double): Double = {
    var res = 0.0
    if (rating > average)
      (res = 5 - average)
    else if (rating == average)
      (res = 1 )
    else
      (res = average - 1)
    res
  }

  def predict(userId: Int, movieId: Int): Double = {
    var deviation = global_deviation.lookup(movieId).head
    prepared_ratings.lookup(userId).map(x => x._2 + (deviation * scale(x._2 + deviation,x._2))).head
  }
}
