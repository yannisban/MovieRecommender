package app.aggregator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var result: RDD[(String, Double, Int, List[String],Int)] = _
  private var avgmovies: RDD[((Int,(Double, Int)))] = _
  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    val updated_ratings = ratings.groupBy(row => (row._1, row._2))
      .map(row => if (row._2.size == 1) (row._1,row._2.head) else (row._1,row._2.toList.maxBy(_._5)))
      .map(row => (row._2._2) -> (row._2._4,1))

    avgmovies = updated_ratings.reduceByKey {
      case ((sumL, countL), (sumR, countR)) =>
        (sumL + sumR, countL + countR)
    }

    result = avgmovies.rightOuterJoin(title.map( row=> row._1 -> (row._2,row._3)))
      .map(row => (row._2._2._1, row._2._1.getOrElse((0.0,0))._1,row._2._1.getOrElse((0.0,0))._2, row._2._2._2,row._1)).persist()
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    return result.map(row=> (row._1, if (row._3 != 0) row._2/ row._3 else 0.0))
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    var filtered = result.map(row => (row._1, row._2,row._3,row._4))
      .map(row => (row._1, if (row._2 != 0.0) row._2 / row._3 else 0.0,row._4))

    for(keyword <- keywords){
    filtered = filtered.filter(row => row._3.contains(keyword))}

    if (filtered.isEmpty()) {
      return -1.0
    }

    val res = filtered.map(row => (row._2,1))
      .filter(row => row._1 > 0.0)
      .reduce {
        case ((sumL, countL), (sumR, countR)) =>
          (sumL + sumR, countL + countR)
      }

    return res._1/res._2
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val deltaRDD = sc.parallelize(delta_)
      .map(row => row._2 -> (row._4 - row._3.getOrElse(0.0),if (row._3.getOrElse(0.0) != 0.0) 0 else 1))
      .reduceByKey {
        case ((sumL, countL), (sumR, countR)) =>
          (sumL + sumR, countL + countR)
      }

//      subtract previous rating if movie already rated and dont update counter

    var result_ids = result.map(row => row._5 -> (row._1,row._2,row._3,row._4))

    result = result_ids.leftOuterJoin(deltaRDD)
      .map(row => (row._2._1._1,row._2._1._2 + row._2._2.getOrElse((0.0,0))._1, row._2._1._3 + row._2._2.getOrElse((0.0,0))._2,row._2._1._4,row._1))
      .persist()

//    add rating to sum and counter

  }
}
