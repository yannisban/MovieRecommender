package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {

//    val ratings = //sc.textFile(getClass.getResource(path).getPath)
      val ratings = sc.textFile("/Users/yannisbantzis/Documents/EPFL/SEM 2/COM460Proj/src/main/resources"+path)
      .map(line => line.split("\\|"))
      .map(line => {
        (line(0).toInt, line(1).toInt, line(2).toDouble, line(3).toInt)
      })


    val groupedRdd = ratings.groupBy { case (user, movie, rating, timestamp) =>
      (user, movie)
    }

    val newRdd = groupedRdd.flatMap { case ((user, movie), group) =>
      val sortedGroup = group.toList.sortBy(_._4) // Sort by timestamp
      sortedGroup.map { case (u, m, r, t) =>
        val prevRating = sortedGroup.filter(_._4 < t).lastOption.map(_._3)
        (u, m, prevRating, r, t)
      }
    }


    return newRdd
  }
}