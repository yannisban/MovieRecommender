package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import java.time.Instant
import java.time.ZoneOffset



class SimpleAnalytics() extends Serializable {
  private var ratingsPartitioner: HashPartitioner = new HashPartitioner(10)
  private var moviesPartitioner: HashPartitioner = new HashPartitioner(10)
  private var titlesGroupedById : RDD[(Int, Iterable[(Int, String, List[String])])] = _
  private var ratingsGroupedByYearByTitle : RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = _
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit ={

    titlesGroupedById = movie.groupBy(movie => movie._1)
      .partitionBy(moviesPartitioner)
      .persist()


    ratingsGroupedByYearByTitle = ratings.map(line => {
      (line._1,line._2, line._3,line._4,Instant.ofEpochSecond(line._5).atZone(ZoneOffset.UTC).toLocalDateTime.getYear)
    })
      .groupBy(rating => rating._5)
      .mapValues(ratingsperyear => ratingsperyear.groupBy(rating => rating._2))
      .partitionBy(ratingsPartitioner)
      .persist()

  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {

    val countRDD: RDD[(Int, Int)] = ratingsGroupedByYearByTitle.mapValues(_.size)

    return countRDD
  }


  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    val countRDD= ratingsGroupedByYearByTitle.map(a => (a._1, a._2.mapValues(_.size)))

    val res_int = countRDD.map(a => (a._1, a._2.toList.filter(_._2 == a._2.maxBy(_._2)._2).sortBy(_._1).takeRight(1).head._1))

    val movie_titles = titlesGroupedById.map(a => (a._1, a._2.head._2))

    val res_swapped = res_int.map(_.swap)

    val joined_res = res_swapped.join(movie_titles)

    val res = joined_res.map(a => (a._2._1, a._2._2))


    return res
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {

    val movies = titlesGroupedById.map(a => (a._2.head._2, a._2.head._3))

    val mostrated = getMostRatedMovieEachYear.map(_.swap)

    val joined = mostrated.join(movies).map(a => (a._2._1,a._2._2))

    return joined
  }


  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {

    val movies = titlesGroupedById.map(a => (a._2.head._2, a._2.head._3))

    val mostrated = getMostRatedMovieEachYear.map(_.swap)

    val joined = mostrated.join(movies)

    val categories = joined.flatMap(movie => movie._2._2)

    val counts = categories.map(x => (x, 1)).reduceByKey(_ + _)

    val maximum = counts.sortBy(_._2,ascending = false).take(1)(0)

    val minimum = counts.sortBy(_._2).take(1)(0)

    val descending = counts.filter(count => count._2 == maximum._2).sortBy(_._1)

    val ascending = counts.map(count => (count._1,count._2)).filter(count => count._2 == minimum._2).sortBy(_._1)


    return (ascending.take(1)(0),descending.take(1)(0))
  }
  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val genres = requiredGenres.collect()
    return movies.filter(movie => movie._3.intersect(genres).nonEmpty).map(movie => movie._2)
  }


  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {

    val broadcastGenres = broadcastCallback(requiredGenres)
    return movies.filter(movie => movie._3.intersect(broadcastGenres.value).nonEmpty).map(movie => movie._2)
  }

}

