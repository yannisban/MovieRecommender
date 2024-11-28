package app.recommender.collaborativeFiltering


import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val ratings = ratingsRDD.map(x=> (x._1,x._2,x._4))
    val ratings_prepared = ratings.map { case (user, movie, rating) =>
      Rating(user, movie, rating)
    }

    model = new ALS()
      .setIterations(maxIterations)
      .setSeed(seed)
      .setRank(rank)
      .setBlocks(n_parallel)
      .setLambda(regularizationParameter)
      .run(ratings_prepared)
  }

  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId,movieId)
  }

}
