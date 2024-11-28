package app.loaders
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext



/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load():  RDD[(Int, String, List[String])] = {


    val distFile = sc.textFile("/Users/yannisbantzis/Documents/EPFL/SEM 2/COM460Proj/src/main/resources"+path)
      .map(line => line.replace("\"",""))
      .map(line => line.split("\\|"))
      .map(line => {(line(0).toInt,line(1).replace("\"",""),line.toList.drop(2))})



    return distFile
  }
}

