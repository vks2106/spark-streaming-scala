package MovingRatings
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.Map
import java.io.File
import org.apache.spark.sql.SparkSession


object MovieRatingsExample {

  def mapUserIdAndMovieRatings(spark : SparkSession ): RDD[(Int,(Int,Double))]= {
    val dataFile: RDD[String] =spark.sparkContext.textFile("C:\\gitlab\\scala\\spark-streaming-scala\\src\\main\\resources\\movies\\u.data")

    val userIdMappedWithMovieIdAndRating: RDD[(Int, (Int, Double))] =dataFile.map(line =>{
      val fields=line.split("\\s+") //Using regex to split
      (fields(0).toInt,(fields(1).toInt,fields(2).toDouble))
    })
    userIdMappedWithMovieIdAndRating
  }




  def main(args: Array[String]): Unit = {
    import util.SparkSetup._
    mapUserIdAndMovieRatings(spark).take(2)
    //Finding movie sets along with the rating given by each particular user
    //Finding movie sets along with the rating given by each particular user


  }



}
