import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MovieRatingAnalysis {
  def main(args: Array[String]): Unit = {
    println("Building spark session")
    val spark = SparkSession
      .builder()
      .appName("Spark-Redis Demo")
      .config("spark.redis.host","spark-redis-001.i2dctu.0001.euw1.cache.amazonaws.com")
      .config("spark.redis.port","6379")
      .getOrCreate()

    println("Reading data from the stream")
    val ratings = spark
      .readStream
      .format("redis")
      .option("stream.keys","ratings")
      .schema(StructType(Array(
        StructField("userid", StringType),
        StructField("movieid", StringType),
        StructField("rating", StringType),
        StructField("timestamp", StringType),
      )))
      .load()

    val byrating = ratings.groupBy(col("rating")).count

    val ratingWriter: RatingForeachWriter = new RatingForeachWriter("spark-redis-001.i2dctu.0001.euw1.cache.amazonaws.com","6379")

    println("Writing results back into Redis")

    val query = byrating
      .writeStream
      .outputMode("update")
      .foreach(ratingWriter)
      .start()

    query.awaitTermination()
  }
}
