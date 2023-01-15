import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

class RatingForeachWriter(p_host: String, p_port: String) extends ForeachWriter[Row] {
  val host: String = p_host
  val port: String = p_port


  var jedis: Jedis = _

  def connect(): Unit = {
    jedis = new Jedis(host, port.toInt)
  }
  override def open(partitionId: Long, epochId: Long): Boolean = {
    return true
  }

  override def process(record: Row): Unit = {
    var rating = record.getString(0)
    var count = record.getLong(1)
    println("Connecting to REDIS")
    if(jedis == null) {
      connect()
      println("Connected to REDIS. Start pushing results")
    }

    println("PUSHING data back to REDIS for KEY : "+"prating:"+rating)
    jedis.hset("prating:"+rating, "rating", rating)
    jedis.hset("prating:"+rating, "count", count.toString)
  }

  override def close(errorOrNull: Throwable): Unit = {

  }
}
