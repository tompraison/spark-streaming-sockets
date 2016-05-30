package arrow.thea.sockets

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods._

/**
 * Created by tpraison on 5/14/16.
 */
object App {

  def main(args :Array[String]){
    val conf = new SparkConf().setMaster("local[2]").setAppName("spark-streaming-sockets")
    val ssc = new StreamingContext(conf, Seconds(60))
    val lines = ssc.receiverStream(new SocketIOReceiver("http://stream.wikimedia.org/rc"))
    val titles = lines.map(s => {
      val p = parse(s)
      p \ "title".toString
    }).map(t => (t,1)).reduceByKey(_ + _)


    titles.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
