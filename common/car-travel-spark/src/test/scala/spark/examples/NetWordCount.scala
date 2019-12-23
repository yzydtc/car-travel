package spark.examples
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network every second.
 *
 * Usage: NetworkWordCount <hostname> <port>
 * <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 * `$ nc -lk 9999`
 * and then run the example
 * `$ bin/run-example org.apache.spark.examples.streaming.NetworkWordCount localhost 9999`
 */
object NetworkWordCount {
  //设置spark控制台日志级别
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]) {

    val host = "hadoop002"
    val port = 9998;

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    //    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    ssc.checkpoint("F:\\didi_Bigdata\\project\\car-travel\\common\\car-travel-spark\\target\\streaming_output")
    val wordCounts = words.map(x => (x, 1)).updateStateByKey((vals: Seq[Int], sumOpt: Option[Int]) => {
      var count = vals.sum
      if (!sumOpt.isEmpty) {
        count = count + sumOpt.get
      }
      Some(count)
    })

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}