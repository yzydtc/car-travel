package spark.examples
import com.cartravel.common.Constants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamingAndKakfa {
  //设置日志告警级别
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    import org.apache.spark._
    import org.apache.spark.streaming._

    //1.sparkconf
    val sparkconf = new SparkConf().setAppName("sparkStreamingAndKafka").setMaster("local[*]")
    //2.SparkStreaming编程入口
    val streamingContext = new StreamingContext(sparkconf,Seconds(1))
    //设置检查点
    streamingContext.checkpoint("/sparkapp/tmp")
    //创建kafka参数列表
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "hadoop001:9092,hadoop002:9092,hadoop003:9092",
      //"bootstrap.servers" -> Constants.KAFKA_BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test0002",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )
    //创建topic参数
    //val topics = Array("test_msg")
    val topics = Array("hai_kou_order_topic")
    topics.foreach(println)
    println("topic:"+topics)
    //创建stream对象
    val  stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String,String](topics,kafkaParams)
    )

    //打印结果
    stream.count().print()
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }
}