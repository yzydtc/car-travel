package spark.examples

import org.apache.spark.{SparkConf, SparkContext}

class WordCount {

}
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建配置对象
    val sparkconf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2.创建上下文
    val sc = new SparkContext(sparkconf)
    //3.创建RDD
    val linesRDD = sc.textFile("../car-travel/common/car-travel-spark/src/test/data/word")
    //wc过程
    //4.对RDD每一行行进行分割（使用空格分割）
    val wordArrayRdd = linesRDD.map(_.split(" "))
    //5.分割完成之后，我们做一下扁平化，把多维集合转化为一维集合
    val wordsRDD = wordArrayRdd.flatMap(x=>x)
    //6.单词计数，出现一次计数一个1
    val wordCount = wordsRDD.map((_,1))
    //7.最关键的一步就是对单词的所有计数进行汇总
    val restCount = wordCount.reduceByKey(_+_)

    //输出结果
    restCount.foreach(println)
    //restCount.checkpoint()
    restCount.persist()
    sc.stop();
  }
}
