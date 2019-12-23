package com.cartravel.spark
import java.util

import com.alibaba.fastjson.JSONArray
import com.cartravel.util.{HBaseUtil, MapUtil}
import com.uber.h3core.H3Core
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.locationtech.jts.geom.{Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.JavaConversions._

class VirtualStationsProcessor {

}

object  VirtualStationsProcessor {

  val h3 = H3Core.newInstance()
  /***
   * 经纬度转成哈希值的方法
   * @param lat 经度
   * @param lng 纬度
   * @param res 精度范围
   */
  def locationToH3 (lat:Double,lng:Double,res:Int): Long ={
    h3.geoToH3(lat,lng,res)
  }


  def main(args: Array[String]): Unit = {
    val DDYZY_HTAB_HAIKOU_ORDER = "DDYZY_HTAB_HAIKOU_ORDER"
    import org.apache.spark._
    //设置日志打印等级
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]").setAppName("Virtual_Stations")
    //建立SparkSql程序入口
    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    val sqlContext = sparkSession.sqlContext
    //注册自定义函数
    sparkSession.udf.register("locationToH3",locationToH3 _)
    //解析经纬度的值
    val districList = new java.util.ArrayList[com.cartravel.util.District] {}
    val districts = MapUtil.getDistricts("海口市",null)
    MapUtil.parseDistrictInfo(districts,null,districList)

    //行政区域广播变量
    val districtBroadcastVar = sparkSession.sparkContext.broadcast(districList)
    //加载Hbase中的订单数据
    val hbconf = HBaseConfiguration.create(sparkSession.sparkContext.hadoopConfiguration)
    //didi_dev
    //hbconf.set("hbase.zookeeper.quorum", "node01,node02,node03,node04,node05,node06")
    //hbconf.set("hbase.zookeeper.property.clientPort", "2181")
    //ali_dev
    hbconf.set("hbase.zookeeper.quorum", "hadoop001,hadoop002,hadoop003")
    hbconf.set("hbase.zookeeper.property.clientPort", "2181")
    val order: DataFrame = HbaseLoader.loadData(hbconf,sqlContext,DDYZY_HTAB_HAIKOU_ORDER)
    //打印订单数据,测试用，上线删除
    println("订单数据:")
   // order.show(5)
    //建立临时视图
    order.createTempView("order")
    val gridDf = sparkSession.sql(
      s"""
         |select
         |ORDER_ID,
         |CITY_ID,
         |STARTING_LNG,
         |STARTING_LAT,
         |locationToH3(STARTING_LAT,STARTING_LNG,12) as h3Code
         |from order
         |""".stripMargin
    )
    println("栅格化后的数据:")
    gridDf.show()
    gridDf.createTempView("order_grid")

    //分组统计
    val groupCountDf = gridDf.groupBy("h3coude").count().filter("count>=10")
    groupCountDf.createTempView("groupcount")
    val groupJoinDf = sparkSession.sql(
      s"""
         |select
         |ORDER_ID,
         |CITY_ID,
         |STARTING_LNG,
         |STARTING_LAT,
         |row_number()over(partition by order_id.h3coude order by STARTING_LNG,STARTING_LAT asc) rn
         |from order_grid join groupcount on order_grid.h3coude = groupcount.h3coude
         |having(rn=1)
         |""".stripMargin
    )
  }
}

/***
 * Hbase加载类
 */
object HbaseLoader {
  /**
   * Hbase数据加载方法，返回RDD
   * @param conf
   * @param sc
   * @param tableName
   * @return
   */
  def loadData(conf:Configuration,sc:SparkContext,tableName:String):RDD[(ImmutableBytesWritable,Result)]={
    val scanner = new Scan
    //插入列簇，列
    scanner.addFamily(Bytes.toBytes("f1"))
    scanner.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ORDER_ID"))
    scanner.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("CITY_ID"))
    scanner.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("STARTING_LNG"))
    scanner.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("STARTING_LAT"))
    //scan对象转字符串
    import org.apache.hadoop.hbase.protobuf.ProtobufUtil
    val proto = ProtobufUtil.toScan(scanner)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    //设置habse表的信息到configuration
    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    conf.set(TableInputFormat.SCAN,scanToString)
    sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
  }

  /**
   *hbse加载数据方法，返回df
   * @param conf
   * @param sqlContext
   * @param tableName
   * @return
   */
  //返回df的加载数据方法
  def loadData(conf:Configuration,sqlContext:SQLContext,tableName:String):DataFrame = {
    val sc = sqlContext.sparkContext
    val value : RDD[(ImmutableBytesWritable,Result)] = loadData(conf,sc,tableName)
    val rowRDD =value.mapPartitions( iterator =>{
      val newItems = new util.ArrayList[Row]()
      var row:Row = null
      while(iterator.hasNext){
        val result = iterator.next()._2
        row = Row.fromSeq(Seq(
          Bytes.toString(result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("ORDER_ID"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("CITY_ID"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("STARTING_LNG"))),
          Bytes.toString(result.getValue(Bytes.toBytes("f1"),Bytes.toBytes("STARTING_LAT")))))
        newItems.add(row)
      }
      Iterator(newItems)
    }).flatMap(row => row)

    val fields = Array(
      new StructField("ORDER_ID",StringType),
      new StructField("CITY_ID",StringType),
      new StructField("STARTING_LNG",StringType),
      new StructField("STARTING_LAT",StringType)
    )
    val structType =new StructType(fields)
    sqlContext.createDataFrame(rowRDD,structType)
  }

  /**
   * 保存hbase的数据
   * @param configuraton 配置信息
   * @param result hbase的结果集
   * @param tableName 保存的表名
   */
  def saveOrWriteData(configuraton:Configuration,result:DataFrame,tableName:String): Unit ={
    configuraton.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val job = new Job(configuraton)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val kvRdd = result.rdd.mapPartitions(iterator=>{
      val newItems = new util.ArrayList[(ImmutableBytesWritable,Put)]()
      while(iterator.hasNext){
        val next:Row = iterator.next()
        val put = new Put(Bytes.toBytes(next.getString(0)))
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ORDER_ID"),Bytes.toBytes(next.getAs[String]("ORDER_ID")))
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("CITY_ID"),Bytes.toBytes(next.getAs[String]("CITY_ID")))
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("STARTING_LNG"),Bytes.toBytes(next.getAs[String]("STARTING_LNG")))
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("STARTING_LAT"),Bytes.toBytes(next.getAs[String]("STARTING_LAT")))
        //插入元素
        newItems.add((new ImmutableBytesWritable,put))
      }
      Iterator(newItems)
    }).flatMap(kv => kv)
    HBaseUtil.createTable(HBaseUtil.getConnection,tableName,"f1")
    kvRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}