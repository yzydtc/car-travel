import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

class HbaseTestScala {

}
object HbaseTestScala {
  def main(args: Array[String]): Unit = {
    lazy val conf:Configuration = HBaseConfiguration.create()
    conf.set("habse.zookeeper.quorum","node05.kaikeba.com,node06.kaikeba.com,node01.kaikeba.com," +
      "node02.kaikeba.com,node03.kaikeba.com,node04.kaikeba.com");
    conf.set("hbase.zookeeper.property.clientPort","2181");
    val connection = ConnectionFactory.createConnection(conf)

    val admin = connection.getAdmin


    println(admin.tableExists(TableName.valueOf("HTAB_GPS")))

  }
}
