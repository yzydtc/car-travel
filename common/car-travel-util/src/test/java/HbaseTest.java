
import com.cartravel.common.Constants;
import com.cartravel.common.TrackPoint;
import com.cartravel.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.List;


public class HbaseTest {

    public static void main(String[] args) throws Exception {
        //配置属性
        Configuration conf = HBaseConfiguration.create();
        //conf.set("habse.zookeeper.quorum", Constants.KAFKA_BOOTSTRAP_SERVERS);
        //conf.set("hbase.zookeeper.quorum","hadoop001,hadoop002,hadoop003");
        conf.set("hbase.zookeeper.quorum","node01,node02,node03,node04,node05,node06");
        //conf.set("hbase.zookeeper.quorum", "192.168.52.100,192.168.52.110,192.168.52.120");
        conf.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        conf.addResource(new Path(ClassLoader.getSystemResource("hdfs-site.xml").toURI()));
        conf.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //建立连接
        Connection connection = ConnectionFactory.createConnection(conf);
        //创建表对象
        //创建测试
        Admin admin = connection.getAdmin();
        //判断表测试
        String talbeName1 = "DDYZY_HTAB_HAIKOU_ORDER";
        boolean ifexists = admin.tableExists(TableName.valueOf(talbeName1));
        System.out.println(ifexists);
        //创建测试
//        String tableName="T1";
//        String columnFamily="f1";
//        HTableDescriptor table = new HTableDescriptor(tableName);
//        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
//        table.addFamily(family);
//        admin.createTable(table);
        //插入测试
//        Table table = connection.getTable(TableName.valueOf("HTAB_GPS"));
//        byte [] rowkey = "11".getBytes();
//        byte [] family = "f1".getBytes();
//        Put put = new Put(rowkey);
//
//        put.addColumn(rowkey,family,"1".getBytes());
//        table.put(put);
        //List list = HBaseUtil.getRest("HTAB_GPS","39a096b71376b82f35732eff6d95779b",
       //         "1477969970","1477969982", TrackPoint.class);
       // System.out.println(list.size());
       // connection.close();


    }

}
