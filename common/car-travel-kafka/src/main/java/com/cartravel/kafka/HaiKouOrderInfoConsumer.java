package com.cartravel.kafka;

import com.cartravel.common.Constants;
import com.cartravel.common.TopicName;
import com.cartravel.util.HBaseUtil;
import com.cartravel.util.JedisUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HaiKouOrderInfoConsumer implements Runnable {
    //设置日志
    private static Logger log = Logger.getLogger(GpsConsumer.class);
    //正则用于匹配订单行数据
    private static Pattern pattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
    //设置kafkaConsumer对象
    private final KafkaConsumer<String, String> consumer;
    //topic对象
    private final String topic;
    //计数消费到的消息条数
    private static int count = 0;
    //文件输出流
    private FileOutputStream file = null;
    private BufferedOutputStream out = null;
    private PrintWriter writer = null;
    //数据行数分割对象
    private String lineSerparator = null;
    //分支对象
    private int batchNum = 0;
    //redis连接对象
    JedisUtil instance = null;
    //redis对象
    Jedis jedis = null;
    //城市编码
    private String cityCode = "";
    //
    private Map<String, String> orderMap = new HashMap<String, String>();
    //时间格式化对象
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public HaiKouOrderInfoConsumer(String topic, String groupId) {
        if (topic.equalsIgnoreCase(TopicName.CHENG_DU_GPS_TOPIC.getTopicName())) {
            cityCode = Constants.CITY_CODE_CHENG_DU;
        } else if (topic.equalsIgnoreCase(TopicName.XI_AN_GPS_TOPIC.getTopicName())) {
            cityCode = Constants.CITY_CODE_XI_AN;
        } else if (topic.equalsIgnoreCase(TopicName.HAI_KOU_ORDER_TOPIC.getTopicName())) {
            cityCode = Constants.CITY_CODE_HAI_KOU;
        } else {
            throw new IllegalArgumentException(topic + ",主题名称不合法!");
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", Constants.KAFKA_BOOTSTRAP_SERVERS);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
//        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;

    }

    /**
     * 线程方法
     */
    @Override
    public void run() {
        while (true){
            try {
                dowork();
            }catch (Exception e){
               e.printStackTrace();
            }
        }
    }

    /**
     * 线程实现方法
     */
    public void dowork() throws Exception{
        batchNum++;
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
        System.out.println("第"+batchNum+"批次,"+records.count());


            //创建Hbase表对象
            Table table = HBaseUtil.getTable(Constants.HTAB_HAIKOU_ORDER);
            //创建Jedis对象
            JedisUtil instance = JedisUtil.getInstance();
            //创建put对象
            List<Put> put = new ArrayList<>();
            //创建rowkey
            String rowkey = "";

            if (orderMap.size()>0) {
                orderMap.clear();
            }
        if(records.count() >0) {
            //判断Hbase的表是否存在，若没有，创建表
            if(!HBaseUtil.tableExists(Constants.HTAB_HAIKOU_ORDER)){
                HBaseUtil.createTable(HBaseUtil.getConnection(),Constants.HTAB_HAIKOU_ORDER,Constants.DEFAULT_FAMILY);
            }
            for(ConsumerRecord<String,String> record : records){
                count++;
                log.warn("ReceiveMsg:("+record.key()+","+record.value()+"),offset:"+record.offset()+",count:"+count);
                //取出每行的数据
                String line = record.value();
                //调用判断,数据长度判断，进行简单的清洗
                if(isDataline(line)){
                    String fields[] = line.split(" ");
                    if(fields.length!=26){
                        continue;
                    }
                    //rowkey为订单ID+出发时间
                    rowkey = fields[0]+'_'+fields[13].replaceAll("-","")
                            +fields[14].replaceAll(";","");
                    orderMap.put("ORDER_ID",   fields[0]);
                    orderMap.put("PRODUCT_ID", fields[1]);
                    orderMap.put("CITY_ID",    fields[2]);
                    orderMap.put("DISTRICT",   fields[3]);
                    orderMap.put("COUNTY",     fields[4]);
                    orderMap.put("TYPE",       fields[5]);
                    orderMap.put("COMBO_TYPE", fields[6]);
                    orderMap.put("TRAFFIC_TYPE", fields[7]);
                    orderMap.put("PASSENGER_COUNT", fields[8]);
                    orderMap.put("DRIVER_PRODUCT_ID", fields[9]);
                    orderMap.put("START_DEST_DISTANCE", fields[10]);
                    orderMap.put("ARRIVE_TIME", fields[11] + " " + fields[12]);
                    orderMap.put("DEPARTURE_TIME", fields[13] + " " + fields[14]);
                    orderMap.put("PRE_TOTAL_FEE", fields[15]);
                    orderMap.put("NORMAL_TIME", fields[16]);
                    orderMap.put("BUBBLE_TRACE_ID", fields[17]);
                    orderMap.put("PRODUCT_1LEVEL", fields[18]);
                    orderMap.put("DEST_LNG", fields[19]);
                    orderMap.put("DEST_LAT", fields[20]);
                    orderMap.put("STARTING_LNG", fields[21]);
                    orderMap.put("STARTING_LAT", fields[22]);
                    orderMap.put("YEAR", fields[23]);
                    orderMap.put("MONTH", fields[24]);
                    orderMap.put("DAY", fields[25]);
                    put.add(HBaseUtil.createPut(rowkey,Constants.DEFAULT_FAMILY.getBytes(),orderMap));
                }
            }
            table.put(put);
            instance.returnJedis(jedis);
        }
        log.warn("正常结束");
    }

    /**
     * 判断是否为数据行
     */
    public static Boolean isDataline (String line) throws Exception{
        if (StringUtils.isEmpty(line) ){
            return false;
        }
        Matcher matcher = pattern.matcher(line);
        Boolean b = matcher.find();
        return  b;
    }

    /**
     * 栅格化长度
     */
    public static int[] gridlength (double lnglats[]){
        int lng = Integer.parseInt(new java.text.DecimalFormat("0").format(lnglats[0]*111*1000/Constants.GRID_LENGTH));
        int lat = Integer.parseInt(new java.text.DecimalFormat("0").format(lnglats[1]*111*1000/Constants.GRID_LENGTH));
        return  new int[]{lng,lat};
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache.kafka").setLevel(Level.INFO);
        String topic = "hai_kou_order_topic";
        String groupId="haikou_order_g_005";
        HaiKouOrderInfoConsumer hkoic = new HaiKouOrderInfoConsumer(topic,groupId);
        Thread t = new Thread(hkoic);
        t.start();
    }
}
