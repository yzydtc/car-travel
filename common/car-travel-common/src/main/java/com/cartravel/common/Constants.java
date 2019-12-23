package com.cartravel.common;

public class Constants {
    //ali_zookeeper
    //ali_cdh
    //public final static String KAFKA_BOOTSTRAP_SERVERS = "hadoop001:9092,hadoop002:9092,hadoop003:9092";
    //didi_cdh
    public final static String KAFKA_BOOTSTRAP_SERVERS = "node01.kaikeba.com:9092,node02.kaikeba.com:9092,node03.kaikeba.com:9092,node04.kaikeba.com:9092,node05.kaikeba.com:9092,node06.kaikeba.com:9092";
    //存储订单gps轨迹信息
    public final static String HTAB_GPS = "DDYZY_HTAB_GPS";
    //海口订单信息表
    public final static String HTAB_HAIKOU_ORDER = "DDYZY_HTAB_HAIKOU_ORDER";
    //默认的hbase表的列簇名
    public final static String DEFAULT_FAMILY = "f1";
    //成都市编码
    public final static String CITY_CODE_CHENG_DU = "510100";
    //西安市城市编码
    public final static String CITY_CODE_XI_AN = "610100";
    //海口市编码
    public final static String CITY_CODE_HAI_KOU = "460100";
    //订单数，在redis中作为hash结构的key名称
    public final static  String ORDER_COUNT = "order_count";
    //人数统计,在redis中存储乘车人数实时统计的结果
    public final static String PASSENGER_COUNT = "passenger_count";
    //实时订单
    public final static String REALTIME_ORDERS = "realtime_orders";
    //订单起始时间，讲订单实体类进行序列化到redis中，用于判断订单是实时订单还是历史订单
    public final static String ORDER_START_END_TIME = "order_start_end_time";
    //经纬度栅格化半径(单位:米)
    public final static int GRID_LENGTH = 100;

}
