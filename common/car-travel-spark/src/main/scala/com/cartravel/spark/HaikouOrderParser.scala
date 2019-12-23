package com.cartravel.spark

class HaikouOrderParser extends OrderParser {
  override def parser(orderInfo: String): TravelOrder = {
    val regex = "[0-9]{4}-[0-9]{2}-[0-9]".r
    val iterator = regex.findFirstIn(orderInfo)
    if(iterator.isEmpty){
      return null;
    }

    //使用\t分割
    var orderInfos = orderInfo.split(" ",-1);
    var order = new HaiKouTravelOrder();

    //数据清洗第一步，过滤不符合业务的行,会过滤文件的第一行
    println("orderInfos:"+orderInfos.size)
    if(null==orderInfos||(orderInfos.size!=26)){
      return null;
    }

    order.orderId = orderInfos(0)
    order.productId = orderInfos(1)
    order.cityId = orderInfos(2)
    order.district = orderInfos(3)
    order.county = orderInfos(4)
    order.orderTimeType = orderInfos(5)
    order.comboType = orderInfos(6)
    order.trafficType = orderInfos(7)
    order.passengerCount = orderInfos(8)
    order.driverProductId = orderInfos(9)
    order.startDestDistance = orderInfos(10)
    order.arriveDay = orderInfos(11)+""
    order.arriveTime = orderInfos(12)+""
    order.departureDay = orderInfos(13)+""
    order.departureTime = orderInfos(14)+""
    order.preTotalFee = orderInfos(15)
    order.normalTime = orderInfos(16)
    order.bubbleTraceId = orderInfos(17)
    order.productLlevel = orderInfos(18)
    order.destLng = orderInfos(19)
    order.destLat = orderInfos(20)
    order.startingLng = orderInfos(21)
    order.startingLat = orderInfos(22)
    order.year = orderInfos(23)
    order.month = orderInfos(24)
    order.day = orderInfos(25)

    //订单数据中没有实际产生时间,可以按照订单出发时间作为订单的产生时间
    order.createDay=order.departureDay

    order
  }
}

//object HaikouOrderParser {
//  def main(args: Array[String]): Unit = {
//    val haikouOrderParser = new HaikouOrderParser();
//    val orderInfo="17592719043682 3 83 0898 460107 0 0 0 4 3 4361 2017-05-19 01:09:12 2017-05-19 01:05:19 13 11 10466d3f609cb938dd153738103b0303 3 110.3645 20.0353 110.3665 20.0059 2017 05 19";
//    haikouOrderParser.parser(orderInfo)
//  }
//}