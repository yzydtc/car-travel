package com.cartravel.spark

class XianOrderParser extends OrderParser {
  override def parser(orderInfo: String): TravelOrder = {
    var orderInfos = orderInfo.split(",",-1)
    var order = new XiAnTravelOrder();
    order.orderId= orderInfos(0);
    order
  }
}
