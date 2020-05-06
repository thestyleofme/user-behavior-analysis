package com.github.codingdebugallday.analysis.orderpay.pojos

/**
 * 输入订单事件样例类
 */
case class OrderEvent(orderId: Long,
                      eventType: String,
                      txId: String,
                      eventTime: Long) extends Serializable
