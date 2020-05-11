package com.github.codingdebugallday.analysis.orderpay.pojos

/**
 * 订单支付到账事件样例类
 */
case class ReceiptEvent(txId: String,
                        payChannel: String,
                        eventTime: Long) extends Serializable
