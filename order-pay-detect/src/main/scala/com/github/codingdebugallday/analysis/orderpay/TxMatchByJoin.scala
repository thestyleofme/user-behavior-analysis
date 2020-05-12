package com.github.codingdebugallday.analysis.orderpay

import com.github.codingdebugallday.analysis.orderpay.pojos.{OrderEvent, ReceiptEvent}
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

//noinspection DuplicatedCode
object TxMatchByJoin {
  private val log = Logger(LoggerFactory.getLogger(TxMatchByJoin.getClass))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val orderEventSourceStream: DataStream[String] = env.readTextFile(
      getClass.getResource("/OrderLog.csv").getPath)
    // 订单输入事件流
    val orderEventStream: KeyedStream[OrderEvent, String] = orderEventSourceStream
      .map(data => {
        try {
          val splits: Array[String] = data.split(",")
          OrderEvent(splits(0).trim.toLong, splits(1).trim, splits(2).trim, splits(3).trim.toLong * 1000L)
        } catch {
          case e: Exception =>
            log.error("dirty record, data: {}", data, e)
            null
        }
      })
      .filter(_ != null)
      // 只关心支付了的订单
      .filter(o => o.txId != null && o.txId != "")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.milliseconds(500)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime
        }
      })
      .keyBy(_.txId)

    // 支付到账事件流
    val receiptEventSourceStream: DataStream[String] = env.readTextFile(
      getClass.getResource("/ReceiptLog.csv").getPath)
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = receiptEventSourceStream
      .map(data => {
        try {
          val splits: Array[String] = data.split(",")
          ReceiptEvent(splits(0).trim, splits(1).trim, splits(2).trim.toLong * 1000L)
        } catch {
          case e: Exception =>
            log.error("dirty record, data: {}", data, e)
            null
        }
      })
      .filter(_ != null)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.milliseconds(500)) {
        override def extractTimestamp(element: ReceiptEvent): Long = {
          element.eventTime
        }
      })
      .keyBy(_.txId)

    // intervalJoin 只能找到匹配的
    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print("matched")

    env.execute("tx pay match by join job")

  }

}

/**
 * 只能找到匹配的
 */
class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
