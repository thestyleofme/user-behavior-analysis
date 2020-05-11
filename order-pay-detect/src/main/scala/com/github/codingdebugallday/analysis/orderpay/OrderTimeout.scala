package com.github.codingdebugallday.analysis.orderpay

import java.util

import com.github.codingdebugallday.analysis.orderpay.pojos.{OrderEvent, OrderResult}
import com.typesafe.scalalogging.Logger
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

/**
 * 订单超时事件处理
 */
//noinspection DuplicatedCode
object OrderTimeout {

  private val log = Logger(LoggerFactory.getLogger(OrderTimeout.getClass))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val sourceStream: DataStream[String] = env.readTextFile(
      getClass.getResource("/OrderLog.csv").getPath)
    val orderEventStream: KeyedStream[OrderEvent, Long] = sourceStream
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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.milliseconds(500)) {
        override def extractTimestamp(element: OrderEvent): Long = {
          element.eventTime
        }
      })
      .keyBy(_.orderId)

    // 定义一个匹配模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] =
      Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
        .followedBy("follow").where(_.eventType == "pay")
        .within(Time.minutes(15))

    // 模式运用到stream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    // 调用select方法，提取事件序列，超时事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect()
    )

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout job")
  }
}

/**
 * 自定义超时事件序列 处理函数 即订单超时
 */
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    // 这里是没有匹配上 这里应该只有一个begin
    val timeoutOrderId: Long = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

/**
 * 自定义匹配上的事件序列 处理函数 即正常支付
 */
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    // 匹配上了 begin follow都行
    val payedOrderId: Long = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId, "payed successfully")
  }
}
