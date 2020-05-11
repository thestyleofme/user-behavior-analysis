package com.github.codingdebugallday.analysis.orderpay

import com.github.codingdebugallday.analysis.orderpay.pojos.{OrderEvent, ReceiptEvent}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * 两条流实时对账
 */
//noinspection DuplicatedCode
object TxMatchDetect {

  private val log = Logger(LoggerFactory.getLogger(TxMatchDetect.getClass))
  // 订单流中有 到账流中无
  private val unmatchedPay = new OutputTag[OrderEvent]("unmatchedPay")
  // 到账流中有 订单流中无
  private val unmatchedReceipt = new OutputTag[ReceiptEvent]("unmatchedReceipt")

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

    // 将两条流连接起来
    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .connect(receiptEventStream)
      .process(new TxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPay).print("unmatchedPay")
    processedStream.getSideOutput(unmatchedReceipt).print("unmatchedReceipt")

    env.execute("tx match job")
  }

  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

    // 定义状态来保存已经到达的订单支付事件和到账事件
    lazy private val payState = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payState", classOf[OrderEvent]))
    lazy private val receiptState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receiptState", classOf[ReceiptEvent]))

    // 订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断有没有对应的到账事件
      val receipt: ReceiptEvent = receiptState.value()
      if (receipt != null) {
        // 如果已经有receipt，在主流中输出 清空到账状态
        out.collect((pay, receipt))
        receiptState.clear()
      } else {
        // 如果还没到，那么把pay存入状态，并且注册一个定时器等待5s
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime + 5 * 1000L)
      }
    }

    // 到账事件的处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 同样的处理流程
      val pay: OrderEvent = payState.value()
      if (pay != null) {
        out.collect(pay, receipt)
        payState.clear()
      } else {
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime + 5 * 1000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 到时间了，如果还没收到某个事件，就输出报警
      val pay: OrderEvent = payState.value()
      if (pay != null) {
        // 说明receipt没来，输出pay到侧输出流
        ctx.output(unmatchedPay, pay)
      }
      val receipt: ReceiptEvent = receiptState.value()
      if (receipt != null) {
        // 说明pay没来，输出receipt到侧输出流
        ctx.output(unmatchedReceipt, receipt)
      }
      // 情况状态
      payState.clear()
      receiptState.clear()
    }
  }

}
