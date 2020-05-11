package com.github.codingdebugallday.analysis.orderpay

import com.github.codingdebugallday.analysis.orderpay.pojos.{OrderEvent, OrderResult}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

//noinspection DuplicatedCode
object OrderTimeoutWithoutCep {

  private val log = Logger(LoggerFactory.getLogger(OrderTimeoutWithoutCep.getClass))

  private val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

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

    // 定义process function进行超时检测
    //    val timeoutWarningStream: DataStream[OrderResult] = orderEventStream.process(new OrderTimeoutWarning())
    //    timeoutWarningStream.print()

    val orderResultStream: DataStream[OrderResult] = orderEventStream.process(new OrderPayMatch())
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute("order timeout without cep job")
  }

  /**
   * 所有的匹配处理
   */
  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    // 保存pay是否来过的状态
    lazy private val isPayedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))
    // 保存定时器的时间戳
    lazy private val timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerState", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
      val isPayed: Boolean = isPayedState.value()
      val timerTs: Long = timerState.value()
      if (value.eventType == "create") {
        // 如果是create，接下来判断书否有pay来过
        if (isPayed) {
          // 如果已经来过，匹配成功，输出主流，清空状态
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 如果没有pay 注册定时器 等待pay到来
          val ts: Long = timerTs + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay") {
        // 如果是pay事件，先判断是否create过，用timer判断
        if (timerTs > 0) {
          // 有定时器，说明已经有create过
          // 继续判断，是否超过了timeout时间
          if (timerTs >= value.eventTime) {
            // 表示定时器时间还没到，pay就来了，成功匹配
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            // 表示超时了
            ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but already timeout"))
          }
          // 输出状态 清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // 没有create 直接先来了pay，先更新状态，注册定时器等待create
          // 相当于乱序了，这里直接注册当前的eventTime，等watermask触发
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime)
          timerState.update(value.eventTime)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 根据状态的值，判断哪个数据没来
      if (isPayedState.value()) {
        // 如果true 表示pay先到了，没等到create
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "already payed buy not found create"))
      } else {
        // 表示create了，没有pay
        ctx.output(orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }
  }

}

/**
 * 简单处理
 */
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 保存pay是否来过的状态
  lazy private val isPayedState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    val isPayed: Boolean = isPayedState.value()
    if (value.eventType == "create" && !isPayed) {
      // 如果遇到了create事件，并且没有来过，注册定时器开始等待
      ctx.timerService().registerEventTimeTimer(value.eventTime + 15 * 60 * 1000L)
    } else if (value.eventType == "pay") {
      // 如果是pay事件，直接把状态改为true
      isPayedState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    // 直接判断isPayed是否为true
    val isPayed: Boolean = isPayedState.value()
    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed timeout"))
    }
    // 清空状态
    isPayedState.clear()
  }

}
