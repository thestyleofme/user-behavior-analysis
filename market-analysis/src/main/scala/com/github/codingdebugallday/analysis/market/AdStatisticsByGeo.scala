package com.github.codingdebugallday.analysis.market

import java.sql.Timestamp

import com.github.codingdebugallday.analysis.market.pojos.{AdClickEvent, AdCountByProvince, BlackListWarning}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * 带黑名单的用户广告点击统计
 */
object AdStatisticsByGeo {

  private val log = Logger(LoggerFactory.getLogger(AdStatisticsByGeo.getClass))

  private val blackListOutputTag = new OutputTag[BlackListWarning]("blackListOutputTag")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val sourceStream: DataStream[String] = env.readTextFile(
      getClass.getResource("/AdClickLog.csv").getPath)
    val adEventStream: DataStream[AdClickEvent] = sourceStream
      .map(data => {
        try {
          val splits: Array[String] = data.split(",")
          AdClickEvent(splits(0).trim.toLong, splits(1).trim.toLong, splits(2).trim, splits(3).trim, splits(4).trim.toLong * 1000L)
        } catch {
          case e: Exception =>
            log.error("dirty record, data: {}", data, e)
            null
        }
      })
      .filter(_ != null)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdClickEvent](Time.seconds(1)) {
        override def extractTimestamp(element: AdClickEvent): Long = {
          element.timestamp
        }
      })
    // 自定义process function 过滤掉刷点击量的行为
    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(15))

    // 按照省份开窗聚合
    val adCountStream: DataStream[AdCountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.minutes(10))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("ad click count")
    filterBlackListStream.getSideOutput(blackListOutputTag).print("black list")

    env.execute("ad statistics by geo job")
  }

  class FilterBlackListUser(maxClickNumber: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    /**
     * 当前用户对当前广告的点击量
     */
    lazy private val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
    /**
     * 是否发送过黑名单的状态
     */
    lazy private val isSentBlackList = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSentBlackList", classOf[Boolean]))
    /**
     * 定时器触发的时间戳
     */
    lazy private val resetTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimer", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val curCount: Long = countState.value()
      // 如果是第一次处理，注册定时器，每天00:00触发
      if (curCount == 0) {
        val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }
      // 判断点击是否达到上限，达到则加入黑名单
      if (curCount >= maxClickNumber) {
        // 判断是否发送过黑名单，只发送一次
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, s"click over $maxClickNumber times today."))
        }
      } else {
        // 计数状态加1
        countState.update(curCount + 1)
        // 正常输出到主流
        out.collect(value)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      // 定时器触发时，清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        resetTimer.clear()
        countState.clear()
      }
    }
  }

}

class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = {
    accumulator + 1
  }

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    out.collect(AdCountByProvince(new Timestamp(window.getStart).toString,
      new Timestamp(window.getEnd).toString, key, input.iterator.next()))
  }
}


