package com.github.codingdebugallday.analysis.market

import java.sql.Timestamp

import com.github.codingdebugallday.analysis.market.pojos.MarketingViewCount
import com.github.codingdebugallday.analysis.market.source.SimulatedEventSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * <p>
 * APP推广渠道实时统计
 * </p>
 *
 * @author isacc 2020/04/19 0:27
 * @since 1.0
 */
//noinspection DuplicatedCode
object AppMarketing {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      // 按渠道和行为分组
      .map(_ => {
        ("dummyKey", 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new MarketingCountTotal())

    dataStream.print()

    env.execute("app marketing job")
  }

}

class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    out.collect(MarketingViewCount(new Timestamp(window.getStart).toString,
      new Timestamp(window.getEnd).toString,
      "app marketing",
      "total",
      input.iterator.next()))
  }
}

