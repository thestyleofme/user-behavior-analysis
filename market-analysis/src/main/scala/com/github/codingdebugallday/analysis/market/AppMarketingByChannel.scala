package com.github.codingdebugallday.analysis.market

import java.sql.Timestamp

import com.github.codingdebugallday.analysis.market.pojos.MarketingViewCount
import com.github.codingdebugallday.analysis.market.source.SimulatedEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * <p>
 * APP推广总数实时统计
 * </p>
 *
 * @author isacc 2020/04/19 0:27
 * @since 1.0
 */
//noinspection DuplicatedCode
object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      // 按渠道和行为分组
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())

    dataStream.print()

    env.execute("app marketing by channel job")
  }

}

/**
 * 自定义处理函数
 */
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    out.collect(MarketingViewCount(new Timestamp(context.window.getStart).toString,
      new Timestamp(context.window.getEnd).toString,
      key._1,
      key._2,
      elements.size))
  }
}

