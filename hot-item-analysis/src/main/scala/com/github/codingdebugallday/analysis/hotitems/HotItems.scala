package com.github.codingdebugallday.analysis.hotitems

import java.sql.Timestamp
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.github.codingdebugallday.analysis.common.pojos.UserBehavior
import com.github.codingdebugallday.analysis.hotitems.pojos.ItemViewCount
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * <p>
 * 需求：近1个小时内的热门商品topN，每隔5分钟输出一次
 * </p>
 *
 * @author isacc 2020/04/13 15:33
 * @since 1.0
 */
object HotItems {

  private val log = Logger(LoggerFactory.getLogger(HotItems.getClass))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // source
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hdspdev001:6667,hdspdev002:6667,hdspdev003:6667")
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(
      "hotitems",
      new SimpleStringSchema(),
      properties)
    kafkaConsumer.setStartFromEarliest()
    //    kafkaConsumer.setStartFromLatest()
    val sourceStream: DataStream[String] = env.addSource(kafkaConsumer)
    //    val sourceStream: DataStream[String] = env.readTextFile(
    //      "E:/myGitCode/user-behavior-analysis/server_log/UserBehavior1.csv")
    val dataStream: DataStream[UserBehavior] = sourceStream
      .map(data => {
        try {
          val splits: Array[String] = data.split(",")
          UserBehavior(splits(0).trim.toLong, splits(1).trim.toLong, splits(2).trim.toInt, splits(3).trim, splits(4).trim.toLong * 1000L)
        } catch {
          case e: Exception =>
            log.error("dirty record, data: {}", data, e)
            null
        }
      })
      .filter(_ != null)
      .filter(_.behavior.equalsIgnoreCase("pv"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(5)) {
        override def extractTimestamp(element: UserBehavior): Long = {
          element.timestamp
        }
      })
    // transform
    // 看热门商品 只关注pv
    val processedStream: DataStream[String] = dataStream
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //      .allowedLateness(Time.minutes(5))
      //      .sideOutputLateData(new OutputTag[UserBehavior]("latenessItem"))
      .aggregate(new CountAgg(), new WindowResultAgg()) // 窗口聚合
      .keyBy(_.windowEnd) // 按照窗口分组
      .process(new TopNHotItems(5))

    // sink
    processedStream.print()

    env.execute("hot items job")
  }

}

/**
 * 自定义预聚合函数
 */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
 * 自定义预聚合函数计算平均数的实例，跟代码逻辑无关
 */
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  // (Long, Int) long代表时间戳的总值，int代码个数
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = {
    (accumulator._1 + value.timestamp, accumulator._2 + 1)
  }

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

/**
 * 自定义窗口函数，输出ItemViewCount
 */
class WindowResultAgg() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor("itemState", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
    itemState.add(value)
    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，对所有数据进行排序并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 将所有state中的数据取出，放到一个ListBuffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    val iterator: util.Iterator[ItemViewCount] = itemState.get().iterator()
    while (iterator.hasNext) {
      allItems += iterator.next()
    }
    // 按照count大小降序排列并取前N个
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortWith(_.count > _.count).take(topSize)
    //    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    // 清空状态
    itemState.clear()
    // topN 输出
    val stringBuilder = new mutable.StringBuilder()
    stringBuilder.append("current time: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      stringBuilder.append("No").append(i + 1).append(":")
        .append(" itemId=").append(currentItem.itemId)
        .append(" pv=").append(currentItem.count)
        .append("\n")
    }
    stringBuilder.append("===========================================")
    // 控制输出频率
    TimeUnit.SECONDS.sleep(1)
    out.collect(stringBuilder.toString())
  }
}

