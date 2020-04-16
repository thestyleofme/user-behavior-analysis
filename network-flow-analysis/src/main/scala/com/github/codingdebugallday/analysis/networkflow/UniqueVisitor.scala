package com.github.codingdebugallday.analysis.networkflow

import com.github.codingdebugallday.analysis.common.pojos.UserBehavior
import com.github.codingdebugallday.analysis.networkflow.pojos.UvCount
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/17 0:26
 * @since 1.0
 */
//noinspection DuplicatedCode
object UniqueVisitor {

  private val log = Logger(LoggerFactory.getLogger(UniqueVisitor.getClass))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // source
    //    val properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "hdspdev001:6667,hdspdev002:6667,hdspdev003:6667")
    //    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(
    //      "hotitems",
    //      new SimpleStringSchema(),
    //      properties)
    //    kafkaConsumer.setStartFromEarliest()
    //    //    kafkaConsumer.setStartFromLatest()
    //    val sourceStream: DataStream[String] = env.addSource(kafkaConsumer)
    val sourceStream: DataStream[String] = env.readTextFile(
      "E:/myGitCode/user-behavior-analysis/server_log/UserBehavior.csv")
    val dataStream: DataStream[UvCount] = sourceStream
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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(5)) {
        override def extractTimestamp(element: UserBehavior): Long = {
          element.timestamp
        }
      })
      .filter(_.behavior.equalsIgnoreCase("pv"))
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print("uv count")

    env.execute("unique visitor job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {

  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    // 也可redis set去重 或者 布隆过滤器（推荐）
    var userIdSet: Set[Long] = Set[Long]()
    // 把当前窗口的所有数据的userId放入set中，最后输出set的大小
    for (userBehavior <- input) {
      userIdSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd, userIdSet.size))
  }
}

