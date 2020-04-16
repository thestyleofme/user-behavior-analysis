package com.github.codingdebugallday.analysis.networkflow

import com.github.codingdebugallday.analysis.common.pojos.UserBehavior
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/16 23:41
 * @since 1.0
 */
object PageView {

  private val log = Logger(LoggerFactory.getLogger(PageView.getClass))

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
    val dataStream: DataStream[(String, Int)] = sourceStream
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
      .map(_ => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")

    env.execute("page view job")

  }

}
