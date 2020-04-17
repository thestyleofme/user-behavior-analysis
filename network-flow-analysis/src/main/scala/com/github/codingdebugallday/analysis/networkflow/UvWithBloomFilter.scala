package com.github.codingdebugallday.analysis.networkflow

import java.net.URI

import com.github.codingdebugallday.analysis.common.pojos.UserBehavior
import com.github.codingdebugallday.analysis.networkflow.pojos.UvCount
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/18 1:12
 * @since 1.0
 */
//noinspection DuplicatedCode
object UvWithBloomFilter {

  private val log = Logger(LoggerFactory.getLogger(UvWithBloomFilter.getClass))

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
      "E:/myGitCode/user-behavior-analysis/server_log/UserBehavior1.csv")
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
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())

    dataStream.print("uv count")

    env.execute("unique visitor job")
  }
}

/**
 * 自定义窗口触发器
 */
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
  }

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 每来一条数据，就触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

/**
 * 定义一个bloom filter
 */
class Bloom(size: Long) extends Serializable {
  // 位图的总大小
  private val cap = if (size > 0) {
    size
  } else {
    // 16M = 2^10*2^10*16*8 bit
    1 << 27
  }

  // 定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }

}

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  //  redis://user:password@host:port/db
  //  lazy val jedis: Jedis = new Jedis(new URI("redis://:redis!qaz@hdspdev008:26379/15"), 15000, 15000)
  lazy val jedis: Jedis = new Jedis(new URI("redis://localhost:6379/15"), 15000, 15000)
  // 64M 位图大小布隆过滤器
  //  lazy val bloom: Bloom = new Bloom(1 << 29)
  lazy val bloom: Bloom = new Bloom(1 << 23)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图在redis储存格式 key->windowEnd value->bitmap
    val storeKey: String = context.window.getEnd.toString
    // 把每个窗口的uv count值也存入名为count的redis表，表存放内容为（windowEnd -> uvCount）
    val countStr: String = jedis.hget("uv_count", storeKey)
    val count: Long = if (countStr != null) {
      countStr.toLong
    } else {
      0L
    }
    // 用bloom filter判断当前userId是否已经存在
    for (o <- elements) {
      val userId: String = o.toString()
      // 多次hash判断redis位图有没有这些位 seed应该是质数 这样分布均匀点
      val hash1: Long = bloom.hash(userId, 61)
      val hash2: Long = bloom.hash(userId, 71)
      val hash3: Long = bloom.hash(userId, 83)
      if (!jedis.getbit(storeKey, hash1) &&
        !jedis.getbit(storeKey, hash2) &&
        !jedis.getbit(storeKey, hash3)) {
        // 如果不存在，设置对应位置为1，count也加1
        jedis.setbit(storeKey, hash1, true)
        jedis.setbit(storeKey, hash2, true)
        jedis.setbit(storeKey, hash3, true)
        jedis.hset("uv_count", storeKey, (count + 1).toString)
        // 输出
        out.collect(UvCount(storeKey.toLong, count + 1))
      } else {
        // 输出
        out.collect(UvCount(storeKey.toLong, count))
      }
    }
  }
}
