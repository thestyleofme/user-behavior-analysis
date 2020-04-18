package com.github.codingdebugallday.analysis.market.source

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.github.codingdebugallday.analysis.market.pojos.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/19 1:27
 * @since 1.0
 */
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  /**
   * 标识数据源是否正常运行
   */
  private var running: Boolean = true
  /**
   * 定义用户行为的集合
   */
  private val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  /**
   * 定义渠道的集合
   */
  private val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  private val random: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElementNumber: Long = Long.MaxValue
    var count = 0L
    while (running && count < maxElementNumber) {
      val userId: String = UUID.randomUUID().toString
      val behavior: String = behaviorTypes(random.nextInt(behaviorTypes.size))
      val channel: String = channelSets(random.nextInt(channelSets.size))
      val ts: Long = System.currentTimeMillis()
      ctx.collect(MarketingUserBehavior(userId, behavior, channel, ts))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}
