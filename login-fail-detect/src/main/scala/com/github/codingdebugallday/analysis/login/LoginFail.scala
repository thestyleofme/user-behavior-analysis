package com.github.codingdebugallday.analysis.login

import java.{lang, util}

import com.github.codingdebugallday.analysis.login.pojos.{LoginEvent, LoginWarning}
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * 连续登录失败检测
 */
object LoginFail {

  private val log = Logger(LoggerFactory.getLogger(LoginFail.getClass))

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val sourceStream: DataStream[String] = env.readTextFile(
      getClass.getResource("/LoginLog.csv").getPath)
    val loginEventStream: DataStream[LoginEvent] = sourceStream
      .map(data => {
        try {
          val splits: Array[String] = data.split(",")
          LoginEvent(splits(0).trim.toLong, splits(1).trim, splits(2).trim, splits(3).trim.toLong * 1000L)
        } catch {
          case e: Exception =>
            log.error("dirty record, data: {}", data, e)
            null
        }
      })
      .filter(_ != null)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(500)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime
        }
      })

    val warningStream: DataStream[LoginWarning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarningProcess(2))

    warningStream.print("warning")

    env.execute("login fail detect job")
  }

}

class LoginWarningProcess(maxFailNumber: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning] {
  // 定义状态，保存2秒内所有的登录失败事件
  private lazy val loginFailListState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailListState", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
    /*val loginFailList: lang.Iterable[LoginEvent] = loginFailListState.get()
    // 只添加fail的事件到状态
    if (value.eventType == "fail") {
      // 第一次fail，需要注册定时器
      if (!loginFailList.iterator().hasNext) {
        // 2秒后出发
        ctx.timerService().registerEventTimeTimer(value.eventTime + 2000)
      }
      loginFailListState.add(value)
    } else {
      // 成功则清除状态
      loginFailListState.clear()
    }*/
    if (value.eventType == "fail") {
      // 如果失败，判断之前是否有登录失败事件
      val iterator: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
      if (iterator.hasNext) {
        // 如果之前已经有登录失败事件，就比较事件时间
        val lastLoginEvent: LoginEvent = iterator.next()
        // todo 需考虑数据乱序的问题（应结合watermask）以及登录失败次数 这里是2次 若是超过2呢
        if (value.eventTime - lastLoginEvent.eventTime >= 2000) {
          // 如果两次间隔小于2s，输出报警
          out.collect(LoginWarning(value.userId, lastLoginEvent.eventTime, value.eventTime, "login fail in 2s"))
        }
        // 更新最近一次的登录失败事件，保存到状态中
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 之前没有登录失败事件即第一次登录失败，直接添加到状态
        loginFailListState.add(value)
      }
    } else {
      // 成功则清除状态
      loginFailListState.clear()
    }
  }

  /*override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#OnTimerContext, out: Collector[LoginWarning]): Unit = {
    // 根据状态里的失败个数决定是否触发报警
    var allLoginFails = new ListBuffer[LoginEvent]()
    val iterator: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
    while (iterator.hasNext) {
      allLoginFails += iterator.next()
    }
    if (allLoginFails.length >= maxFailNumber) {
      out.collect(LoginWarning(ctx.getCurrentKey,
        allLoginFails.head.eventTime,
        allLoginFails.last.eventTime,
        s"login fail in 2s for ${allLoginFails.length} times"))
    }
    // 最后清空状态
    loginFailListState.clear()
  }*/
}
