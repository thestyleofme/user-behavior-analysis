package com.github.codingdebugallday.analysis.login

import java.util

import com.github.codingdebugallday.analysis.login.pojos.{LoginEvent, LoginWarning}
import com.typesafe.scalalogging.Logger
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.slf4j.LoggerFactory

/**
 * 连续登录失败检测
 */
//noinspection DuplicatedCode
object LoginFailWithCep {

  private val log = Logger(LoggerFactory.getLogger(LoginFailWithCep.getClass))

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
      .keyBy(_.userId)

    // 定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))
    // 应用模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)
    // select出匹配到的事件
    val loginFailDataStream: DataStream[LoginWarning] = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print("warning")

    env.execute("login fail with cep job")
  }

}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, LoginWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = {
    // 按照name取出对应事件
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    LoginWarning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")
  }
}
