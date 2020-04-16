package com.github.codingdebugallday.analysis.hotitems

import java.util.Properties

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.io.BufferedSource

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/04/15 15:06
 * @since 1.0
 */
object File2KafkaUtil {

  private val log = Logger(LoggerFactory.getLogger(File2KafkaUtil.getClass))

  def main(args: Array[String]): Unit = {
    val brokers = "hdspdev001:6667,hdspdev002:6667,hdspdev003:6667"
    val topic = "hotitems"
    val filePath = "E:/myGitCode/user-behavior-analysis/server_log/UserBehavior.csv"
    file2Kafka(brokers, topic, filePath)
  }

  def file2Kafka(brokers: String, topic: String, filePath: String): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val kafkaProducer = new KafkaProducer[String, String](props)
    // 从文件中读取数据，推送到kafka
    val bufferedSource: BufferedSource = io.Source.fromFile(filePath)
    for (line <- bufferedSource.getLines()) {
      val producerRecord = new ProducerRecord[String, String](topic, line)
      log.info("send to kafka, data: {}", line)
      kafkaProducer.send(producerRecord)
    }
    kafkaProducer.close()
  }

}
