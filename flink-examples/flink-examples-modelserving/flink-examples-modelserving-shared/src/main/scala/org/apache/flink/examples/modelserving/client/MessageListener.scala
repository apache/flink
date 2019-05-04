/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.modelserving.client

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

/**
  * kafka message listener
  */
object MessageListener {
  private val AUTOCOMMITINTERVAL = "1000" // Frequency off offset commits
  private val SESSIONTIMEOUT = "30000"    // The timeout used to detect failures
  private val MAXPOLLRECORDS = "10"       // Max number of records consumed in a single poll

  /**
    * Creates a Properties instance for Kafka customized with values passed in argument.
    *
    * @param brokers  kafka brokers.
    * @param keyDeserealizer key serializer.
    * @param valueDeserealizer value serializer.
    * @return kafka properties
    */
  def consumerProperties(brokers: String, group: String, keyDeserealizer: String,
                         valueDeserealizer: String): Map[String, String] = {
    Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> AUTOCOMMITINTERVAL,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> SESSIONTIMEOUT,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> MAXPOLLRECORDS,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> keyDeserealizer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> valueDeserealizer
    )
  }

  /**
    * Creates Message listener.
    *
    * @param brokers  kafka brokers.
    * @param topic topic.
    * @param group group.
    * @param processor record processor.
    * @return message listener.
    */
  def apply[K, V](brokers: String, topic: String, group: String,
                  processor: RecordProcessorTrait[K, V]): MessageListener[K, V] =
    new MessageListener[K, V](brokers, topic, group, classOf[ByteArrayDeserializer].getName,
      classOf[ByteArrayDeserializer].getName, processor)
}

/**
  * kafka message listener
  */
class MessageListener[K, V](brokers: String, topic: String, group: String,
                            keyDeserealizer: String, valueDeserealizer: String,
                            processor: RecordProcessorTrait[K, V]) extends Runnable {

  import MessageListener._

  import scala.collection.JavaConversions._

  val consumer = new KafkaConsumer[K, V](consumerProperties(brokers, group, keyDeserealizer,
    valueDeserealizer))
  consumer.subscribe(Seq(topic))
  var completed = false

  /**
    * Stop kafka message listener
    */
  def complete(): Unit = {
    completed = true
  }

  /**
    * Run kafka message listener
    */
  override def run(): Unit = {
    while (!completed) {
      val records = consumer.poll(100)
      for (record <- records) {
        processor.processRecord(record)
      }
    }
    consumer.close()
    System.out.println("Listener completes")
  }

  def start(): Unit = {
    val t = new Thread(this)
    t.start()
  }
}
