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

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord,
  RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer

/**
  * Kafka message sender object.
  */
object MessageSender {

  private val ACKCONFIGURATION = "all" // Blocking on the full commit of the record
  private val RETRYCOUNT = "1" // Number of retries on put
  private val BATCHSIZE = "1024"  // Buffers for unsent records for each partition -
                                  // controlls batching
  private val LINGERTIME = "1" // Timeout for more records to arive - controlls batching
  private val BUFFERMEMORY = "1024000" // Controls the total amount of memory available to
  // the producer for buffering. If records are sent faster than they can be transmitted to
  // the server then this buffer space will be exhausted. When the buffer space is exhausted
  // additional send calls will block. The threshold for time to block is determined by
  // max.block.ms after which it throws a TimeoutException.

  /**
    * Creates a Properties instance for Kafka customized with values passed in argument.
    *
    * @param brokers  kafka brokers.
    * @param keySerializer key serializer class.
    * @param valueSerializer value serializer class.
    * @return kafka properties
    */
  def providerProperties(brokers: String, keySerializer: String, valueSerializer: String):
  Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.ACKS_CONFIG, ACKCONFIGURATION)
    props.put(ProducerConfig.RETRIES_CONFIG, RETRYCOUNT)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, BATCHSIZE)
    props.put(ProducerConfig.LINGER_MS_CONFIG, LINGERTIME)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, BUFFERMEMORY)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer)
    props
  }

  /**
    * Creates Kafka message sender.
    *
    * @param brokers  kafka brokers.
    * @return kafka message sender
    */
  def apply(brokers: String): MessageSender =
    new MessageSender(brokers)
}

/**
  * Kafka message sender object.
  */
class MessageSender(val brokers: String) {

  import MessageSender._
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](
    providerProperties(brokers, classOf[ByteArraySerializer].getName,
      classOf[ByteArraySerializer].getName))

  /**
    * Write key/value message to Kafka.
    *
    * @param topic  kafka topic.
    * @param key  key value.
    * @param value  value message.
    */
  def writeKeyValue(topic: String, key: Array[Byte], value: Array[Byte]): Unit = {
    val result = producer.send(
      new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value)).get
    producer.flush()
  }

  /**
    * Write value message to Kafka.
    *
    * @param topic  kafka topic.
    * @param value  value message.
    */
  def writeValue(topic: String, value: Array[Byte]): Unit = {
    val result = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic,
      null.asInstanceOf[Array[Byte]], value)).get
    producer.flush()
  }

  /**
    * Batch write value message to Kafka.
    *
    * @param topic  kafka topic.
    * @param batch  batch of value message.
    */
  def batchWriteValue(topic: String, batch: Seq[Array[Byte]]): Seq[RecordMetadata] = {
    val result = batch.map(value =>
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic,
        null.asInstanceOf[Array[Byte]], value)).get)
    producer.flush()
    result
  }

  /**
    * Close Kafka message sender.
    *
    */
  def close(): Unit = {
    producer.close()
  }
}
