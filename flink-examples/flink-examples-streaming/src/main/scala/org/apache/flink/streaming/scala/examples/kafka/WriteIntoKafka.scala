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

package org.apache.flink.streaming.scala.examples.kafka

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * Generate a String every 500 ms and write it into a Kafka topic
 *
 * Please pass the following arguments to run the example:
 * {{{
 * --topic test
 * --bootstrap.servers
 * localhost:9092
 * }}}
 */
object WriteIntoKafka {

  def main(args: Array[String]): Unit = {

    // parse input arguments
    val params = ParameterTool.fromArgs(args)

    if (params.getNumberOfParameters < 2) {
      println("Missing parameters!")
      println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>")
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))

    // very simple data generator
    val messageStream: DataStream[String] = env.addSource(new SourceFunction[String]() {
      var running = true

      override def run(ctx: SourceContext[String]): Unit = {
        var i = 0L
        while (this.running) {
          ctx.collect(s"Element - ${i}")
          i += 1
          Thread.sleep(500)
        }
      }

      override def cancel(): Unit = running = false
    })

    // create a Kafka producer for Kafka 0.8.x
    val kafkaProducer = new FlinkKafkaProducer08(
      params.getRequired("topic"),
      new SimpleStringSchema,
      params.getProperties)

    // write data into Kafka
    messageStream.addSink(kafkaProducer)

    env.execute("Write into Kafka example")
  }

}
