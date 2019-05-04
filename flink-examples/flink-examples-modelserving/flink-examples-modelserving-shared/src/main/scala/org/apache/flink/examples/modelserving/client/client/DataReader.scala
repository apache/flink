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

package org.apache.flink.examples.modelserving.client.client

import org.apache.flink.examples.modelserving.client.MessageListener
import org.apache.flink.examples.modelserving.configuration.ModelServingConfiguration

/**
  * Simple appliv==cation to validate thet messages are written to kafka
  */
object DataReader {

  import ModelServingConfiguration._

  def main(args: Array[String]) {

    println(s"Using kafka brokers at ${LOCAL_KAFKA_BROKER}")

    val listener =
      MessageListener(LOCAL_KAFKA_BROKER, DATA_TOPIC, DATA_GROUP, new RecordProcessor())
    listener.start()
  }
}
