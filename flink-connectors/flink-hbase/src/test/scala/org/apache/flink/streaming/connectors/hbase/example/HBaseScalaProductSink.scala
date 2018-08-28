/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.hbase.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.hbase.{HBaseSink, HBaseTableMapper}

import scala.collection.mutable.ArrayBuffer

/**
  * This is an example showing the to use the Scala Product HBase Sink in the Streaming API.
  *
  * <p>The example assumes that a table exists in a local hbase database.
  */
object HBaseScalaProductSink {
  val TEST_TABLE = "testTable"
  case class TestCaseClass(key: String, value: Int)

  val collection = new ArrayBuffer[TestCaseClass](20)

  for (i <- 0 until 20) {
    collection += TestCaseClass("hbase-" + i, i)
  }

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val source = env.fromCollection(collection)

    HBaseSink.addSink(source)
      .setClusterKey("127.0.0.1:2181:/hbase")
      .setTableMapper(
        new HBaseTableMapper()
          .addMapping(0, "CF1", "key", classOf[String])
          .addMapping(1, "CF2", "value", classOf[Integer])
          .setRowKey(0, classOf[String]))
      .setTableName(TEST_TABLE)
      .build

    env.execute("HBase Sink example")
  }
}
