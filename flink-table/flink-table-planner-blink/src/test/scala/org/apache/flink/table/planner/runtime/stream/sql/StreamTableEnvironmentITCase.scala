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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.JavaPojos.Device
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, StringSink}
import org.junit.Assert.assertEquals
import org.junit.Test

import java.util.Collections


/**
  * Integration tests for methods on [[org.apache.flink.table.api.scala.StreamTableEnvironment]].
  */
class StreamTableEnvironmentITCase extends StreamingTestBase {

  @Test
  def testToAppendStreamWithRawType(): Unit = {
    val devices = env.fromCollection(Seq(
      new Device(1L, "device1", Collections.singletonMap("A", 10)),
      new Device(2L, "device2", Collections.emptyMap()),
      new Device(3L, "device3", Collections.singletonMap("B", 20))
    ))

    // register DataStream as Table
    tEnv.createTemporaryView("devices", devices,'deviceId, 'deviceName, 'metrics)

    val result = tEnv.sqlQuery("SELECT * FROM devices WHERE deviceId >= 2")
    val sink = new StringSink[Device]()
    result.toAppendStream[Device].addSink(sink)

    env.execute()

    val expected = List(
      "Device{deviceId=2, deviceName='device2', metrics={}}",
      "Device{deviceId=3, deviceName='device3', metrics={B=20}}")
    assertEquals(expected.sorted, sink.getResults.sorted)
  }
}