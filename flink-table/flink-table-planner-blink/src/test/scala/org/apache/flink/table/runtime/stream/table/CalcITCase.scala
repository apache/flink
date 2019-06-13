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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.types.Row

import scala.collection.mutable
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class CalcITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testColumnOperation(): Unit = {

    val data = List(
      (1, 1L, "Kevin"),
      (2, 2L, "Sunny"))

    val stream = failingDataSource(data)
    val t = stream.toTable(tEnv, 'a, 'b, 'c)

    val result = t
      // Adds column without alias
      .addColumns('a + 2)
      // Renames columns
      .renameColumns('a as 'a2, 'b as 'b2)
      .renameColumns("c as c2")
      // Drops columns
      .dropColumns('b2)
      .dropColumns("c2")

    val sink = new TestingAppendSink
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,3",
      "2,4"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}
