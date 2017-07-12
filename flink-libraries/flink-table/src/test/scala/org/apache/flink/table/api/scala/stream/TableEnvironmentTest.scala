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

package org.apache.flink.table.api.scala.stream

import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.java.tuple.{Tuple5 => JTuple5}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JStreamExecEnv}
import org.apache.flink.table.api.java.{StreamTableEnvironment => JStreamTableEnv}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test
import org.mockito.Mockito.{mock, when}

class TableEnvironmentTest extends TableTestBase {

  @Test
  def testProctimeAttribute(): Unit = {
    val util = streamTestUtil()
    // cannot replace an attribute with proctime
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'pt.proctime)
  }

  @Test
  def testReplacedRowtimeAttribute(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('rt.rowtime, 'b, 'c, 'd, 'e)
  }

  @Test
  def testAppendedRowtimeAttribute(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rt.rowtime)
  }

  @Test
  def testRowtimeAndProctimeAttribute1(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'rt.rowtime, 'pt.proctime)
  }

  @Test
  def testRowtimeAndProctimeAttribute2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'pt.proctime, 'rt.rowtime)
  }

  @Test
  def testRowtimeAndProctimeAttribute3(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('rt.rowtime, 'b, 'c, 'd, 'e, 'pt.proctime)
  }

  @Test
  def testProctimeAttributeParsed(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a, b, c, d, e, pt.proctime")
  }

  @Test
  def testReplacingRowtimeAttributeParsed(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a.rowtime, b, c, d, e")
  }

  @Test
  def testAppedingRowtimeAttributeParsed(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a, b, c, d, e, rt.rowtime")
  }

  @Test
  def testRowtimeAndProctimeAttributeParsed1(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "a, b, c, d, e, pt.proctime, rt.rowtime")
  }

  @Test
  def testRowtimeAndProctimeAttributeParsed2(): Unit = {
    val (jTEnv, ds) = prepareSchemaExpressionParser
    jTEnv.fromDataStream(ds, "rt.rowtime, b, c, d, e, pt.proctime")
  }

  private def prepareSchemaExpressionParser:
    (JStreamTableEnv, DataStream[JTuple5[JLong, JInt, String, JInt, JLong]]) = {

    val jStreamExecEnv = mock(classOf[JStreamExecEnv])
    when(jStreamExecEnv.getStreamTimeCharacteristic).thenReturn(TimeCharacteristic.EventTime)
    val jTEnv = TableEnvironment.getTableEnvironment(jStreamExecEnv)

    val sType = new TupleTypeInfo(Types.LONG, Types.INT, Types.STRING, Types.INT, Types.LONG)
      .asInstanceOf[TupleTypeInfo[JTuple5[JLong, JInt, String, JInt, JLong]]]
    val ds = mock(classOf[DataStream[JTuple5[JLong, JInt, String, JInt, JLong]]])
    when(ds.getType).thenReturn(sType)

    (jTEnv, ds)
  }

}
