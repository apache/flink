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

package org.apache.flink.table.api.stream

import java.math.BigDecimal

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.stream.TimeAttributesITCase.TimestampWithEqualWatermark
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class StreamTableEnvironmentValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testInvalidTimeAttributes(): Unit = {
    val util = streamTestUtil()
    // table definition makes no sense
    util.addTable[(Long, Int, String, Int, Long)]('a.rowtime.rowtime, 'b, 'c, 'd, 'e)
  }

  @Test(expected = classOf[TableException])
  def testInvalidProctimeAttribute(): Unit = {
    val util = streamTestUtil()
    // cannot replace an attribute with proctime
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b.proctime, 'c, 'd, 'e)
  }

  @Test(expected = classOf[TableException])
  def testRowtimeAttributeReplaceFieldOfInvalidType(): Unit = {
    val util = streamTestUtil()
    // cannot replace a non-time attribute with rowtime
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c.rowtime, 'd, 'e)
  }

  @Test(expected = classOf[TableException])
  def testRowtimeAndInvalidProctimeAttribute(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('rt.rowtime, 'b, 'c, 'd, 'pt.proctime)
  }

  @Test(expected = classOf[TableException])
  def testOnlyOneRowtimeAttribute1(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a.rowtime, 'b, 'c, 'd, 'e, 'rt.rowtime)
  }

  @Test(expected = classOf[TableException])
  def testOnlyOneProctimeAttribute1(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'pt1.proctime, 'pt2.proctime)
  }

  @Test(expected = classOf[TableException])
  def testRowtimeAttributeUsedName(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'a.rowtime)
  }

  @Test(expected = classOf[TableException])
  def testProctimeAttributeUsedName(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'b.proctime)
  }

  @Test(expected = classOf[TableException])
  def testAsWithToManyFields(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]('a, 'b, 'c, 'd)
  }

  @Test(expected = classOf[TableException])
  def testAsWithAmbiguousFields(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]('a, 'b, 'b)
  }

  @Test(expected = classOf[TableException])
  def testOnlyFieldRefInAs(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]('a, 'b as 'c, 'd)
  }

  @Test(expected = classOf[TableException])
  def testInvalidTimeCharacteristic(): Unit = {
    val data = List((1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
  }
}
