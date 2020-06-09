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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.runtime.stream.TimeAttributesITCase.TimestampWithEqualWatermark
import org.apache.flink.table.utils.TableTestBase

import org.junit.Test

import java.math.BigDecimal

class StreamTableEnvironmentValidationTest extends TableTestBase {

  // ----------------------------------------------------------------------------------------------
  // schema definition by position
  // ----------------------------------------------------------------------------------------------

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtimeAliasByPosition(): Unit = {
    val util = streamTestUtil()
    // don't allow aliasing by position
    util.addTable[(Long, Int, String, Int, Long)]('a.rowtime as 'b, 'b, 'c, 'd, 'e)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidRowtimeAttributesByPosition(): Unit = {
    val util = streamTestUtil()
    // table definition makes no sense
    util.addTable[(Long, Int, String, Int, Long)]('a.rowtime.rowtime, 'b, 'c, 'd, 'e)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidProctimeAttributesByPosition(): Unit = {
    val util = streamTestUtil()
    // table definition makes no sense
    util.addTable[(Long, Int, String, Int, Long)]('a.proctime.proctime, 'b, 'c, 'd, 'e)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidTimeAttributesByPosition(): Unit = {
    val util = streamTestUtil()
    // table definition makes no sense
    util.addTable[(Long, Int, String, Int, Long)]('a.rowtime.rowtime, 'b, 'c, 'd, 'e)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidProctimeAttributeByPosition(): Unit = {
    val util = streamTestUtil()
    // cannot replace an attribute with proctime
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b.proctime, 'c, 'd, 'e)
  }

  @Test(expected = classOf[ValidationException])
  def testRowtimeAttributeReplaceFieldOfInvalidTypeByPosition(): Unit = {
    val util = streamTestUtil()
    // cannot replace a non-time attribute with rowtime
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c.rowtime, 'd, 'e)
  }

  @Test(expected = classOf[ValidationException])
  def testRowtimeAndInvalidProctimeAttributeByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('rt.rowtime, 'b, 'c, 'd, 'pt.proctime)
  }

  @Test(expected = classOf[ValidationException])
  def testOnlyOneRowtimeAttribute1ByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a.rowtime, 'b, 'c, 'd, 'e, 'rt.rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testOnlyOneProctimeAttribute1ByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'pt1.proctime, 'pt2.proctime)
  }

  @Test(expected = classOf[ValidationException])
  def testRowtimeAttributeUsedNameByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'a.rowtime)
  }

  @Test(expected = classOf[ValidationException])
  def testProctimeAttributeUsedNameByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Long, Int, String, Int, Long)]('a, 'b, 'c, 'd, 'e, 'b.proctime)
  }

  @Test(expected = classOf[ValidationException])
  def testAsWithTooManyFieldsByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]('a, 'b, 'c, 'd)
  }

  @Test(expected = classOf[ValidationException])
  def testAsWithAmbiguousFieldsByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]('a, 'b, 'b)
  }

  @Test(expected = classOf[ValidationException])
  def testOnlyFieldRefInAsByPosition(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]('a, 'b as 'c, 'd)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidTimeCharacteristicByPosition(): Unit = {
    val data = List((1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"))
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampWithEqualWatermark())
    stream.toTable(tEnv, 'rowtime.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
  }

  // ----------------------------------------------------------------------------------------------
  // schema definition by name
  // ----------------------------------------------------------------------------------------------

  @Test(expected = classOf[ValidationException])
  def testInvalidAliasByName(): Unit = {
    val util = streamTestUtil()
    // we reference by name, but the field does not exist
    util.addTable[(Long, Int, String, Int, Long)]('x as 'r)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidFieldByName(): Unit = {
    val util = streamTestUtil()
    // we reference by name, but the field does not exist
    util.addTable[(Long, Int, String, Int, Long)]('x as 'r)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidField2ByName(): Unit = {
    val util = streamTestUtil()
    // we mix reference by position and by name
    util.addTable[(Long, Int, String, Int, Long)]('x, '_1)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidAliasWithProctimeAttribute(): Unit = {
    val util = streamTestUtil()
    // alias in proctime not allowed
    util.addTable[(Int, Long, String)]('_1, ('_2 as 'new).proctime, '_3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidReplacingProctimeAttribute(): Unit = {
    val util = streamTestUtil()
    // proctime must not replace an existing field
    util.addTable[(Int, Long, String)]('_1, '_2.proctime, '_3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidAliasWithRowtimeAttribute(): Unit = {
    val util = streamTestUtil()
    // aliased field does not exist
    util.addTable[(Int, Long, String)]('_1, 'newnew.rowtime as 'new, '_3)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidAliasWithRowtimeAttribute2(): Unit = {
    val util = streamTestUtil()
    // aliased field has wrong type
    util.addTable[(Int, Long, String)]('_1, '_3.rowtime as 'new, '_2)
  }
}
