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

package org.apache.flink.table.api.scala.stream.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

class TableEnvironmentValidationTest extends TableTestBase {
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

}
