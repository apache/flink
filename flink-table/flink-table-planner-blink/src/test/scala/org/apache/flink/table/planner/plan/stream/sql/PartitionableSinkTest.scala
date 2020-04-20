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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{SqlDialect, TableConfig, ValidationException}
import org.apache.flink.table.planner.runtime.batch.sql.PartitionableSinkITCase
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class PartitionableSinkTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addTableSource[(Long, Long, Long)]("MyTable", 'a, 'b, 'c)
  PartitionableSinkITCase.registerTableSink(
    util.tableEnv,
    "sink",
    new RowTypeInfo(
      Array[TypeInformation[_]](Types.LONG, Types.LONG, Types.LONG),
      Array("a", "b", "c")),
    grouping = false,
    Array("b", "c"))

  @Test
  def testStatic(): Unit = {
    util.verifySqlUpdate("INSERT INTO sink PARTITION (b=1, c=1) SELECT a FROM MyTable")
  }

  @Test
  def testDynamic(): Unit = {
    util.verifySqlUpdate("INSERT INTO sink SELECT a, b, c FROM MyTable")
  }

  @Test
  def testPartial(): Unit = {
    util.verifySqlUpdate("INSERT INTO sink PARTITION (b=1) SELECT a, c FROM MyTable")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongStatic(): Unit = {
    util.verifySqlUpdate("INSERT INTO sink PARTITION (a=1) SELECT b, c FROM MyTable")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongFields(): Unit = {
    util.verifySqlUpdate("INSERT INTO sink PARTITION (b=1) SELECT a, b, c FROM MyTable")
  }
}
