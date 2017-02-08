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

package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Types._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.TestBaseUtils.compareResultAsText
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class CastingITCase(configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testNumericAutocastInArithmetic() {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val table = env.fromElements(
      (1.toByte, 1.toShort, 1, 1L, 1.0f, 1.0d, 1L, 1001.1)).toTable(tableEnv)
      .select('_1 + 1, '_2 + 1, '_3 + 1L, '_4 + 1.0f,
        '_5 + 1.0d, '_6 + 1, '_7 + 1.0d, '_8 + '_1)

    val results = table.toDataSet[Row].collect()
    val expected = "2,2,2,2.0,2.0,2.0,2.0,1002.1"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  @throws[Exception]
  def testNumericAutocastInComparison() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val table = env.fromElements(
      (1.toByte, 1.toShort, 1, 1L, 1.0f, 1.0d),
      (2.toByte, 2.toShort, 2, 2L, 2.0f, 2.0d))
      .toTable(tableEnv, 'a, 'b, 'c, 'd, 'e, 'f)
      .filter('a > 1 && 'b > 1 && 'c > 1L && 'd > 1.0f && 'e > 1.0d && 'f > 1)

    val results = table.toDataSet[Row].collect()
    val expected: String = "2,2,2,2,2.0,2.0"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  @throws[Exception]
  def testCasting() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = env.fromElements((1, 0.0, 1L, true)).toTable(tableEnv)
      .select(
        // * -> String
      '_1.cast(STRING), '_2.cast(STRING), '_3.cast(STRING), '_4.cast(STRING),
        // NUMERIC TYPE -> Boolean
      '_1.cast(BOOLEAN), '_2.cast(BOOLEAN), '_3.cast(BOOLEAN),
        // NUMERIC TYPE -> NUMERIC TYPE
      '_1.cast(DOUBLE), '_2.cast(INT), '_3.cast(SHORT),
        // Boolean -> NUMERIC TYPE
      '_4.cast(DOUBLE),
        // identity casting
      '_1.cast(INT), '_2.cast(DOUBLE), '_3.cast(LONG), '_4.cast(BOOLEAN))

    val results = table.toDataSet[Row].collect()
    val expected = "1,0.0,1,true," + "true,false,true," +
      "1.0,0,1," + "1.0," + "1,0.0,1,true\n"
    compareResultAsText(results.asJava, expected)
  }

  @Test
  @throws[Exception]
  def testCastFromString() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val table = env.fromElements(("1", "true", "2.0")).toTable(tableEnv)
      .select('_1.cast(BYTE), '_1.cast(SHORT), '_1.cast(INT), '_1.cast(LONG),
        '_3.cast(DOUBLE), '_3.cast(FLOAT), '_2.cast(BOOLEAN))

    val results = table.toDataSet[Row].collect()
    val expected = "1,1,1,1,2.0,2.0,true\n"
    compareResultAsText(results.asJava, expected)
  }
}

