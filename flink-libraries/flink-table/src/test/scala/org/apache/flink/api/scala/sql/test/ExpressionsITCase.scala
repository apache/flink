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

package org.apache.flink.api.scala.sql.test

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.codegen.CodeGenException
import org.apache.flink.api.table.plan.TranslationContext
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ExpressionsITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testExactDecimal(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val sqlQuery = s"SELECT 11.2, 0.7623533651719233, 7623533651719233.0, " +
      s"${Double.MaxValue}, ${Double.MinValue}, " +
      s"CAST(${Float.MaxValue} AS FLOAT), CAST(${Float.MinValue} AS FLOAT), " +
      s"CAST(${Byte.MaxValue} AS TINYINT), CAST(${Byte.MinValue} AS TINYINT), " +
      s"CAST(${Short.MaxValue} AS SMALLINT), CAST(${Short.MinValue} AS SMALLINT), " +
      s"CAST(${Int.MaxValue} AS INTEGER), CAST(${Int.MinValue} AS INTEGER), " +
      s"CAST(${Long.MaxValue} AS BIGINT), CAST(${Long.MinValue} AS BIGINT) FROM MyTable"

    val ds = env.fromElements((1, 0))
    tEnv.registerDataSet("MyTable", ds, 'a, 'b)

    val result = tEnv.sql(sqlQuery)

    val expected = "11.2,0.7623533651719233,7.623533651719233E15," +
      "1.7976931348623157E308,-1.7976931348623157E308," +
      "3.4028235E38,-3.4028235E38," +
      "127,-128," +
      "32767,-32768," +
      "2147483647,-2147483648," +
      "9223372036854775807,-9223372036854775808"
    val results = result.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test(expected = classOf[CodeGenException])
  def testUnsupportedDecimal(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val sqlQuery = s"SELECT 0.76235336517192335 FROM MyTable"

    val ds = env.fromElements((1, 0))
    tEnv.registerDataSet("MyTable", ds, 'a, 'b)

    val result = tEnv.sql(sqlQuery)

    result.toDataSet[Row](getConfig).collect()
  }

}
