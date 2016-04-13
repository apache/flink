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
  def testNullLiteral(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = getScalaTableEnvironment
    TranslationContext.reset()

    val sqlQuery = "SELECT a, b, CAST(NULL AS INT), CAST(NULL AS VARCHAR) = '' FROM MyTable"

    val ds = env.fromElements((1, 0))
    tEnv.registerDataSet("MyTable", ds, 'a, 'b)

    val result = tEnv.sql(sqlQuery)

    val expected = if (getConfig.getNullCheck) {
      "1,0,null,null"
    } else {
      "1,0,-1,true"
    }
    val results = result.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
