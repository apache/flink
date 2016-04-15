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
import org.apache.flink.api.table.{TableEnvironment, Row}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.codegen.CodeGenException
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Assert._
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
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT a, b, CAST(NULL AS INT), CAST(NULL AS VARCHAR) = '' FROM MyTable"

    val t = env.fromElements((1, 0))
    tEnv.registerDataSet("MyTable", t, 'a, 'b)

    val result = tEnv.sql(sqlQuery)

    try {
      val ds = result.toDataSet[Row]
      if (!config.getNullCheck) {
        fail("Exception expected if null check is disabled.")
      }
      val results = ds.collect()
      val expected = "1,0,null,null"
      TestBaseUtils.compareResultAsText(results.asJava, expected)
    }
    catch {
      case e: CodeGenException =>
        if (config.getNullCheck) {
          throw e
        }
    }
  }

}
