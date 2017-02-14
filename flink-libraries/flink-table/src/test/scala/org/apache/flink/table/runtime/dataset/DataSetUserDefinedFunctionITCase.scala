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
package org.apache.flink.table.runtime.dataset

import org.apache.flink.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala.batch.utils.TableProgramsClusterTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.utils._
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class DataSetUserDefinedFunctionITCase (
  configMode: TableConfigMode)
  extends TableProgramsClusterTestBase(configMode) {

  @Test
  def testTableFunctionConstructorWithParams(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val in = testTableFunctionData(env).toTable(tableEnv).as('a, 'b, 'c)
    val func30 = new TableFunc3
    val func31 = new TableFunc3("OneConf_")
    val func32 = new TableFunc3("TwoConf_")

    val result = in
      .join(func30('c) as('d, 'e))
      .select('c, 'd, 'e)
      .join(func31('c) as ('f, 'g))
      .select('c, 'd, 'e, 'f, 'g)
      .join(func32('c) as ('h, 'i))
      .select('c, 'd, 'f, 'h, 'e, 'g, 'i)
      .toDataSet[Row]

    val results = result.collect()

    val expected = "Anna#44,Anna,OneConf_Anna,TwoConf_Anna,44,44,44\n"+
      "Jack#22,Jack,OneConf_Jack,TwoConf_Jack,22,22,22\n"+
      "John#19,John,OneConf_John,TwoConf_John,19,19,19\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  private def testTableFunctionData(env: ExecutionEnvironment): DataSet[(Int, Long, String)] = {

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }
}
