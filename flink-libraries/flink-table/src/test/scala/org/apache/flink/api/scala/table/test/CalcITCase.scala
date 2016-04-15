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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class CalcITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testSimpleCalc(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
        .select('_1, '_2, '_3)
        .where('_1 < 7)
        .select('_1, '_3)

    val expected = "1,Hi\n" + "2,Hello\n" + "3,Hello world\n" +
      "4,Hello world, how are you?\n" + "5,I am fine.\n" + "6,Luke Skywalker\n"
      val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcWithTwoFilters(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
        .select('_1, '_2, '_3)
        .where('_1 < 7 && '_2 === 3)
        .select('_1, '_3)
        .where('_1 === 4)
        .select('_1)

    val expected = "4\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcWithAggregation(): Unit = {
    
    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = CollectionDataSets.get3TupleDataSet(env).toTable
        .select('_1, '_2, '_3)
        .where('_1 < 15)
        .groupBy('_2)
        .select('_1.min, '_2.count as 'cnt)
        .where('cnt > 3)

    val expected = "7,4\n" + "11,4\n"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCalcJoin(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).as('a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSet(env).as('d, 'e, 'f, 'g, 'h)

    val joinT = ds1.select('a, 'b).join(ds2).where('b === 'e).select('a, 'b, 'd, 'e, 'f)
      .where('b > 1).select('a, 'd).where('d === 2)

    val expected = "2,2\n" + "3,2\n"
    val results = joinT.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
