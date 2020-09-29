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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.dataview.ListViewTypeInfo
import org.apache.flink.table.planner.plan.nodes.common.CommonPythonAggregate
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.TestPythonAggregateFunction
import org.apache.flink.table.planner.typeutils.DataViewUtils.{DataViewSpec, ListViewSpec}
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.Assert.assertEquals
import org.junit.Test

class PythonAggregateTest extends TableTestBase {

  @Test
  def testPythonGroupAggregateWithoutKeys(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable.select(func('a, 'c))

    util.verifyPlan(resultTable)
  }

  @Test
  def testPythonGroupAggregate(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable.groupBy('b)
      .select('b, func('a, 'c))

    util.verifyPlan(resultTable)
  }

  @Test(expected = classOf[TableException])
  def testMixedUsePythonAggAndJavaAgg(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable.groupBy('b)
      .select('b, func('a, 'c), 'a.count())

    util.verifyPlan(resultTable)
  }

  @Test
  def testExtractDataViewSpecs(): Unit = {
    val accTypeInfo =
      new RowTypeInfo(Types.STRING(), new ListViewTypeInfo(Types.STRING()))

    val specs = TestCommonPythonAggregate.extractDataViewSpecs(accTypeInfo)

    val expected = Array(
      new ListViewSpec("agg0$f1", 1, DataTypes.ARRAY(DataTypes.STRING())))

    assertEquals(expected(0).getClass, specs(0).getClass)
    assertEquals(expected(0).getDataType, specs(0).getDataType)
    assertEquals(expected(0).getStateId, specs(0).getStateId)
    assertEquals(expected(0).getFieldIndex, specs(0).getFieldIndex)
  }

  @Test(expected = classOf[TableException])
  def testExtractSecondLevelDataViewSpecs(): Unit = {
    val accTypeInfo =
      new RowTypeInfo(Types.STRING(), new RowTypeInfo(new ListViewTypeInfo(Types.STRING())))

    val specs = TestCommonPythonAggregate.extractDataViewSpecs(accTypeInfo)
  }

  @Test(expected = classOf[TableException])
  def testExtractDataViewSpecsFromTupleType(): Unit = {
    val accTypeInfo =
      new TupleTypeInfo(Types.STRING(), new ListViewTypeInfo(Types.STRING()))

    val specs = TestCommonPythonAggregate.extractDataViewSpecs(accTypeInfo)
  }
}

object TestCommonPythonAggregate extends CommonPythonAggregate {
  def extractDataViewSpecs(accType: TypeInformation[_]): Array[DataViewSpec] = {
    extractDataViewSpecs(0, accType)
  }
}
