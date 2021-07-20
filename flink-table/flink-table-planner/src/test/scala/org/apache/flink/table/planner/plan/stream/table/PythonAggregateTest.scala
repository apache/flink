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

import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.dataview.{ListView, MapView}
import org.apache.flink.table.planner.plan.nodes.exec.utils.CommonPythonUtil
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.TestPythonAggregateFunction
import org.apache.flink.table.planner.typeutils.DataViewUtils.{ListViewSpec, MapViewSpec}
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

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testPythonGroupAggregate(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable.groupBy('b)
      .select('b, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testMixedUsePythonAggAndJavaAgg(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable.groupBy('b)
      .select('b, func('a, 'c), 'a.count())

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testExtractDataViewSpecs(): Unit = {
    val accType = DataTypes.ROW(
      DataTypes.FIELD("f0", DataTypes.STRING()),
      DataTypes.FIELD("f1", ListView.newListViewDataType(DataTypes.STRING())),
      DataTypes.FIELD("f2", MapView.newMapViewDataType(DataTypes.STRING(), DataTypes.BIGINT())))

    val specs = CommonPythonUtil.extractDataViewSpecs(0, accType)

    val expected = Array(
      new ListViewSpec(
        "agg0$f1",
        1,
        DataTypes.ARRAY(DataTypes.STRING()).bridgedTo(classOf[java.util.List[_]])),
      new MapViewSpec(
        "agg0$f2",
        2,
        DataTypes.MAP(
          DataTypes.STRING(), DataTypes.BIGINT()).bridgedTo(classOf[java.util.Map[_, _]]),
        false))

    assertEquals(expected(0).getClass, specs(0).getClass)
    assertEquals(expected(0).getDataType, specs(0).getDataType)
    assertEquals(expected(0).getStateId, specs(0).getStateId)
    assertEquals(expected(0).getFieldIndex, specs(0).getFieldIndex)
    assertEquals(expected(1).getClass, specs(1).getClass)
    assertEquals(expected(1).getDataType, specs(1).getDataType)
    assertEquals(expected(1).getStateId, specs(1).getStateId)
    assertEquals(expected(1).getFieldIndex, specs(1).getFieldIndex)
  }

  @Test(expected = classOf[TableException])
  def testExtractSecondLevelDataViewSpecs(): Unit = {
    val accType = DataTypes.ROW(
      DataTypes.FIELD("f0", DataTypes.ROW(
        DataTypes.FIELD("f0", ListView.newListViewDataType(DataTypes.STRING())))))
    CommonPythonUtil.extractDataViewSpecs(0, accType)
  }

  @Test(expected = classOf[TableException])
  def testExtractDataViewSpecsFromStructuredType(): Unit = {
    val accType = DataTypes.STRUCTURED(
      classOf[Tuple1[_]],
      DataTypes.FIELD("f0", ListView.newListViewDataType(DataTypes.STRING())))
    CommonPythonUtil.extractDataViewSpecs(0, accType)
  }
}
