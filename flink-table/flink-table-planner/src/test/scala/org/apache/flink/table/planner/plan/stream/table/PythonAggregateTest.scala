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
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.runtime.dataview.{ListViewSpec, MapViewSpec}

import org.assertj.core.api.Assertions.{assertThat, assertThatExceptionOfType}
import org.junit.jupiter.api.Test

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

    val resultTable = sourceTable
      .groupBy('b)
      .select('b, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testMixedUsePythonAggAndJavaAgg(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable
      .groupBy('b)
      .select('b, func('a, 'c), 'a.count())

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testExtractDataViewSpecs(): Unit = {
    val accType = DataTypes.ROW(
      DataTypes.FIELD("f0", DataTypes.STRING()),
      DataTypes.FIELD("f1", ListView.newListViewDataType(DataTypes.STRING())),
      DataTypes.FIELD("f2", MapView.newMapViewDataType(DataTypes.STRING(), DataTypes.BIGINT()))
    )

    val specs = CommonPythonUtil.extractDataViewSpecs(0, accType)

    val expected = Array(
      new ListViewSpec(
        "agg0$f1",
        1,
        DataTypes.ARRAY(DataTypes.STRING()).bridgedTo(classOf[java.util.List[_]])),
      new MapViewSpec(
        "agg0$f2",
        2,
        DataTypes
          .MAP(DataTypes.STRING(), DataTypes.BIGINT())
          .bridgedTo(classOf[java.util.Map[_, _]]),
        false)
    )

    assertThat(specs(0)).hasSameClassAs(expected(0))
    assertThat(specs(0).getDataType).isEqualTo(expected(0).getDataType)
    assertThat(specs(0).getStateId).isEqualTo(expected(0).getStateId)
    assertThat(specs(0).getFieldIndex).isEqualTo(expected(0).getFieldIndex)
    assertThat(specs(1)).hasSameClassAs(expected(1))
    assertThat(specs(1).getDataType).isEqualTo(expected(1).getDataType)
    assertThat(specs(1).getStateId).isEqualTo(expected(1).getStateId)
    assertThat(specs(1).getFieldIndex).isEqualTo(expected(1).getFieldIndex)
  }

  @Test
  def testExtractSecondLevelDataViewSpecs(): Unit = {
    val accType = DataTypes.ROW(
      DataTypes.FIELD(
        "f0",
        DataTypes.ROW(DataTypes.FIELD("f0", ListView.newListViewDataType(DataTypes.STRING())))))

    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(() => CommonPythonUtil.extractDataViewSpecs(0, accType))
  }

  @Test
  def testExtractDataViewSpecsFromStructuredType(): Unit = {
    val accType = DataTypes.STRUCTURED(
      classOf[Tuple1[_]],
      DataTypes.FIELD("f0", ListView.newListViewDataType(DataTypes.STRING())))

    assertThatExceptionOfType(classOf[TableException])
      .isThrownBy(() => CommonPythonUtil.extractDataViewSpecs(0, accType))
  }
}
