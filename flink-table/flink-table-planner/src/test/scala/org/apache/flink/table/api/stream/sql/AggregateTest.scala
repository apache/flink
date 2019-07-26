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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, GenericInMemoryCatalog}
import org.apache.flink.table.delegation.{Executor, Planner}
import org.apache.flink.table.functions.{AggregateFunction, AggregateFunctionDefinition}
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.types.Row
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.mockito.Mockito

class AggregateTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  private val table = streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testGroupbyWithoutWindow() = {
    val sql = "SELECT COUNT(a) FROM MyTable GROUP BY b"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(table),
            term("select", "b", "a")
          ),
          term("groupBy", "b"),
          term("select", "b", "COUNT(a) AS EXPR$0")
        ),
        term("select", "EXPR$0")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testUserDefinedAggregateFunctionWithScalaAccumulator(): Unit = {
    val defaultCatalog = "default_catalog"
    val catalogManager = new CatalogManager(
      defaultCatalog, new GenericInMemoryCatalog(defaultCatalog, "default_database"))

    val functionCatalog = new FunctionCatalog(catalogManager)
    val tablEnv = new StreamTableEnvironmentImpl(
      catalogManager,
      functionCatalog,
      new TableConfig,
      Mockito.mock(classOf[StreamExecutionEnvironment]),
      Mockito.mock(classOf[Planner]),
      Mockito.mock(classOf[Executor]),
      true
    )

    tablEnv.registerFunction("udag", new MyAgg)
    val aggFunctionDefinition = functionCatalog
      .lookupFunction("udag").get()
      .getFunctionDefinition
      .asInstanceOf[AggregateFunctionDefinition]

    val typeInfo = aggFunctionDefinition.getAccumulatorTypeInfo
    assertTrue(typeInfo.isInstanceOf[CaseClassTypeInfo[_]])
    assertEquals(2, typeInfo.getTotalFields)
    val caseTypeInfo = typeInfo.asInstanceOf[CaseClassTypeInfo[_]]
    assertEquals(Types.LONG, caseTypeInfo.getTypeAt(0))
    assertEquals(Types.LONG, caseTypeInfo.getTypeAt(1))

    tablEnv.registerFunction("udag2", new MyAgg2)
    val aggFunctionDefinition2 = functionCatalog
      .lookupFunction("udag2").get()
      .getFunctionDefinition
      .asInstanceOf[AggregateFunctionDefinition]

    val typeInfo2 = aggFunctionDefinition2.getAccumulatorTypeInfo
    assertTrue(s"actual type: $typeInfo2", typeInfo2.isInstanceOf[RowTypeInfo])
    assertEquals(2, typeInfo2.getTotalFields)
    val rowTypeInfo = typeInfo2.asInstanceOf[RowTypeInfo]
    assertEquals(Types.LONG, rowTypeInfo.getTypeAt(0))
    assertEquals(Types.INT, rowTypeInfo.getTypeAt(1))
  }
}

case class MyAccumulator(var sum: Long, var count: Long)

class MyAgg extends AggregateFunction[Long, MyAccumulator] {

  //Overloaded accumulate method
  def accumulate(acc: MyAccumulator, value: Long): Unit = {
  }

  override def createAccumulator(): MyAccumulator = MyAccumulator(0, 0)

  override def getValue(accumulator: MyAccumulator): Long = 1L
}

class MyAgg2 extends AggregateFunction[Long, Row] {

  def accumulate(acc: Row, value: Long): Unit = {}

  override def createAccumulator(): Row = new Row(2)

  override def getValue(accumulator: Row): Long = 1L

  override def getAccumulatorType: TypeInformation[Row] = new RowTypeInfo(Types.LONG, Types.INT)
}
