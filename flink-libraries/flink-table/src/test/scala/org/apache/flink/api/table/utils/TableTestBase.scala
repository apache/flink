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

package org.apache.flink.api.table.utils

import org.apache.calcite.plan.RelOptUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.{Table, TableEnvironment}
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Assert
import org.mockito.Mockito.{mock, when}

/**
  * Test base for testing Table API / SQL plans.
  */
class TableTestBase {

  def batchTestUtil(): BatchTableTestUtil = {
    BatchTableTestUtil()
  }

  def streamTestUtil(): StreamTableTestUtil = {
    StreamTableTestUtil()
  }

}

abstract class TableTestUtil {
  def addTable[T: TypeInformation](name: String, fields: Expression*): Table
  def verifySql(query: String, expected: String): Unit
  def verifyTable(resultTable: Table, expected: String): Unit
}

object TableTestUtil {

  // this methods are currently just for simplifying string construction,
  // we could replace it with logic later

  def unaryNode(node: String, input: String, term: String*): String = {
    s"""$node(${term.mkString(", ")})
       |  $input
       |""".stripMargin
  }

  def binaryNode(node: String, left: String, right: String, term: String*): String = {
    s"""$node(${term.mkString(", ")})
       |  $left
       |  $right
       |""".stripMargin
  }

  def term(term: String, value: String*): String = {
    s"$term=[${value.mkString(", ")}]"
  }

  def batchTableNode(idx: Int): String = {
    s"DataSetScan(table=[[_DataSetTable_$idx]])"
  }

  def streamTableNode(idx: Int): String = {
    s"DataStreamScan(table=[[_DataStreamTable_$idx]])"
  }
}

case class BatchTableTestUtil() extends TableTestUtil {

  val env = mock(classOf[ExecutionEnvironment])
  val tEnv = TableEnvironment.getTableEnvironment(env)

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
    : Table = {
    val ds = mock(classOf[DataSet[T]])
    val jDs = mock(classOf[JDataSet[T]])
    when(ds.javaSet).thenReturn(jDs)
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    when(jDs.getType).thenReturn(typeInfo)

    val t = ds.toTable(tEnv, fields: _*)
    tEnv.registerTable(name, t)
    t
  }

  def verifySql(query: String, expected: String): Unit = {
    verifyTable(tEnv.sql(query), expected)
  }

  def verifyTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tEnv.optimize(relNode)
    val actual = RelOptUtil.toString(optimized)
    Assert.assertEquals(expected, actual)
  }
}

case class StreamTableTestUtil() extends TableTestUtil {

  val env = mock(classOf[StreamExecutionEnvironment])
  val tEnv = TableEnvironment.getTableEnvironment(env)

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
    : Table = {

    val ds = mock(classOf[DataStream[T]])
    val jDs = mock(classOf[JDataStream[T]])
    when(ds.javaStream).thenReturn(jDs)
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    when(jDs.getType).thenReturn(typeInfo)

    val t = ds.toTable(tEnv, fields: _*)
    tEnv.registerTable(name, t)
    t
  }

  def verifySql(query: String, expected: String): Unit = {
    verifyTable(tEnv.sql(query), expected)
  }

  def verifyTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = tEnv.optimize(relNode)
    val actual = RelOptUtil.toString(optimized)
    Assert.assertEquals(expected, actual)
  }
}
