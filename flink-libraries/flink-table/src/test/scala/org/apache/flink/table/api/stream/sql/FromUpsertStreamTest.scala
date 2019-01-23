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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestUtil.{UpsertTableNode, term, unaryNode}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.types.Row
import org.junit.Test
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import java.lang.{Boolean => JBool}

class FromUpsertStreamTest extends TableTestBase {

  private val streamUtil: StreamTableTestUtil = streamTestUtil()

  @Test
  def testRemoveUpsertToRetraction() = {
    streamUtil.addTableFromUpsert[(Boolean, (Int, String, Long))](
      "MyTable", 'a, 'b.key, 'c, 'proctime.proctime, 'rowtime.rowtime)

    val sql = "SELECT a, b, c, proctime, rowtime FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        UpsertTableNode(0),
        term("select", "a", "b", "c", "PROCTIME(proctime) AS proctime",
          "CAST(rowtime) AS rowtime")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testMaterializeTimeIndicatorAndCalcUpsertToRetractionTranspose() = {
    streamUtil.addTableFromUpsert[(Boolean, (Int, String, Long))](
      "MyTable", 'a, 'b.key, 'c, 'proctime.proctime, 'rowtime.rowtime)

    val sql = "SELECT b as b1, c, proctime as proctime1, rowtime as rowtime1 FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamUpsertToRetraction",
        unaryNode(
          "DataStreamCalc",
          UpsertTableNode(0),
          term("select", "b AS b1", "c", "PROCTIME(proctime) AS proctime1",
            "CAST(rowtime) AS rowtime1")
        ),
        term("keys", "b1"),
        term("select", "b1", "c", "proctime1", "rowtime1")
      )
    streamUtil.verifySql(sql, expected, true)
  }

  @Test
  def testCalcCannotTransposeUpsertToRetraction() = {
    streamUtil.addTableFromUpsert[(Boolean, (Int, String, Long))]("MyTable", 'a, 'b.key, 'c)

    val sql = "SELECT a, c FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamUpsertToRetraction",
          UpsertTableNode(0),
          term("keys", "b"),
          term("select", "a", "b", "c")
        ),
        term("select", "a", "c")
      )
    streamUtil.verifySql(sql, expected, true)
  }

  @Test
  def testSingleRowUpsert() = {
    streamUtil.addTableFromUpsert[(Boolean, (Int, String, Long))]("MyTable", 'a, 'b, 'c)
    val sql = "SELECT a, b FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamUpsertToRetraction",
        unaryNode(
          "DataStreamCalc",
          UpsertTableNode(0),
          term("select", "a", "b")
        ),
        term("select", "a", "b")
      )
    streamUtil.verifySql(sql, expected, true)
  }

  @Test
  def testFromUpsertForJavaTableEnvironment() = {
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.STRING, Types.LONG): _*)
    val tupleTypeInfo = new TupleTypeInfo(
      BasicTypeInfo.BOOLEAN_TYPE_INFO, typeInfo).asInstanceOf[TupleTypeInfo[JTuple2[JBool, Row]]]
    streamUtil.addJavaTableFromUpsert[Row](tupleTypeInfo, "MyTable", "a, b.key, c")

    val sql = "SELECT a, b as bb FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamUpsertToRetraction",
        unaryNode(
          "DataStreamCalc",
          UpsertTableNode(0),
          term("select", "a", "b AS bb")
        ),
        term("keys", "bb"),
        term("select", "a", "bb")
      )

    streamUtil.verifyJavaSql(sql, expected, true)
  }
}
