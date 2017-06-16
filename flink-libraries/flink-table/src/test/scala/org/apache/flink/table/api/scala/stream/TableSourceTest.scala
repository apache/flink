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

package org.apache.flink.table.api.scala.stream

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableException, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.{DefinedProctimeAttribute, DefinedRowtimeAttribute, StreamTableSource}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{term, unaryNode}
import org.apache.flink.types.Row
import org.junit.Test

class TableSourceTest extends TableTestBase {

  @Test
  def testRowTimeTableSourceSimple(): Unit = {
    val util = streamTestUtil()
    util.tEnv.registerTableSource("rowTimeT", new TestRowtimeSource("addTime"))

    val t = util.tEnv.scan("rowTimeT").select("addTime, id, name, val")

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[rowTimeT]], fields=[id, val, name, addTime])",
        term("select", "TIME_MATERIALIZATION(addTime) AS addTime", "id", "name", "val")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {
    val util = streamTestUtil()
    util.tEnv.registerTableSource("rowTimeT", new TestRowtimeSource("addTime"))

    val t = util.tEnv.scan("rowTimeT")
      .filter("val > 100")
      .window(Tumble over 10.minutes on 'addTime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            "StreamTableSourceScan(table=[[rowTimeT]], fields=[id, val, name, addTime])",
            term("select", "name", "val", "addTime"),
            term("where", ">(val, 100)")
          ),
          term("groupBy", "name"),
          term("window", "TumblingGroupWindow('w, 'addTime, 600000.millis)"),
          term("select", "name", "AVG(val) AS TMP_1", "end('w) AS TMP_0")
        ),
        term("select", "name", "TMP_0", "TMP_1")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {
    val util = streamTestUtil()
    util.tEnv.registerTableSource("procTimeT", new TestProctimeSource("pTime"))

    val t = util.tEnv.scan("procTimeT").select("pTime, id, name, val")

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[procTimeT]], fields=[id, val, name, pTime])",
        term("select", "TIME_MATERIALIZATION(pTime) AS pTime", "id", "name", "val")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProcTimeTableSourceOverWindow(): Unit = {
    val util = streamTestUtil()
    util.tEnv.registerTableSource("procTimeT", new TestProctimeSource("pTime"))

    val t = util.tEnv.scan("procTimeT")
      .window(Over partitionBy 'id orderBy 'pTime preceding 2.hours as 'w)
      .select('id, 'name, 'val.sum over 'w as 'valSum)
      .filter('valSum > 100)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          "StreamTableSourceScan(table=[[procTimeT]], fields=[id, val, name, pTime])",
          term("partitionBy", "id"),
          term("orderBy", "pTime"),
          term("range", "BETWEEN 7200000 PRECEDING AND CURRENT ROW"),
          term("select", "id", "val", "name", "pTime", "SUM(val) AS w0$o0")
        ),
        term("select", "id", "name", "w0$o0 AS valSum"),
        term("where", ">(w0$o0, 100)")
      )
    util.verifyTable(t, expected)
  }

  @Test(expected = classOf[TableException])
  def testRowtimeTableSourceWithEmptyName(): Unit = {
    val util = streamTestUtil()
    util.tEnv.registerTableSource("rowTimeT", new TestRowtimeSource(" "))

    val t = util.tEnv.scan("rowTimeT")
      .select('id)

    util.tEnv.optimize(t.getRelNode, false)
  }

  @Test(expected = classOf[TableException])
  def testProctimeTableSourceWithEmptyName(): Unit = {
    val util = streamTestUtil()
    util.tEnv.registerTableSource("procTimeT", new TestProctimeSource(" "))

    val t = util.tEnv.scan("procTimeT")
      .select('id)

    util.tEnv.optimize(t.getRelNode, false)
  }
}

class TestRowtimeSource(timeField: String)
    extends StreamTableSource[Row] with DefinedRowtimeAttribute {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getRowtimeAttribute: String = timeField

  override def getReturnType: TypeInformation[Row] = {
    new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))
  }
}

class TestProctimeSource(timeField: String)
    extends StreamTableSource[Row] with DefinedProctimeAttribute {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getProctimeAttribute: String = timeField

  override def getReturnType: TypeInformation[Row] = {
    new RowTypeInfo(
      Array(Types.INT, Types.LONG, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "val", "name"))
  }
}


