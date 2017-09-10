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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.{Assert, Test}

class TableSourceTest extends TableTestBase {

  @Test
  def testTableSourceWithLongRowTimeField(): Unit = {

    val tableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime"
    )

    val util = streamTestUtil()
    util.tableEnv.registerTableSource("rowTimeT", tableSource)

    val t = util.tableEnv.scan("rowTimeT").select("rowtime, id, name, val")

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[rowTimeT]], fields=[id, rowtime, val, name])",
        term("select", "rowtime", "id", "name", "val")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testTableSourceWithTimestampRowTimeField(): Unit = {

    val tableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.SQL_TIMESTAMP, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime"
    )

    val util = streamTestUtil()
    util.tableEnv.registerTableSource("rowTimeT", tableSource)

    val t = util.tableEnv.scan("rowTimeT").select("rowtime, id, name, val")

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[rowTimeT]], fields=[id, rowtime, val, name])",
        term("select", "rowtime", "id", "name", "val")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testRowTimeTableSourceGroupWindow(): Unit = {

    val tableSource = new TestRowtimeSource(
      Array("id", "rowtime", "val", "name"),
      Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
      "rowtime"
    )

    val util = streamTestUtil()
    util.tableEnv.registerTableSource("rowTimeT", tableSource)

    val t = util.tableEnv.scan("rowTimeT")
      .filter("val > 100")
      .window(Tumble over 10.minutes on 'rowtime as 'w)
      .groupBy('name, 'w)
      .select('name, 'w.end, 'val.avg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupWindowAggregate",
          unaryNode(
            "DataStreamCalc",
            "StreamTableSourceScan(table=[[rowTimeT]], fields=[id, rowtime, val, name])",
            term("select", "name", "val", "rowtime"),
            term("where", ">(val, 100)")
          ),
          term("groupBy", "name"),
          term("window", "TumblingGroupWindow('w, 'rowtime, 600000.millis)"),
          term("select", "name", "AVG(val) AS TMP_1", "end('w) AS TMP_0")
        ),
        term("select", "name", "TMP_0", "TMP_1")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProcTimeTableSourceSimple(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.registerTableSource("procTimeT", new TestProctimeSource("pTime"))

    val t = util.tableEnv.scan("procTimeT").select("pTime, id, name, val")

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[procTimeT]], fields=[id, val, name, pTime])",
        term("select", "PROCTIME(pTime) AS pTime", "id", "name", "val")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProcTimeTableSourceOverWindow(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.registerTableSource("procTimeT", new TestProctimeSource("pTime"))

    val t = util.tableEnv.scan("procTimeT")
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

  @Test
  def testProjectableProcTimeTableSource(): Unit = {
    // ensures that projection is not pushed into table source with proctime indicators
    val util = streamTestUtil()

    val projectableTableSource = new TestProctimeSource("pTime") with ProjectableTableSource[Row] {
      override def projectFields(fields: Array[Int]): TableSource[Row] = {
        // ensure this method is not called!
        Assert.fail()
        null.asInstanceOf[TableSource[Row]]
      }
    }
    util.tableEnv.registerTableSource("PTimeTable", projectableTableSource)

    val t = util.tableEnv.scan("PTimeTable")
      .select('name, 'val)
      .where('val > 10)

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[PTimeTable]], fields=[id, val, name, pTime])",
        term("select", "name", "val"),
        term("where", ">(val, 10)")
      )
    util.verifyTable(t, expected)
  }

  @Test
  def testProjectableRowTimeTableSource(): Unit = {
    // ensures that projection is not pushed into table source with rowtime indicators
    val util = streamTestUtil()

    val projectableTableSource = new TestRowtimeSource(
        Array("id", "rowtime", "val", "name"),
        Array(Types.INT, Types.LONG, Types.LONG, Types.STRING)
          .asInstanceOf[Array[TypeInformation[_]]],
        "rowtime") with ProjectableTableSource[Row] {

      override def projectFields(fields: Array[Int]): TableSource[Row] = {
        // ensure this method is not called!
        Assert.fail()
        null.asInstanceOf[TableSource[Row]]
      }
    }
    util.tableEnv.registerTableSource("RTimeTable", projectableTableSource)

    val t = util.tableEnv.scan("RTimeTable")
      .select('name, 'val)
      .where('val > 10)

    val expected =
      unaryNode(
        "DataStreamCalc",
        "StreamTableSourceScan(table=[[RTimeTable]], fields=[id, rowtime, val, name])",
        term("select", "name", "val"),
        term("where", ">(val, 10)")
      )
    util.verifyTable(t, expected)
  }
}

class TestRowtimeSource(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]],
    rowtimeField: String)
  extends StreamTableSource[Row] with DefinedRowtimeAttribute {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = ???

  override def getRowtimeAttribute: String = rowtimeField

  override def getReturnType: TypeInformation[Row] = {
    new RowTypeInfo(fieldTypes, fieldNames)
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


