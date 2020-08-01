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

package org.apache.flink.table.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.UnitTypeInfo
import org.apache.flink.table.api.TableEnvironmentTest._
import org.apache.flink.table.api.Types._
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo.{PROCTIME_INDICATOR => PROCTIME, ROWTIME_INDICATOR => ROWTIME}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.Test

class TableEnvironmentTest extends TableTestBase {

  // ----------------------------------------------------------------------------------------------
  // schema definition by position
  // ----------------------------------------------------------------------------------------------

  @Test
  def testProjectByPosition(): Unit = {
    val utils = Seq(streamTestUtil(), batchTestUtil())

    utils.foreach { util =>

      // case class
      util.verifySchema(
        util.addTable[CClass]('a, 'b, 'c),
        Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE))

      util.verifySchema(
        util.addTable[CClass]('a, 'b),
        Seq("a" -> INT, "b" -> STRING))

      util.verifySchema(
        util.addTable[CClass]('a),
        Seq("a" -> INT))

      // row
      util.verifySchema(
        util.addTable('a, 'b, 'c)(TEST_ROW),
        Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE))

      util.verifySchema(
        util.addTable('a, 'b)(TEST_ROW),
        Seq("a" -> INT, "b" -> STRING))

      util.verifySchema(
        util.addTable('a)(TEST_ROW),
        Seq("a" -> INT))

      // tuple
      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('a, 'b, 'c),
        Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE))

      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('a, 'b),
        Seq("a" -> INT, "b" -> STRING))

      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('a),
        Seq("a" -> INT))
    }
  }

  @Test
  def testStreamProjectWithAddingTimeAttributesByPosition(): Unit = {
    val util = streamTestUtil()

    // case class
    util.verifySchema(
      util.addTable[CClass]('a, 'b, 'c , 'proctime.proctime),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[CClass]('a, 'b, 'c, 'rowtime.rowtime),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "rowtime" -> ROWTIME))

    util.verifySchema(
      util.addTable[CClass]('a, 'b, 'c, 'rowtime.rowtime, 'proctime.proctime),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "rowtime" -> ROWTIME, "proctime" -> PROCTIME))

    // row
    util.verifySchema(
      util.addTable('a, 'b, 'c, 'proctime.proctime)(TEST_ROW),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable('a, 'b, 'c, 'rowtime.rowtime)(TEST_ROW),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "rowtime" -> ROWTIME))

    util.verifySchema(
      util.addTable('a, 'b, 'c, 'rowtime.rowtime, 'proctime.proctime)(TEST_ROW),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "rowtime" -> ROWTIME, "proctime" -> PROCTIME))

    // tuple
    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('a, 'b, 'c, 'proctime.proctime),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('a, 'b, 'c, 'rowtime.rowtime),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "rowtime" -> ROWTIME))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('a, 'b, 'c, 'rowtime.rowtime, 'proctime.proctime),
      Seq("a" -> INT, "b" -> STRING, "c" -> DOUBLE, "rowtime" -> ROWTIME, "proctime" -> PROCTIME))
  }

  @Test
  def testStreamAliasWithReplacingTimeAttributesByPosition(): Unit = {
    val util = streamTestUtil()

    // case class
    util.verifySchema(
      util.addTable[CClassWithTime]('a, 'b.rowtime, 'c),
      Seq("a" -> INT, "b" -> ROWTIME, "c" -> STRING))

    util.verifySchema(
      util.addTable[CClassWithTime]('a, 'b.rowtime, 'c),
      Seq("a" -> INT, "b" -> ROWTIME, "c" -> STRING))

    // row
    util.verifySchema(
      util.addTable('a, 'b.rowtime, 'c)(TEST_ROW_WITH_TIME),
      Seq("a" -> INT, "b" -> ROWTIME, "c" -> STRING))

    util.verifySchema(
      util.addTable('a, 'b.rowtime, 'c)(TEST_ROW_WITH_TIME),
      Seq("a" -> INT, "b" -> ROWTIME, "c" -> STRING))

    // tuple
    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('a, 'b.rowtime, 'c),
      Seq("a" -> INT, "b" -> ROWTIME, "c" -> STRING))

    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('a, 'b.rowtime, 'c),
      Seq("a" -> INT, "b" -> ROWTIME, "c" -> STRING))
  }

  // ----------------------------------------------------------------------------------------------
  // schema definition by name
  // ----------------------------------------------------------------------------------------------

  @Test
  def testProjectByName(): Unit = {
    val utils = Seq(streamTestUtil(), batchTestUtil())

    utils.foreach { util =>

      // atomic
      util.verifySchema(
        util.addTable[Int](),
        Seq("f0" -> INT))

      util.verifySchema(
        util.addTable[Int]('myint),
        Seq("myint" -> INT))

      // case class
      util.verifySchema(
        util.addTable[CClass](),
        Seq("cf1" -> INT, "cf2" -> STRING, "cf3" -> DOUBLE))

      util.verifySchema(
        util.addTable[CClass]('cf1, 'cf2),
        Seq("cf1" -> INT, "cf2" -> STRING))

      util.verifySchema(
        util.addTable[CClass]('cf1, 'cf3),
        Seq("cf1" -> INT, "cf3" -> DOUBLE))

      util.verifySchema(
        util.addTable[CClass]('cf3, 'cf1),
        Seq("cf3" -> DOUBLE, "cf1" -> INT))

      // row
      util.verifySchema(
        util.addTable()(TEST_ROW),
        Seq("rf1" -> INT, "rf2" -> STRING, "rf3" -> DOUBLE))

      util.verifySchema(
        util.addTable('rf1, 'rf2)(TEST_ROW),
        Seq("rf1" -> INT, "rf2" -> STRING))

      util.verifySchema(
        util.addTable('rf1, 'rf3)(TEST_ROW),
        Seq("rf1" -> INT, "rf3" -> DOUBLE))

      util.verifySchema(
        util.addTable('rf3, 'rf1)(TEST_ROW),
        Seq("rf3" -> DOUBLE, "rf1" -> INT))

      // tuple
      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]](),
        Seq("f0" -> INT, "f1" -> STRING, "f2" -> DOUBLE))

      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('f0, 'f1),
        Seq("f0" -> INT, "f1" -> STRING))

      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('f0, 'f2),
        Seq("f0" -> INT, "f2" -> DOUBLE))

      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('f2, 'f0),
        Seq("f2" -> DOUBLE, "f0" -> INT))

      // pojo
      util.verifySchema(
        util.addTable[PojoClass](),
        Seq("pf1" -> INT, "pf2" -> STRING, "pf3" -> DOUBLE))

      util.verifySchema(
        util.addTable[PojoClass]('pf1, 'pf2),
        Seq("pf1" -> INT, "pf2" -> STRING))

      util.verifySchema(
        util.addTable[PojoClass]('pf1, 'pf3),
        Seq("pf1" -> INT, "pf3" -> DOUBLE))

      util.verifySchema(
        util.addTable[PojoClass]('pf3, 'pf1),
        Seq("pf3" -> DOUBLE, "pf1" -> INT))

      // generic
      util.verifySchema(
        util.addTable[Class[_]]('mygeneric),
        Seq("mygeneric" -> new GenericTypeInfo[Class[_]](classOf[Class[_]])))

      util.verifySchema(
        util.addTable[Class[_]](),
        Seq("f0" -> new GenericTypeInfo[Class[_]](classOf[Class[_]])))

      // any type info
      util.verifySchema(
        util.addTable[Unit](),
        Seq("f0" -> new UnitTypeInfo()))

      util.verifySchema(
        util.addTable[Unit]('unit),
        Seq("unit" -> new UnitTypeInfo()))
    }
  }

  @Test
  def testStreamProjectWithAddingTimeAttributesByName(): Unit = {
    val util = streamTestUtil()

    // atomic
    util.verifySchema(
      util.addTable[Int]('proctime.proctime, 'myint),
      Seq("proctime" -> PROCTIME, "myint" -> INT))

    util.verifySchema(
      util.addTable[Int]('rowtime.rowtime, 'myint),
      Seq("rowtime" -> ROWTIME, "myint" -> INT))

    util.verifySchema(
      util.addTable[Int]('myint, 'proctime.proctime),
      Seq("myint" -> INT, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[Int]('myint, 'rowtime.rowtime),
      Seq("myint" -> INT, "rowtime" -> ROWTIME))

    // case class
    util.verifySchema(
      util.addTable[CClass]('proctime.proctime, 'cf1, 'cf3),
      Seq("proctime" -> PROCTIME, "cf1" -> INT, "cf3" -> DOUBLE))

    util.verifySchema(
      util.addTable[CClass]('rowtime.rowtime, 'cf3, 'cf1),
      Seq("rowtime" -> ROWTIME, "cf3" -> DOUBLE, "cf1" -> INT))

    util.verifySchema(
      util.addTable[CClass]('cf1, 'proctime.proctime, 'cf3),
      Seq("cf1" -> INT, "proctime" -> PROCTIME, "cf3" -> DOUBLE))

    util.verifySchema(
      util.addTable[CClass]('cf3, 'rowtime.rowtime, 'cf1),
      Seq("cf3" -> DOUBLE, "rowtime" -> ROWTIME, "cf1" -> INT))

    util.verifySchema(
      util.addTable[CClass]('cf1, 'cf3, 'proctime.proctime),
      Seq("cf1" -> INT, "cf3" -> DOUBLE, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[CClass]('cf3, 'cf1, 'rowtime.rowtime),
      Seq("cf3" -> DOUBLE, "cf1" -> INT, "rowtime" -> ROWTIME))

    // row
    util.verifySchema(
      util.addTable('proctime.proctime, 'rf1, 'rf3)(TEST_ROW),
      Seq("proctime" -> PROCTIME, "rf1" -> INT, "rf3" -> DOUBLE))

    util.verifySchema(
      util.addTable('rowtime.rowtime, 'rf3, 'rf1)(TEST_ROW),
      Seq("rowtime" -> ROWTIME, "rf3" -> DOUBLE, "rf1" -> INT))

    util.verifySchema(
      util.addTable('rf3, 'proctime.proctime, 'rf1)(TEST_ROW),
      Seq("rf3" -> DOUBLE, "proctime" -> PROCTIME, "rf1" -> INT))

    util.verifySchema(
      util.addTable('rf3, 'rowtime.rowtime, 'rf1)(TEST_ROW),
      Seq("rf3" -> DOUBLE, "rowtime" -> ROWTIME, "rf1" -> INT))

        util.verifySchema(
      util.addTable('rf3, 'rf1, 'proctime.proctime)(TEST_ROW),
      Seq("rf3" -> DOUBLE, "rf1" -> INT, "proctime" -> PROCTIME))

        util.verifySchema(
      util.addTable('rf3, 'rf1, 'rowtime.rowtime)(TEST_ROW),
      Seq("rf3" -> DOUBLE, "rf1" -> INT, "rowtime" -> ROWTIME))

    // tuple
    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('proctime.proctime, 'f0, 'f2),
      Seq("proctime" -> PROCTIME, "f0" -> INT, "f2" -> DOUBLE))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('rowtime.rowtime, 'f2, 'f0),
      Seq("rowtime" -> ROWTIME, "f2" -> DOUBLE, "f0" -> INT))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('f0, 'proctime.proctime, 'f2),
      Seq("f0" -> INT, "proctime" -> PROCTIME, "f2" -> DOUBLE))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('f2, 'rowtime.rowtime, 'f0),
      Seq("f2" -> DOUBLE, "rowtime" -> ROWTIME, "f0" -> INT))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('f0, 'f2, 'proctime.proctime),
      Seq("f0" -> INT, "f2" -> DOUBLE, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[JTuple3[Int, String, Double]]('f2, 'f0, 'rowtime.rowtime),
      Seq("f2" -> DOUBLE, "f0" -> INT, "rowtime" -> ROWTIME))

    // pojo
    util.verifySchema(
      util.addTable[PojoClass]('proctime.proctime, 'pf1, 'pf3),
      Seq("proctime" -> PROCTIME, "pf1" -> INT, "pf3" -> DOUBLE))

    util.verifySchema(
      util.addTable[PojoClass]('rowtime.rowtime, 'pf3, 'pf1),
      Seq("rowtime" -> ROWTIME, "pf3" -> DOUBLE, "pf1" -> INT))

    util.verifySchema(
      util.addTable[PojoClass]('pf1, 'proctime.proctime, 'pf3),
      Seq("pf1" -> INT, "proctime" -> PROCTIME, "pf3" -> DOUBLE))

    util.verifySchema(
      util.addTable[PojoClass]('pf3, 'rowtime.rowtime, 'pf1),
      Seq("pf3" -> DOUBLE, "rowtime" -> ROWTIME, "pf1" -> INT))

    util.verifySchema(
      util.addTable[PojoClass]('pf1, 'pf3, 'proctime.proctime),
      Seq("pf1" -> INT, "pf3" -> DOUBLE, "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[PojoClass]('pf3, 'pf1, 'rowtime.rowtime),
      Seq("pf3" -> DOUBLE, "pf1" -> INT, "rowtime" -> ROWTIME))

    // generic
    util.verifySchema(
      util.addTable[Class[_]]('proctime.proctime, 'mygeneric),
      Seq("proctime" -> PROCTIME, "mygeneric" -> new GenericTypeInfo[Class[_]](classOf[Class[_]])))

    util.verifySchema(
      util.addTable[Class[_]]('rowtime.rowtime, 'mygeneric),
      Seq("rowtime" -> ROWTIME, "mygeneric" -> new GenericTypeInfo[Class[_]](classOf[Class[_]])))

    util.verifySchema(
      util.addTable[Class[_]]('mygeneric, 'proctime.proctime),
      Seq("mygeneric" -> new GenericTypeInfo[Class[_]](classOf[Class[_]]), "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[Class[_]]('mygeneric, 'rowtime.rowtime),
      Seq("mygeneric" -> new GenericTypeInfo[Class[_]](classOf[Class[_]]), "rowtime" -> ROWTIME))

    // any type info
    util.verifySchema(
      util.addTable[Unit]('proctime.proctime, 'unit),
      Seq("proctime" -> PROCTIME, "unit" -> new UnitTypeInfo()))

    util.verifySchema(
      util.addTable[Unit]('rowtime.rowtime, 'unit),
      Seq("rowtime" -> ROWTIME, "unit" -> new UnitTypeInfo()))

    util.verifySchema(
      util.addTable[Unit]('unit, 'proctime.proctime),
      Seq("unit" -> new UnitTypeInfo(), "proctime" -> PROCTIME))

    util.verifySchema(
      util.addTable[Unit]('unit, 'rowtime.rowtime),
      Seq("unit" -> new UnitTypeInfo(), "rowtime" -> ROWTIME))
  }

  @Test
  def testStreamProjectWithReplacingTimeAttributesByName(): Unit = {
    val util = streamTestUtil()

    // atomic
    util.verifySchema(
      util.addTable[Long]('new.rowtime),
      Seq("new" -> ROWTIME))

    util.verifySchema(
      util.addTable[Int]('new.proctime),
      Seq("new" -> PROCTIME))

    // case class
    util.verifySchema(
      util.addTable[CClassWithTime]('cf1, 'xxx.proctime, 'cf3),
      Seq("cf1" -> INT, "xxx" -> PROCTIME, "cf3" -> STRING))

    util.verifySchema(
      util.addTable[CClassWithTime]('cf1, 'cf2.rowtime, 'cf3),
      Seq("cf1" -> INT, "cf2" -> ROWTIME, "cf3" -> STRING))

    // row
    util.verifySchema(
      util.addTable('rf1, 'xxx.proctime, 'rf3)(TEST_ROW_WITH_TIME),
      Seq("rf1" -> INT, "xxx" -> PROCTIME, "rf3" -> STRING))

    util.verifySchema(
      util.addTable('rf1, 'rf2.rowtime, 'rf3)(TEST_ROW_WITH_TIME),
      Seq("rf1" -> INT, "rf2" -> ROWTIME, "rf3" -> STRING))

    // tuple
    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('f0, 'xxx.proctime, 'f2),
      Seq("f0" -> INT, "xxx" -> PROCTIME, "f2" -> STRING))

    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('f0, 'f1.rowtime, 'f2),
      Seq("f0" -> INT, "f1" -> ROWTIME, "f2" -> STRING))
  }

  @Test
  def testAliasByName(): Unit = {
    val utils = Seq(streamTestUtil(), batchTestUtil())

    utils.foreach { util =>

      // case class
      util.verifySchema(
        util.addTable[CClass]('cf1, 'cf3 as 'new, 'cf2),
        Seq("cf1" -> INT, "new" -> DOUBLE, "cf2" -> STRING))

      // row
      util.verifySchema(
        util.addTable('rf1, 'rf3 as 'new, 'rf2)(TEST_ROW),
        Seq("rf1" -> INT, "new" -> DOUBLE, "rf2" -> STRING))

      // tuple
      util.verifySchema(
        util.addTable[JTuple3[Int, String, Double]]('f0, 'f2 as 'new, 'f1),
        Seq("f0" -> INT, "new" -> DOUBLE, "f1" -> STRING))
    }
  }

  @Test
  def testStreamAliasWithAddingTimeAttributesByName(): Unit = {
    val util = streamTestUtil()

    // atomic
    util.verifySchema(
      util.addTable[Int]('new.proctime),
      Seq("new" -> PROCTIME))

    // case class
    util.verifySchema(
      util.addTable[CClassWithTime]('cf1, 'new.proctime, 'cf2),
      Seq("cf1" -> INT, "new" -> PROCTIME, "cf2" -> LONG))

    util.verifySchema(
      util.addTable[CClassWithTime]('cf1, 'new.rowtime, 'cf2),
      Seq("cf1" -> INT, "new" -> ROWTIME, "cf2" -> LONG))

    // row
    util.verifySchema(
      util.addTable('rf1, 'new.proctime, 'rf2)(TEST_ROW_WITH_TIME),
      Seq("rf1" -> INT, "new" -> PROCTIME, "rf2" -> LONG))

    util.verifySchema(
      util.addTable('rf1, 'new.rowtime, 'rf2)(TEST_ROW_WITH_TIME),
      Seq("rf1" -> INT, "new" -> ROWTIME, "rf2" -> LONG))

    // tuple
    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('f0, 'new.proctime, 'f1),
      Seq("f0" -> INT, "new" -> PROCTIME, "f1" -> LONG))

    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('f0, 'new.rowtime, 'f1),
      Seq("f0" -> INT, "new" -> ROWTIME, "f1" -> LONG))
  }

  @Test
  def testStreamAliasWithReplacingTimeAttributesByName(): Unit = {
    val util = streamTestUtil()

    // case class
    util.verifySchema(
      util.addTable[CClassWithTime]('cf1, 'cf2.rowtime as 'new, 'cf3),
      Seq("cf1" -> INT, "new" -> ROWTIME, "cf3" -> STRING))

    // row
    util.verifySchema(
      util.addTable('rf1, 'rf2.rowtime as 'new, 'rf3)(TEST_ROW_WITH_TIME),
      Seq("rf1" -> INT, "new" -> ROWTIME, "rf3" -> STRING))

    // tuple
    util.verifySchema(
      util.addTable[JTuple3[Int, Long, String]]('f0, 'f1.rowtime as 'new, 'f2),
      Seq("f0" -> INT, "new" -> ROWTIME, "f2" -> STRING))
  }

  @Test
  def testTableSchemaWithDifferentRowTypes(): Unit = {

    def createInnerRow(innerFieldName: String): TypeInformation[_] = {
      Types.ROW(
        Array[String](innerFieldName),
        Array[TypeInformation[_]](Types.INT()))
    }

    def createRow(innerFieldName: String): TypeInformation[_] = {
      Types.ROW(
        Array[String]("field"),
        Array[TypeInformation[_]](createInnerRow(innerFieldName))
      )
    }

    val util = streamTestUtil()
    util.addTable("MyTableA", 'field)(createRow("A"))
    util.addTable("MyTableB", 'field)(createRow("B"))

    val actualSchema = util.tableEnv
      .sqlQuery("SELECT MyTableA.field AS a, MyTableB.field AS b FROM MyTableA, MyTableB")
      .getSchema

    val expectedSchema = TableSchema.builder()
      .field("a", createInnerRow("A"))
      .field("b", createInnerRow("B"))
      .build()

    assertEquals(expectedSchema, actualSchema)
  }
}

object TableEnvironmentTest {

  case class CClass(cf1: Int, cf2: String, cf3: Double)

  case class CClassWithTime(cf1: Int, cf2: Long, cf3: String)

  class PojoClass(var pf2: String, var pf1: Int, var pf3: Double) {
    def this() = this("", 0, 0.0)
  }

  class PojoClassWithTime(var pf2: String, var pf1: Int, var pf3: Long) {
    def this() = this("", 0, 0L)
  }

  val TEST_ROW: TypeInformation[Row] = ROW(
    Array("rf1", "rf2", "rf3"),
    Array[TypeInformation[_]](INT, STRING, DOUBLE))

  val TEST_ROW_WITH_TIME: TypeInformation[Row] = ROW(
    Array("rf1", "rf2", "rf3"),
    Array[TypeInformation[_]](INT, LONG, STRING))

}


