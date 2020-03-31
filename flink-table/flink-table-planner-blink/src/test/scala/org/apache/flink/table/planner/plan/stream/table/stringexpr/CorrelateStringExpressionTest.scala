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

package org.apache.flink.table.planner.plan.stream.table.stringexpr

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils._
import org.apache.flink.types.Row

import org.junit.Test
import org.mockito.Mockito.{mock, when}

class CorrelateStringExpressionTest extends TableTestBase {

  @Test
  def testCorrelateJoinsWithJoinLateral(): Unit = {

    val scalaUtil = scalaStreamTestUtil()
    val javaUtil = javaStreamTestUtil()
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    val jDs = mock(classOf[JDataStream[Row]])
    when(jDs.getType).thenReturn(typeInfo)

    val sDs = mock(classOf[DataStream[Row]])
    when(sDs.javaStream).thenReturn(jDs)

    val jTab = javaUtil.tableEnv.fromDataStream(jDs, "a, b, c")
    val sTab = scalaUtil.tableEnv.fromDataStream(sDs, 'a, 'b, 'c)

    // test cross join
    val func1 = new TableFunc1
    javaUtil.addFunction("func1", func1)
    scalaUtil.addFunction("func1", func1)

    var scalaTable = sTab.joinLateral(call("func1", 'c) as 's).select('c, 's)
    var javaTable = jTab.joinLateral("func1(c).as(s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test left outer join
    scalaTable = sTab.leftOuterJoinLateral(call("func1", 'c) as 's).select('c, 's)
    javaTable = jTab.leftOuterJoinLateral("as(func1(c), s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test overloading
    scalaTable = sTab.joinLateral(call("func1", 'c, "$") as 's).select('c, 's)
    javaTable = jTab.joinLateral("func1(c, '$') as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    javaUtil.addFunction("func2", func2)
    scalaUtil.addFunction("func2", func2)
    scalaTable = sTab.joinLateral(call("func2", 'c) as ('name, 'len)).select('c, 'name, 'len)
    javaTable = jTab.joinLateral(
      "func2(c).as(name, len)").select("c, name, len")
    verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    javaUtil.addFunction("hierarchy", hierarchy)
    scalaUtil.addFunction("hierarchy", hierarchy)
    scalaTable = sTab.joinLateral(
      call("hierarchy", 'c) as ('name, 'adult, 'len)).select('c, 'name, 'len, 'adult)
    javaTable = jTab.joinLateral("AS(hierarchy(c), name, adult, len)")
      .select("c, name, len, adult")
    verifyTableEquals(scalaTable, javaTable)

    // test pojo type
    val pojo = new PojoTableFunc
    javaUtil.addFunction("pojo", pojo)
    scalaUtil.addFunction("pojo", pojo)
    scalaTable = sTab.joinLateral(call("pojo", 'c)).select('c, 'name, 'age)
    javaTable = jTab.joinLateral("pojo(c)").select("c, name, age")
    verifyTableEquals(scalaTable, javaTable)

    // test with filter
    scalaTable = sTab.joinLateral(
      call("func2", 'c) as ('name, 'len)).select('c, 'name, 'len).filter('len > 2)
    javaTable = jTab.joinLateral("func2(c) as (name, len)")
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)
  }

  @Test
  def testFlatMap(): Unit = {
    val scalaUtil = scalaStreamTestUtil()
    val javaUtil = javaStreamTestUtil()
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    val jDs = mock(classOf[JDataStream[Row]])
    when(jDs.getType).thenReturn(typeInfo)

    val sDs = mock(classOf[DataStream[Row]])
    when(sDs.javaStream).thenReturn(jDs)

    val jTab = javaUtil.tableEnv.fromDataStream(jDs, "a, b, c")
    val sTab = scalaUtil.tableEnv.fromDataStream(sDs, 'a, 'b, 'c)

    // test flatMap
    val func1 = new TableFunc1
    scalaUtil.addFunction("func1", func1)
    javaUtil.addFunction("func1", func1)
    var scalaTable = sTab.flatMap(call("func1", 'c)).as('s).select('s)
    var javaTable = jTab.flatMap("func1(c)").as("s").select("s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    scalaUtil.addFunction("func2", func2)
    javaUtil.addFunction("func2", func2)
    scalaTable = sTab.flatMap(call("func2", 'c)).as('name, 'len).select('name, 'len)
    javaTable = jTab.flatMap("func2(c)").as("name, len").select("name, len")
    verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    scalaUtil.addFunction("hierarchy", hierarchy)
    javaUtil.addFunction("hierarchy", hierarchy)
    scalaTable = sTab.flatMap(call("hierarchy", 'c))
      .as('name, 'adult, 'len)
      .select('name, 'len, 'adult)
    javaTable = jTab.flatMap("hierarchy(c)").as("name, adult, len").select("name, len, adult")
    verifyTableEquals(scalaTable, javaTable)

    // test pojo type
    val pojo = new PojoTableFunc
    scalaUtil.addFunction("pojo", pojo)
    javaUtil.addFunction("pojo", pojo)
    scalaTable = sTab.flatMap(call("pojo", 'c)).select('name, 'age)
    javaTable = jTab.flatMap("pojo(c)").select("name, age")
    verifyTableEquals(scalaTable, javaTable)

    // test with filter
    scalaTable = sTab.flatMap(call("func2", 'c))
      .as('name, 'len)
      .select('name, 'len)
      .filter('len > 2)
    javaTable = jTab.flatMap("func2(c)").as("name, len").select("name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)

    // test with scalar function
    scalaTable = sTab.flatMap(call("func1", 'c.substring(2))).as('s).select('s)
    javaTable = jTab.flatMap("func1(substring(c, 2))").as("s").select("s")
    verifyTableEquals(scalaTable, javaTable)
  }
}
