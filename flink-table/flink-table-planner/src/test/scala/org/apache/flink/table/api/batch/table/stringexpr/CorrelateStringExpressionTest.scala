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

package org.apache.flink.table.api.batch.table.stringexpr

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet => JDataSet}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{Expressions, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{PojoTableFunc, TableFunc2, _}
import org.apache.flink.types.Row

import org.junit.Test
import org.mockito.Mockito.{mock, when}

class CorrelateStringExpressionTest extends TableTestBase {

  @Test
  def testCorrelateJoinsWithJoinLateral(): Unit = {
    val util = batchTestUtil()

    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)

    val jDs = mock(classOf[JDataSet[Row]])
    when(jDs.getType).thenReturn(typeInfo)

    val sDs = mock(classOf[DataSet[Row]])
    when(sDs.javaSet).thenReturn(jDs)

    val jTab = util.javaTableEnv.fromDataSet(jDs, $("a"), $("b"), $("c"))
    val sTab = util.tableEnv.fromDataSet(sDs, 'a, 'b, 'c)

    // test cross join
    val func1 = new TableFunc1
    util.javaTableEnv.registerFunction("func1", func1)
    var scalaTable = sTab.joinLateral(func1('c) as 's).select('c, 's)
    var javaTable = jTab.joinLateral("func1(c).as(s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test left outer join
    scalaTable = sTab.leftOuterJoinLateral(func1('c) as 's).select('c, 's)
    javaTable = jTab.leftOuterJoinLateral("as(func1(c), s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test overloading
    scalaTable = sTab.joinLateral(func1('c, "$") as 's).select('c, 's)
    javaTable = jTab.joinLateral("func1(c, '$') as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    util.javaTableEnv.registerFunction("func2", func2)
    scalaTable = sTab.joinLateral(func2('c) as('name, 'len)).select('c, 'name, 'len)
    javaTable = jTab.joinLateral(
      "func2(c).as(name, len)").select("c, name, len")
    verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    util.javaTableEnv.registerFunction("hierarchy", hierarchy)
    scalaTable = sTab.joinLateral(
      hierarchy('c) as('name, 'adult, 'len)).select('c, 'name, 'len, 'adult)
    javaTable = jTab.joinLateral("AS(hierarchy(c), name, adult, len)")
      .select("c, name, len, adult")
    verifyTableEquals(scalaTable, javaTable)

    // test pojo type
    val pojo = new PojoTableFunc
    util.javaTableEnv.registerFunction("pojo", pojo)
    scalaTable = sTab.joinLateral(pojo('c)).select('c, 'name, 'age)
    javaTable = jTab.joinLateral("pojo(c)").select("c, name, age")
    verifyTableEquals(scalaTable, javaTable)

    // test with filter
    scalaTable = sTab.joinLateral(
      func2('c) as('name, 'len)).select('c, 'name, 'len).filter('len > 2)
    javaTable = jTab.joinLateral("func2(c) as (name, len)")
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)

    // test with scalar function
    scalaTable = sTab.joinLateral(func1('c.substring(2)) as 's).select('a, 'c, 's)
    javaTable = jTab.joinLateral(
      "func1(substring(c, 2)) as (s)").select("a, c, s")
    verifyTableEquals(scalaTable, javaTable)
  }
}
