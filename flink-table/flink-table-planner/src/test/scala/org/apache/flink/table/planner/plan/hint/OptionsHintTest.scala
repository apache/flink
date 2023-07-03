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
package org.apache.flink.table.planner.plan.hint

import org.apache.flink.table.api.{DataTypes, Schema, ValidationException}
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.api.internal.StatementSetImpl
import org.apache.flink.table.catalog.{CatalogView, Column, ObjectPath, ResolvedSchema}
import org.apache.flink.table.planner.JHashMap
import org.apache.flink.table.planner.plan.hint.OptionsHintTest.{IS_BOUNDED, Param}
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink
import org.apache.flink.table.planner.utils.{OptionsTableSink, TableTestBase, TableTestUtil}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.hamcrest.Matchers._
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertThat}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class OptionsHintTest(param: Param) extends TableTestBase {
  private val util = param.utilSupplier.apply(this)
  private val is_bounded = param.isBounded

  @Before
  def before(): Unit = {
    util.addTable(s"""
                     |create table t1(
                     |  a int,
                     |  b varchar,
                     |  c as a + 1
                     |) with (
                     |  'connector' = 'OPTIONS',
                     |  '$IS_BOUNDED' = '$is_bounded',
                     |  'k1' = 'v1',
                     |  'k2' = 'v2'
                     |)
       """.stripMargin)

    util.addTable(s"""
                     |create table t2(
                     |  d int,
                     |  e varchar,
                     |  f bigint
                     |) with (
                     |  'connector' = 'OPTIONS',
                     |  '$IS_BOUNDED' = '$is_bounded',
                     |  'k3' = 'v3',
                     |  'k4' = 'v4'
                     |)
       """.stripMargin)
  }

  @Test
  def testOptionsWithGlobalConfDisabled(): Unit = {
    util.tableEnv.getConfig
      .set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, Boolean.box(false))
    expectedException.expect(isA(classOf[ValidationException]))
    expectedException.expectMessage(
      s"OPTIONS hint is allowed only when "
        + s"${TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key} is set to true")
    util.verifyExecPlan("select * from t1/*+ OPTIONS(connector='COLLECTION', k2='#v2') */")
  }

  @Test
  def testInsertWithDynamicOptions(): Unit = {
    val sql =
      s"""
         |insert into t1 /*+ OPTIONS(k1='#v1', k5='v5') */
         |select d, e from t2
         |""".stripMargin
    val stmtSet = util.tableEnv.createStatementSet()
    stmtSet.addInsertSql(sql)
    val testStmtSet = stmtSet.asInstanceOf[StatementSetImpl[_]]
    val relNodes = testStmtSet.getOperations.map(util.getPlanner.translateToRel)
    assertThat(relNodes.length, is(1))
    assert(relNodes.head.isInstanceOf[LogicalLegacySink])
    val sink = relNodes.head.asInstanceOf[LogicalLegacySink]
    assertEquals("{k1=#v1, k2=v2, k5=v5}", sink.sink.asInstanceOf[OptionsTableSink].props.toString)
  }

  @Test
  def testAppendOptions(): Unit = {
    util.verifyExecPlan("select * from t1/*+ OPTIONS(k5='v5', 'a.b.c'='fakeVal') */")
  }

  @Test
  def testOverrideOptions(): Unit = {
    util.verifyExecPlan("select * from t1/*+ OPTIONS(k1='#v1', k2='#v2') */")
  }

  @Test
  def testJoinWithAppendedOptions(): Unit = {
    val sql =
      s"""
         |select * from
         |t1 /*+ OPTIONS(k5='v5', 'a.b.c'='fakeVal') */
         |join
         |t2 /*+ OPTIONS(k6='v6', 'd.e.f'='fakeVal') */
         |on t1.a = t2.d
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithOverriddenOptions(): Unit = {
    val sql =
      s"""
         |select * from
         |t1 /*+ OPTIONS(k1='#v1', k2='#v2') */
         |join
         |t2 /*+ OPTIONS(k3='#v3', k4='#v4') */
         |on t1.a = t2.d
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testOptionsHintOnTableApiView(): Unit = {
    val view1 = util.tableEnv.sqlQuery("select * from t1 join t2 on t1.a = t2.d")
    util.tableEnv.createTemporaryView("view1", view1)
    // The table hints on view expect to be prohibited
    val sql = "select * from view1/*+ OPTIONS(k1='#v1', k2='#v2', k3='#v3', k4='#v4') */"
    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining("View '`default_catalog`.`default_database`.`view1`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testOptionsHintOnSQLView(): Unit = {
    // Equivalent SQL:
    // select * from t1 join t2 on t1.a = t2.d
    val props = new JHashMap[String, String]
    props.put("k1", "v1")
    props.put("k2", "v2")
    props.put("k3", "v3")
    props.put("k4", "v4")
    val view1 =
      CatalogView.of(
        Schema.newBuilder
          .fromResolvedSchema(ResolvedSchema.of(
            Column.physical("a", DataTypes.INT()),
            Column.physical("b", DataTypes.STRING()),
            Column.physical("c", DataTypes.INT()),
            Column.physical("d", DataTypes.INT()),
            Column.physical("e", DataTypes.STRING()),
            Column.physical("f", DataTypes.BIGINT())
          ))
          .build(),
        "a view table",
        "select * from t1 join t2 on t1.a = t2.d",
        "select * from t1 join t2 on t1.a = t2.d",
        props
      )
    val catalog = util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
    catalog.createTable(new ObjectPath(util.tableEnv.getCurrentDatabase, "view1"), view1, false)
    // The table hints on view expect to be prohibited
    val sql = "select * from view1/*+ OPTIONS(k1='#v1', k2='#v2', k3='#v3', k4='#v4') */"
    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining("View '`default_catalog`.`default_database`.`view1`' " +
        "cannot be enriched with new options. Hints can only be applied to tables.")
      .isInstanceOf[ValidationException]
  }

  @Test
  def testOptionsHintInsideView(): Unit = {
    util.tableEnv.executeSql(
      "create view v1 as select * from t1 /*+ OPTIONS(k1='#v111', k4='#v444')*/")
    util.verifyExecPlan(s"""
                           |select * from t2 join v1 on v1.a = t2.d
                           |""".stripMargin)
  }
}

object OptionsHintTest {
  val IS_BOUNDED = "is-bounded"

  case class Param(utilSupplier: TableTestBase => TableTestUtil, isBounded: Boolean) {
    override def toString: String = s"$IS_BOUNDED=$isBounded"
  }

  @Parameters(name = "{index}: {0}")
  def parameters(): Array[Param] = {
    Array(Param(_.batchTestUtil(), isBounded = true), Param(_.streamTestUtil(), isBounded = false))
  }
}
