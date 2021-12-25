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

package org.apache.flink.table.planner.catalog

import com.google.common.collect.Lists
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Schema, Table, TableDescriptor, TableEnvironment, TableException}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.factories.TableFactoryHarness
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil
import org.junit.Assert.assertEquals
import org.junit.rules.ExpectedException
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Rule, Test}

import java.util
import scala.collection.JavaConversions._

/** Test cases for view related DDLs. */
@RunWith(classOf[Parameterized])
class CatalogViewITCase(isStreamingMode: Boolean) extends AbstractTestBase {
  //~ Instance fields --------------------------------------------------------

  private val settings = if (isStreamingMode) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  private val tableEnv: TableEnvironment = TableEnvironmentImpl.create(settings)

  var _expectedEx: ExpectedException = ExpectedException.none

  @Rule
  def expectedEx: ExpectedException = _expectedEx

  @Before
  def before(): Unit = {
    tableEnv.getConfig
      .getConfiguration
      .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1)
    TestCollectionTableFactory.reset()
  }

  //~ Tools ------------------------------------------------------------------

  implicit def rowOrdering: Ordering[Row] = Ordering.by((r : Row) => {
    val builder = new StringBuilder
    0 until r.getArity foreach(idx => builder.append(r.getField(idx)))
    builder.toString()
  })

  def toRow(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  @Test
  def testCreateViewIfNotExistsTwice(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewWith3ColumnDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val viewWith2ColumnDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3(d, e) AS SELECT a, b FROM T1
      """.stripMargin

    val query = "SELECT d, e, f FROM T3"

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewWith3ColumnDDL)
    tableEnv.executeSql(viewWith2ColumnDDL)

    tableEnv.sqlQuery(query).executeInsert("T2").await()
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testCreateViewWithoutFieldListAndWithStar(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE VIEW IF NOT EXISTS T3 AS SELECT * FROM T1
      """.stripMargin

    val query = "SELECT * FROM T3"

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewDDL)

    tableEnv.sqlQuery(query).executeInsert("T2").await()
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testCreateTemporaryView(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    TestCollectionTableFactory.initData(sourceData)

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val viewDDL =
      """
        |CREATE TEMPORARY VIEW T3(d, e, f) AS SELECT a, b, c FROM T1
      """.stripMargin

    val query = "SELECT d, e, f FROM T3"

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(viewDDL)

    tableEnv.sqlQuery(query).executeInsert("T2").await()
    assertEquals(sourceData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  @Test
  def testTemporaryViewMaskPermanentViewWithSameName(): Unit = {
    val sourceData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    val sourceDDL =
      """
        |CREATE TABLE T1(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val sinkDDL =
      """
        |CREATE TABLE T2(
        |  a int,
        |  b varchar,
        |  c int
        |) with (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin

    val permanentView =
      """
        |CREATE VIEW IF NOT EXISTS T3 AS SELECT a, b, c FROM T1
      """.stripMargin

    val permanentViewData = List(
      toRow(1, "1000", 2),
      toRow(2, "1", 3),
      toRow(3, "2000", 4),
      toRow(1, "2", 2),
      toRow(2, "3000", 3))

    val temporaryView =
      """
        |CREATE TEMPORARY VIEW IF NOT EXISTS T3 AS SELECT a, b, c+1 FROM T1
      """.stripMargin

    val temporaryViewData = List(
      toRow(1, "1000", 3),
      toRow(2, "1", 4),
      toRow(3, "2000", 5),
      toRow(1, "2", 3),
      toRow(2, "3000", 4))

    tableEnv.executeSql(sourceDDL)
    tableEnv.executeSql(sinkDDL)
    tableEnv.executeSql(permanentView)
    tableEnv.executeSql(temporaryView)

    TestCollectionTableFactory.initData(sourceData)

    val query = "SELECT * FROM T3"

    tableEnv.sqlQuery(query).executeInsert("T2").await()
    // temporary view T3 masks permanent view T3
    assertEquals(temporaryViewData.sorted, TestCollectionTableFactory.RESULT.sorted)

    TestCollectionTableFactory.reset()
    TestCollectionTableFactory.initData(sourceData)

    val dropTemporaryView =
      """
        |DROP TEMPORARY VIEW IF EXISTS T3
      """.stripMargin
    tableEnv.executeSql(dropTemporaryView)
    tableEnv.sqlQuery(query).executeInsert("T2").await()
    // now we only have permanent view T3
    assertEquals(permanentViewData.sorted, TestCollectionTableFactory.RESULT.sorted)
  }

  private def buildTableDescriptor(): TableDescriptor = {
    val tableDescriptor: TableDescriptor = TableFactoryHarness.newBuilder()
      .boundedScanSource()
      .schema(Schema.newBuilder()
        .column("a", DataTypes.INT())
        .column("b", DataTypes.STRING())
        .column("c", DataTypes.INT())
        .build())
      .sink()
      .build()
    tableDescriptor
  }

  @Test
  def testShowCreateQueryOperationCatalogView(): Unit = {

    val table: Table = tableEnv.from(buildTableDescriptor())
    _expectedEx.expect(classOf[TableException])
    _expectedEx.expectMessage(
      "SHOW CREATE VIEW is not supported for views registered by Table API.")
    tableEnv.createTemporaryView("QueryOperationCatalogView", table)
    tableEnv.executeSql("show create view QueryOperationCatalogView")
  }

  @Test
  def testShowCreateTemporaryView(): Unit = {

    tableEnv.createTable("T1", buildTableDescriptor())

    val tView1DDL: String = "CREATE TEMPORARY VIEW t_v1 AS SELECT a, b, c FROM T1"
    tableEnv.executeSql(tView1DDL)
    val tView1ShowCreateResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv
        .executeSql("show create view t_v1")
        .collect()
    )
    assertEquals(tView1ShowCreateResult, Lists.newArrayList(
      Row.of(
        s"""CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`t_v1`(`a`, `b`, `c`) as
           |SELECT `T1`.`a`, `T1`.`b`, `T1`.`c`
           |FROM `default_catalog`.`default_database`.`T1`"""
          .stripMargin
      )
    ))

    val tView2DDL: String = "CREATE TEMPORARY VIEW t_v2(d, e, f) AS SELECT a, b, c FROM T1"
    tableEnv.executeSql(tView2DDL)
    val tView2ShowCreateResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv
        .executeSql("show create view t_v2")
        .collect()
    )
    assertEquals(tView2ShowCreateResult, Lists.newArrayList(
      Row.of(
        s"""CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`t_v2`(`d`, `e`, `f`) as
           |SELECT `T1`.`a`, `T1`.`b`, `T1`.`c`
           |FROM `default_catalog`.`default_database`.`T1`"""
          .stripMargin
      )
    ))
  }

  @Test
  def testShowCreateCatalogView(): Unit = {

    tableEnv.createTable("T1", buildTableDescriptor())

    val view1DDL: String = "CREATE VIEW v1 AS SELECT a, b, c FROM T1"
    tableEnv.executeSql(view1DDL)
    val view1ShowCreateResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv
        .executeSql("show create view v1")
        .collect()
    )
    assertEquals(view1ShowCreateResult, Lists.newArrayList(
      Row.of(
        s"""CREATE VIEW `default_catalog`.`default_database`.`v1`(`a`, `b`, `c`) as
           |SELECT `T1`.`a`, `T1`.`b`, `T1`.`c`
           |FROM `default_catalog`.`default_database`.`T1`"""
          .stripMargin
      )
    ))

    val view2DDL: String = "CREATE VIEW v2(x, y, z) AS SELECT a, b, c FROM T1"
    tableEnv.executeSql(view2DDL)
    val view2ShowCreateResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv.executeSql("show create view v2")
        .collect()
    )
    assertEquals(view2ShowCreateResult, Lists.newArrayList(
      Row.of(
        s"""CREATE VIEW `default_catalog`.`default_database`.`v2`(`x`, `y`, `z`) as
           |SELECT `T1`.`a`, `T1`.`b`, `T1`.`c`
           |FROM `default_catalog`.`default_database`.`T1`"""
          .stripMargin
      )
    ))
  }

  @Test
  def testShowCreateViewWithLeftJoinGroupBy(): Unit = {
    tableEnv.createTable("t1", buildTableDescriptor())
    tableEnv.createTable("t2", buildTableDescriptor())

    val viewWithLeftJoinGroupByDDL: String =
      s"""create view viewLeftJoinGroupBy as
         |select max(t1.a) max_value
         |from t1 left join t2 on t1.c=t2.c"""
        .stripMargin
    tableEnv.executeSql(viewWithLeftJoinGroupByDDL)
    val showCreateLeftJoinGroupByViewResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv.executeSql("show create view viewLeftJoinGroupBy")
        .collect()
    )
    assertEquals(showCreateLeftJoinGroupByViewResult, Lists.newArrayList(
      Row.of(
        s"""CREATE VIEW `default_catalog`.`default_database`.`viewLeftJoinGroupBy`(`max_value`) as
           |SELECT MAX(`t1`.`a`) AS `max_value`
           |FROM `default_catalog`.`default_database`.`t1`
           |LEFT JOIN `default_catalog`.`default_database`.`t2` ON `t1`.`c` = `t2`.`c`"""
          .stripMargin
      )
    ))
  }

  @Test
  def testShowCreateViewWithUDFOuterJoin(): Unit = {
    tableEnv.createTable("t1", buildTableDescriptor())
    tableEnv.createTable("t2", buildTableDescriptor())
    tableEnv.createTemporarySystemFunction("udfEqualsOne", new ScalarFunction {
      def eval(): Int ={
        1
      }
    })
    val viewWithCrossJoinDDL: String =
      s"""create view viewWithCrossJoin as
         |select udfEqualsOne() a, t1.a a1, t2.b b2 from t1 cross join t2"""
        .stripMargin
    tableEnv.executeSql(viewWithCrossJoinDDL)
    val showCreateCrossJoinViewResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv.executeSql("show create view viewWithCrossJoin")
        .collect()
    )
    assertEquals(showCreateCrossJoinViewResult, Lists.newArrayList(
      Row.of(
        s"""CREATE VIEW `default_catalog`.`default_database`.`viewWithCrossJoin`(`a`, `a1`, `b2`) as
           |SELECT `udfEqualsOne`() AS `a`, `t1`.`a` AS `a1`, `t2`.`b` AS `b2`
           |FROM `default_catalog`.`default_database`.`t1`
           |CROSS JOIN `default_catalog`.`default_database`.`t2`"""
          .stripMargin
      )
    ))
  }

  @Test
  def testShowCreateViewWithInnerJoin(): Unit = {

    tableEnv.createTable("t1", buildTableDescriptor())
    tableEnv.createTable("t2", buildTableDescriptor())
    val viewWithInnerJoinDDL: String =
      s"""create view innerJoinView as
         |select t1.a a1, t2.b b2
         |from t1 inner join t2
         |on t1.c=t2.c"""
        .stripMargin
    tableEnv.executeSql(viewWithInnerJoinDDL)
    val showCreateInnerJoinViewResult: util.List[Row] = CollectionUtil.iteratorToList(
      tableEnv.executeSql("show create view innerJoinView")
        .collect()
    )
    assertEquals(showCreateInnerJoinViewResult, Lists.newArrayList(
      Row.of(
        s"""CREATE VIEW `default_catalog`.`default_database`.`innerJoinView`(`a1`, `b2`) as
           |SELECT `t1`.`a` AS `a1`, `t2`.`b` AS `b2`
           |FROM `default_catalog`.`default_database`.`t1`
           |INNER JOIN `default_catalog`.`default_database`.`t2` ON `t1`.`c` = `t2`.`c`"""
          .stripMargin
      )
    ))
  }

}

object CatalogViewITCase {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    util.Arrays.asList(true, false)
  }
}

