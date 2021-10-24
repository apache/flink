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

package org.apache.flink.table.api.sql.ddl

import com.google.common.collect.Lists
import org.apache.calcite.sql.validate.SqlValidatorException
import org.apache.flink.core.testutils.FlinkAssertions
import org.apache.flink.table.api.ColumnPosition.ReferencedColumnNotFoundException
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment, ValidationException}
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.types.Row
import org.apache.flink.util.{CollectionUtil, TestLogger}
import org.assertj.core.api.Assertions
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Assert, Rule, Test}

import _root_.java.lang.{Boolean => JBoolean, String => JStr}
import _root_.java.util

/**
 * Test Alter Add Column Components SQL statements
 * which executed by [[TableEnvironment#executeSql()]]
 */
@RunWith(classOf[Parameterized])
class ExecuteAlterAddColumnComponentsSqlTest(isStreaming: Boolean,
                                             pTheseWrapper: Boolean) extends TestLogger {

  // Expected rows
  private val numRow = Row.of("num", "INT", new JBoolean(false), null, null, null)
  private val costRow =
    Row.of("cost", "INT", new JBoolean(false), null, null, null)
  private val userRowPri =
    Row.of("user", "BIGINT", new JBoolean(false), "PRI(user)", null, null)
  private val userRow =
    Row.of("user", "BIGINT", new JBoolean(false), null, null, null)
  private val prodRow =
    Row.of("product", "VARCHAR(32)", new JBoolean(true), null, null, null)
  private val amountRow =
    Row.of("amount", "INT", new JBoolean(true), null, null, null)
  private val tsRow =
    Row.of("ts", "TIMESTAMP(3)", new JBoolean(true), null, null, null)
  private val num1Row =
    Row.of("num1", "INT", new JBoolean(false), "PRI(num1)", null, null)
  private val num1RowPri =
    Row.of("num1", "INT", new JBoolean(false), null, null, null)
  private val wmTs = Row.of(
      "ts", getRowTimeByEnvMode(), new JBoolean(true), null, null,
      "`ts` - INTERVAL '1' SECOND"
  )
  private val mc1Row = Row.of(
    "mc1", "TIMESTAMP_LTZ(3)", new JBoolean(false),
    null, "METADATA FROM 'timestamp' VIRTUAL", null
  )
  private val mc2Row = Row.of(
    "mc2", "TIMESTAMP_LTZ(3)", new JBoolean(true),
    null, "METADATA FROM 'timestamp' VIRTUAL", null
  )
  private val mc3Row = Row.of(
    "mc3", "TIMESTAMP_LTZ(3)", new JBoolean(false),
    null, "METADATA FROM 'timestamp' VIRTUAL", null
  )
  private val cmpRow =
    Row.of("cmp", "BIGINT", new JBoolean(false), null, "AS `user` - 1", null)
  private val cmp1Row =
    Row.of("cmp1", "INT", new JBoolean(true), null, "AS `cost` - `amount`", null)
  private val cmp2Row =
    Row.of("cmp2", "INT", new JBoolean(true), null, "AS `cost` - `amount`", null)
  private val cmp3Row =
    Row.of("cmp3", "INT", new JBoolean(false), null, "AS `cost` - 1", null)
  private val phyConstrRow = Row.of(
    "physical_constraint_col", "INT", new JBoolean(false),
    "PRI(physical_constraint_col)", null, null
  )

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  private val settings = if (isStreaming) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  var tEnv: TableEnvironment = TableEnvironment.create(settings)

  private def getRowTimeByEnvMode(): String = {
    if (isStreaming) {
      "TIMESTAMP(3) *ROWTIME*"
    } else {
      "TIMESTAMP(3)"
    }
  }

  private def initTableAndView(): Unit = {
    val createClause: String =
      s"""
         |CREATE TABLE IF NOT EXISTS orders (
         | `user` BIGINT NOT NULl,
         | `product` VARCHAR(32),
         | `amount` INT,
         | `ts` TIMESTAMP(3),
         | PRIMARY KEY(`user`) NOT ENFORCED
         |) """.stripMargin
    var createWithClause: String =
      s"""
         |with (
         | 'connector' = 'datagen'
         |)""".stripMargin
    if (!isStreaming) {
      val sinkPath = _tempFolder.newFolder().toString
      createWithClause =
        s"""
           |with (
           |  'connector' = 'filesystem',
           |  'path' = '$sinkPath',
           |  'format' = 'testcsv'
           |)""".stripMargin
    }
    tEnv.executeSql(createClause + createWithClause)
    tEnv.executeSql("create view orders_view as select * from orders")
  }

  private def processSqlForErrorCases(targetSqlPattern: String, prepareSqls: String*): Unit = {
    initTableAndView()
    if (prepareSqls != null) {
      prepareSqls.foreach(sql => tEnv.executeSql(sql))
    }
    if (pTheseWrapper) {
      tEnv.executeSql(JStr.format(targetSqlPattern, "(", ")"))
    } else {
      tEnv.executeSql(JStr.format(targetSqlPattern, "", ""))
    }
  }

  @Test
  def testAddWMForView(): Unit = {
    val sqlPat = "alter table orders_view add %s watermark for ts as ts - interval '1' second %s"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(sqlPat)
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "ALTER TABLE for a view is not allowed"
      )
  }

  @Test
  def testAddWMForNonexistField(): Unit = {
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(
          "alter table orders add %s watermark " +
            "for non_f as non_f - interval '1' second %s"
      )
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "The rowtime attribute field 'non_f' is not defined in the table"
      )
  }

  @Test
  def testAddWMForDuplicated(): Unit = {
    val sqlPat = "alter table orders add %s watermark for ts as ts - interval '1' second %s"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(sqlPat, JStr.format(sqlPat, "", ""))
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "There already exists a watermark spec for column 'ts' in the base table."
      )
  }

  @Test
  def testAddWMForNormal(): Unit = {
    initTableAndView()
    tEnv
      .executeSql("alter table orders add watermark for ts as ts - interval '1' second")
    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      userRowPri, prodRow, amountRow, wmTs
    )
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
  }

  @Test
  def testAlterTableAddMetadataCol(): Unit = {
    initTableAndView()
    val sql =
      "alter table orders add %s timestamp_ltz(3) %s metadata %s virtual comment 'mc_cmt' %s"
    val sql1 = JStr.format(sql, "mc1", "not null", "from 'timestamp'", "")
    tEnv.executeSql(sql1)
    val sql2 = JStr.format(sql, "mc2", "", "from 'timestamp'", "first")
    tEnv.executeSql(sql2)
    val sql3 = JStr.format(sql, "mc3", "not null", "from 'timestamp'", "after `mc2`")
    tEnv.executeSql(sql3)

    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("desc orders")
        .collect()
    )
    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      mc2Row, mc3Row, userRowPri, prodRow, amountRow, tsRow, mc1Row
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
  }

  @Test
  def testAddMetaColForView(): Unit = {
    val sql = "alter table orders_view add %s mc1 timestamp_ltz(3) not null " +
      "metadata from 'timestamp' virtual comment 'mc_cmt' %s"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(sql)
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "ALTER TABLE for a view is not allowed"
      )
  }

  @Test
  def testAddMetaColDuplicated(): Unit = {
    val sql = "alter table orders add %s amount timestamp_ltz(3) not null " +
      "metadata from 'timestamp' virtual comment 'mc_cmt' %s"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(sql)
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "A column named 'amount' already exists in the table"
      )
  }

  @Test
  def testAlterTableAddComputedCol(): Unit = {
    initTableAndView()
    tEnv.executeSql("alter table orders add cost int not null")

    val sql = "alter table orders add %s comment 'c' %s"
    val sql1 = JStr.format(sql, "cmp1 as (cost - amount)", "")
    tEnv.executeSql(sql1)
    val sql2 = JStr.format(sql, "cmp2 as (cost - amount)", "first")
    tEnv.executeSql(sql2)
    val sql3 = JStr.format(sql, "cmp3 as (cost - 1)", "after cmp2")
    tEnv.executeSql(sql3)

    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("desc orders")
        .collect()
    )
    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      cmp2Row, cmp3Row, userRowPri, prodRow, amountRow, tsRow, costRow, cmp1Row
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
  }

  @Test
  def testAddComputedColForView(): Unit = {
    val sql = "alter table %s add %s %s comment 'c' %s %s"
    val sqlPat = JStr.format(sql, "orders_view", "%s", "cmp1 as (cost - amount)", "", "%s")
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(
        sqlPat,
        JStr.format(sql, "orders", "", "cost int not null", "", "")
      )
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "ALTER TABLE for a view is not allowed"
      )
  }

  @Test
  def testAddComputedColDuplicated(): Unit = {
    val sql = "alter table %s add %s %s comment 'c' %s %s"
    val sqlPat = JStr.format(sql, "orders", "%s", "amount as (`user` - amount)", "first", "%s")
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(
        sqlPat,
        JStr.format(sql, "orders", "", "cost int not null", "", "")
      )
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "A column named 'amount' already exists in the table"
      )
  }

  @Test
  def testAddComputedColUnknown(): Unit = {
    val sql = "alter table %s add %s %s comment 'c' %s %s"
    val sqlPat = JStr.format(sql, "orders", "%s", "cmp1 as (non_col - 1)", "", "%s")
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(
        sqlPat,
        JStr.format(sql, "orders", "", "cost int not null", "", "")
      )
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[SqlValidatorException],
        "Unknown identifier 'non_col'"
      )
  }

  @Test
  def testAddDuplicatedPhyCol(): Unit = {
    initTableAndView()
    val sqlPat = "alter table orders add %s `user` int not null %s"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(sqlPat)
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "Could not execute CreateTable in path " +
          "`default_catalog`.`default_database`.`orders_view`"
      )
  }

  @Test
  def testAddPhyColNotFoundReference(): Unit = {
    val sqlPat = "alter table orders add %s col_err int not null " +
      "comment 'err' after non_exist %s"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(sqlPat)
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ReferencedColumnNotFoundException],
        "The column 'non_exist' referenced by column 'col_err' was not found."
      )
  }

  @Test
  def testAddPhyColForView(): Unit = {
    val addColToView = "alter table orders_view add col int not null comment 'err' after non_exist"
    Assertions.assertThatThrownBy(
      () => processSqlForErrorCases(addColToView)
    ) satisfies FlinkAssertions
      .anyCauseMatches(
        classOf[ValidationException],
        "ALTER TABLE for a view is not allowed"
      )
  }

  @Test
  def testAlterTableAddPhysicalCol(): Unit = {
    initTableAndView()
    val addCol1 = "alter table orders add num int not null comment 'num' first"
    val addCol2 = "alter table orders add cost int not null comment 'cost' after num"
    val addCol3 = "alter table orders add num1 int not null comment 'num1'"
    tEnv.executeSql(addCol1)
    tEnv.executeSql(addCol2)
    tEnv.executeSql(addCol3)
    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      numRow, costRow, userRowPri, prodRow, amountRow, tsRow, num1RowPri
    )
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
  }

  @Test
  def testAlterTableAddColumnWithConstraint(): Unit = {
    initTableAndView()
    val pk = tEnv.getCatalog("default_catalog").get()
      .getTable(new ObjectPath("default_database", "orders"))
      .getUnresolvedSchema.getPrimaryKey.get().getConstraintName
    tEnv.executeSql("alter table orders drop constraint " + pk)
    tEnv.executeSql("alter table orders add physical_constraint_col " +
      "int not null primary key not enforced first")
    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      phyConstrRow, userRow, prodRow, amountRow, tsRow
    )
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("show columns from orders")
        .collect()
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
  }

  @Test
  def testAlterTableAddColumnsNormal(): Unit = {
    initTableAndView()
    val pk = tEnv.getCatalog("default_catalog").get()
      .getTable(new ObjectPath("default_database", "orders"))
      .getUnresolvedSchema.getPrimaryKey.get().getConstraintName
    tEnv.executeSql("alter table orders drop constraint " + pk)

    val sql = "alter table orders add (" +
      "num int not null comment 'num' first," +
      "num1 int not null comment 'num1'," +
      "watermark for ts as ts - interval '1' second," +
      "mc1 timestamp_ltz(3) not null metadata from 'timestamp' virtual comment 'c' first," +
      "cmp as (`user` - 1) comment 'c' after mc1," +
      "constraint ck1 primary key(num1)" +
      ")"
    tEnv.executeSql(sql)
    val resultsWithFrom: util.List[Row] = CollectionUtil.iteratorToList(
      tEnv
        .executeSql("desc orders")
        .collect()
    )
    val expectedResultRows: util.List[Row] = Lists.newArrayList(
      mc1Row, cmpRow, numRow, userRow, prodRow, amountRow, wmTs, num1Row
    )
    Assert.assertEquals(expectedResultRows, resultsWithFrom)
  }
}

object ExecuteAlterAddColumnComponentsSqlTest {
  @Parameterized.Parameters(name = "isStream={0}, pTheseWrapper={1}")
  def parameters(): util.Collection[Array[JBoolean]] = {
    util.Arrays.asList(
      Array(true, true),
      Array(true, false),
      Array(false, true),
      Array(false, false)
    )
  }
}
