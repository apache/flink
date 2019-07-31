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

package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, LocalTimeTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{DataTypes, TableSchema, Types, ValidationException}
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.utils.{DateTimeTestUtil, TableTestBase, TestFilterableTableSource, TestNestedProjectableTableSource, TestPartitionableTableSource, TestProjectableTableSource, TestTableSource}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.junit.{Before, Test}

class TableSourceTest extends TableTestBase {

  private val util = batchTestUtil()
  private val tableSchema = TableSchema.builder().fields(
    Array("a", "b", "c"),
    Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING())).build()

  @Before
  def setup(): Unit = {
    util.tableEnv.registerTableSource("ProjectableTable", new TestProjectableTableSource(
      true,
      tableSchema,
      new RowTypeInfo(
        tableSchema.getFieldDataTypes.map(TypeInfoDataTypeConverter.fromDataTypeToTypeInfo),
        tableSchema.getFieldNames),
      Seq.empty[Row])
    )
    util.tableEnv.registerTableSource("FilterableTable", TestFilterableTableSource(true))
    util.tableEnv.registerTableSource("PartitionableTable", new TestPartitionableTableSource(true))
  }

  @Test
  def testBoundedStreamTableSource(): Unit = {
    util.tableEnv.registerTableSource("MyTable", new TestTableSource(true, tableSchema))
    util.verifyPlan("SELECT * FROM MyTable")
  }

  @Test
  def testUnboundedStreamTableSource(): Unit = {
    util.tableEnv.registerTableSource("MyTable", new TestTableSource(false, tableSchema))
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage("Only bounded StreamTableSource can be used in batch mode.")
    util.verifyPlan("SELECT * FROM MyTable")
  }

  @Test
  def testNonStreamTableSource(): Unit = {
    val tableSource = new TableSource[Row]() {

      override def getProducedDataType: DataType = tableSchema.toRowDataType

      override def getTableSchema: TableSchema = tableSchema
    }
    util.tableEnv.registerTableSource("MyTable", tableSource)
    thrown.expect(classOf[ValidationException])
    thrown.expectMessage(
      "Only StreamTableSource and LookupableTableSource can be used in Blink planner.")
    util.verifyPlan("SELECT * FROM MyTable")
  }

  @Test
  def testSimpleProject(): Unit = {
    util.verifyPlan("SELECT a, c FROM ProjectableTable")
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    util.verifyPlan("SELECT COUNT(1) FROM ProjectableTable")
  }

  @Test
  def testNestedProject(): Unit = {
    val nested1 = new RowTypeInfo(
      Array(Types.STRING, Types.INT).asInstanceOf[Array[TypeInformation[_]]],
      Array("name", "value")
    )

    val nested2 = new RowTypeInfo(
      Array(Types.INT, Types.BOOLEAN).asInstanceOf[Array[TypeInformation[_]]],
      Array("num", "flag")
    )

    val deepNested = new RowTypeInfo(
      Array(nested1, nested2).asInstanceOf[Array[TypeInformation[_]]],
      Array("nested1", "nested2")
    )

    val tableSchema = new TableSchema(
      Array("id", "deepNested", "nested", "name"),
      Array(Types.INT, deepNested, nested1, Types.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.INT, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "deepNested", "nested", "name"))

    util.tableEnv.registerTableSource(
      "T",
      new TestNestedProjectableTableSource(true, tableSchema, returnType, Seq()))

    val sqlQuery =
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM T
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterCanPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2")
  }

  @Test
  def testFilterCannotPushDown(): Unit = {
    // TestFilterableTableSource only accept predicates with `amount`
    util.verifyPlan("SELECT * FROM FilterableTable WHERE price > 10")
  }

  @Test
  def testFilterPartialPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND price > 10")
  }

  @Test
  def testFilterFullyPushDown(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND amount < 10")
  }

  @Test
  def testFilterCannotPushDown2(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 OR price > 10")
  }

  @Test
  def testFilterCannotPushDown3(): Unit = {
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 OR amount < 10")
  }

  @Test
  def testFilterPushDownUnconvertedExpression(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM FilterableTable WHERE
        |    amount > 2 AND id < 100 AND CAST(amount AS BIGINT) > 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilterPushDownWithUdf(): Unit = {
    util.addFunction("myUdf", Func1)
    util.verifyPlan("SELECT * FROM FilterableTable WHERE amount > 2 AND myUdf(amount) < 32")
  }

  @Test
  def testPartitionTableSource(): Unit = {
    util.verifyPlan("SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A' ")
  }

  @Test
  def testPartitionTableSourceWithUdf(): Unit = {
    util.addFunction("MyUdf", Func1)
    util.verifyPlan("SELECT * FROM PartitionableTable WHERE id > 2 AND MyUdf(part2) < 3")
  }

  @Test
  def testTimeLiteralExpressionPushDown(): Unit = {
    val rowTypeInfo = new RowTypeInfo(
      Array[TypeInformation[_]](
        BasicTypeInfo.INT_TYPE_INFO,
        LocalTimeTypeInfo.LOCAL_DATE,
        LocalTimeTypeInfo.LOCAL_TIME,
        LocalTimeTypeInfo.LOCAL_DATE_TIME
      ),
      Array("id", "dv", "tv", "tsv")
    )

    val row = new Row(4)
    row.setField(0, 1)
    row.setField(1, DateTimeTestUtil.localDate("2017-01-23"))
    row.setField(2, DateTimeTestUtil.localTime("14:23:02"))
    row.setField(3, DateTimeTestUtil.localDateTime("2017-01-24 12:45:01.234"))

    val tableSource = TestFilterableTableSource(
      isBounded = true, rowTypeInfo, Seq(row), Set("dv", "tv", "tsv"))
    util.tableEnv.registerTableSource("FilterableTable1", tableSource)

    val sqlQuery =
      s"""
         |SELECT id FROM FilterableTable1 WHERE
         |  tv > TIME '14:25:02' AND
         |  dv > DATE '2017-02-03' AND
         |  tsv > TIMESTAMP '2017-02-03 14:25:02.000'
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
