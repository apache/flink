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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{DataTypes, TableSchema, Types}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.table.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.utils.{BatchTestBase, TestData}
import org.apache.flink.table.util.{TestFilterableTableSource, TestNestedProjectableTableSource, TestPartitionableTableSource, TestProjectableTableSource, TestTableSources}
import org.apache.flink.types.Row

import org.junit.{Before, Ignore, Test}

import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}


class TableSourceITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c"),
      Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.VARCHAR(Int.MaxValue))).build()
    tEnv.registerTableSource("MyTable", new TestProjectableTableSource(
      true,
      tableSchema,
      new RowTypeInfo(
        tableSchema.getFieldDataTypes.map(TypeInfoDataTypeConverter.fromDataTypeToTypeInfo),
        tableSchema.getFieldNames),
      TestData.smallData3)
    )
  }

  @Test
  def testSimpleProject(): Unit = {
    checkResult(
      "SELECT a, c FROM MyTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testProjectWithoutInputRef(): Unit = {
    checkResult(
      "SELECT COUNT(*) FROM MyTable",
      Seq(row(3))
    )
  }

  @Test
  def testNestedProject(): Unit = {
    val data = Seq(
      Row.of(new JLong(1),
        Row.of(
          Row.of("Sarah", new JInt(100)),
          Row.of(new JInt(1000), new JBool(true))
        ),
        Row.of("Peter", new JInt(10000)),
        "Mary"),
      Row.of(new JLong(2),
        Row.of(
          Row.of("Rob", new JInt(200)),
          Row.of(new JInt(2000), new JBool(false))
        ),
        Row.of("Lucy", new JInt(20000)),
        "Bob"),
      Row.of(new JLong(3),
        Row.of(
          Row.of("Mike", new JInt(300)),
          Row.of(new JInt(3000), new JBool(true))
        ),
        Row.of("Betty", new JInt(30000)),
        "Liz"))

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
      Array(Types.LONG, deepNested, nested1, Types.STRING))

    val returnType = new RowTypeInfo(
      Array(Types.LONG, deepNested, nested1, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("id", "deepNested", "nested", "name"))

    tEnv.registerTableSource(
      "T",
      new TestNestedProjectableTableSource(true, tableSchema, returnType, data))

    checkResult(
      """
        |SELECT id,
        |    deepNested.nested1.name AS nestedName,
        |    nested.`value` AS nestedValue,
        |    deepNested.nested2.flag AS nestedFlag,
        |    deepNested.nested2.num AS nestedNum
        |FROM T
      """.stripMargin,
      Seq(row(1, "Sarah", 10000, true, 1000),
        row(2, "Rob", 20000, false, 2000),
        row(3, "Mike", 30000, true, 3000)
      )
    )
  }

  @Test
  def testTableSourceWithFilterable(): Unit = {
    tEnv.registerTableSource("FilterableTable", TestFilterableTableSource(true))
    checkResult(
      "SELECT id, name FROM FilterableTable WHERE amount > 4 AND price < 9",
      Seq(
        row(5, "Record_5"),
        row(6, "Record_6"),
        row(7, "Record_7"),
        row(8, "Record_8"))
    )
  }

  @Test
  def testTableSourceWithPartitionable(): Unit = {
    tEnv.registerTableSource("PartitionableTable", new TestPartitionableTableSource(true))
    checkResult(
      "SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A'",
      Seq(row(3, "John", "A", 2), row(4, "nosharp", "A", 2))
    )
  }

  @Test
  def testCsvTableSource(): Unit = {
    val csvTable = TestTableSources.getPersonCsvTableSource
    tEnv.registerTableSource("csvTable", csvTable)
    checkResult(
      "SELECT id, `first`, `last`, score FROM csvTable",
      Seq(
        row(1, "Mike", "Smith", 12.3),
        row(2, "Bob", "Taylor", 45.6),
        row(3, "Sam", "Miller", 7.89),
        row(4, "Peter", "Smith", 0.12),
        row(5, "Liz", "Williams", 34.5),
        row(6, "Sally", "Miller", 6.78),
        row(7, "Alice", "Smith", 90.1),
        row(8, "Kelly", "Williams", 2.34)
      )
    )
  }

  @Test
  def testLookupJoinCsvTemporalTable(): Unit = {
    val orders = TestTableSources.getOrdersCsvTableSource
    val rates = TestTableSources.getRatesCsvTableSource
    tEnv.registerTableSource("orders", orders)
    tEnv.registerTableSource("rates", rates)

    val sql =
      """
        |SELECT o.amount, o.currency, r.rate
        |FROM (SELECT *, PROCTIME() as proc FROM orders) AS o
        |JOIN rates FOR SYSTEM_TIME AS OF o.proc AS r
        |ON o.currency = r.currency
      """.stripMargin

    checkResult(
      sql,
      Seq(
        row(2, "Euro", 119),
        row(1, "US Dollar", 102),
        row(50, "Yen", 1),
        row(3, "Euro", 119),
        row(5, "US Dollar", 102)
      )
    )
  }
}
