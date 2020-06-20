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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.{DataTypes, TableSchema, Types}
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase.TEMPORARY_FOLDER
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTestBase, TestData}
import org.apache.flink.table.planner.utils.{TestDataTypeTableSource, TestFileInputFormatTableSource, TestFilterableTableSource, TestInputFormatTableSource, TestNestedProjectableTableSource, TestPartitionableSourceFactory, TestLegacyProjectableTableSource, TestTableSourceSinks}
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter
import org.apache.flink.types.Row

import org.junit.{Before, Test}

import java.io.FileWriter
import java.lang.{Boolean => JBool, Integer => JInt, Long => JLong}
import java.math.{BigDecimal => JDecimal}
import java.time.{Instant, LocalDateTime, ZoneId}

import scala.collection.mutable

class LegacyTableSourceITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    env.setParallelism(1) // set sink parallelism to 1
    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c"),
      Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING())).build()
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
      "MyTable", new TestLegacyProjectableTableSource(
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

    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(
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
    TestFilterableTableSource.createTemporaryTable(
      tEnv,
      TestFilterableTableSource.defaultSchema,
      "FilterableTable",
      isBounded = true)
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
  def testTableSourceWithFunctionFilterable(): Unit = {
    TestFilterableTableSource.createTemporaryTable(
      tEnv,
      TestFilterableTableSource.defaultSchema,
      "FilterableTable",
      isBounded = true,
      filterableFields = List("amount", "name"))
    checkResult(
      "SELECT id, name FROM FilterableTable " +
        "WHERE amount > 4 AND price < 9 AND upper(name) = 'RECORD_5'",
      Seq(
        row(5, "Record_5"))
    )
  }

  @Test
  def testTableSourceWithPartitionable(): Unit = {
    TestPartitionableSourceFactory.createTemporaryTable(tEnv, "PartitionableTable", true)
    checkResult(
      "SELECT * FROM PartitionableTable WHERE part2 > 1 and id > 2 AND part1 = 'A'",
      Seq(row(3, "John", "A", 2), row(4, "nosharp", "A", 2))
    )
  }

  @Test
  def testCsvTableSource(): Unit = {
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "csvTable")
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
    TestTableSourceSinks.createOrdersCsvTemporaryTable(tEnv, "orders")
    TestTableSourceSinks.createRatesCsvTemporaryTable(tEnv, "rates")

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

  @Test
  def testInputFormatSource(): Unit = {
    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c"),
      Array(DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING())).build()
    TestInputFormatTableSource.createTemporaryTable(
      tEnv, tableSchema, TestData.smallData3, "MyInputFormatTable")
    checkResult(
      "SELECT a, c FROM MyInputFormatTable",
      Seq(
        row(1, "Hi"),
        row(2, "Hello"),
        row(3, "Hello world"))
    )
  }

  @Test
  def testMultiTypeSource(): Unit = {
    val tableSchema = TableSchema.builder().fields(
      Array("a", "b", "c", "d", "e", "f"),
      Array(
        DataTypes.INT(),
        DataTypes.DECIMAL(5, 2),
        DataTypes.VARCHAR(5),
        DataTypes.CHAR(5),
        DataTypes.TIMESTAMP(9),
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)
      )
    ).build()

    val ints = List(1, 2, 3, 4, null)
    val decimals = List(
      new JDecimal(5.1), new JDecimal(6.1), new JDecimal(7.1), new JDecimal(8.123), null)
    val varchars = List("1", "12", "123", "1234", null)
    val chars = List("1", "12", "123", "1234", null)
    val datetimes = List(
      LocalDateTime.of(1969, 1, 1, 0, 0, 0, 123456789),
      LocalDateTime.of(1970, 1, 1, 0, 0, 0, 123456000),
      LocalDateTime.of(1971, 1, 1, 0, 0, 0, 123000000),
      LocalDateTime.of(1972, 1, 1, 0, 0, 0, 0),
      null)

    val instants = new mutable.MutableList[Instant]
    for (i <- datetimes.indices) {
      if (datetimes(i) == null) {
        instants += null
      } else {
        // Assume the time zone of source side is UTC
        instants += datetimes(i).toInstant(ZoneId.of("UTC").getRules.getOffset(datetimes(i)))
      }
    }

    val data = new mutable.MutableList[Row]

    for (i <- ints.indices) {
      data += row(
        ints(i), decimals(i), varchars(i), chars(i), datetimes(i), instants(i))
    }

    TestDataTypeTableSource.createTemporaryTable(tEnv, tableSchema, "MyInputFormatTable", data.seq)

    checkResult(
      "SELECT a, b, c, d, e, f FROM MyInputFormatTable",
      Seq(
        row(1, "5.10", "1", "1",
          "1969-01-01T00:00:00.123456789",
          "1969-01-01T00:00:00.123456789Z"),
        row(2, "6.10", "12", "12",
          "1970-01-01T00:00:00.123456",
          "1970-01-01T00:00:00.123456Z"),
        row(3, "7.10", "123", "123",
          "1971-01-01T00:00:00.123",
          "1971-01-01T00:00:00.123Z"),
        row(4, "8.12", "1234", "1234",
          "1972-01-01T00:00",
          "1972-01-01T00:00:00Z"),
        row(null, null, null, null, null, null))
    )
  }

  @Test
  def testMultiPaths(): Unit = {
    val tmpFile1 = TEMPORARY_FOLDER.newFile("tmpFile1.tmp")
    new FileWriter(tmpFile1).append("t1\n").append("t2\n").close()

    val tmpFile2 = TEMPORARY_FOLDER.newFile("tmpFile2.tmp")
    new FileWriter(tmpFile2).append("t3\n").append("t4\n").close()

    val schema = new TableSchema(Array("a"), Array(Types.STRING))
    val paths = Array(tmpFile1.getPath, tmpFile2.getPath)

    TestFileInputFormatTableSource.createTemporaryTable(tEnv, schema, "MyMultiPathTable", paths)
    checkResult(
      "select * from MyMultiPathTable",
      Seq(
        row("t1"),
        row("t2"),
        row("t3"),
        row("t4")
      )
    )
  }
}
