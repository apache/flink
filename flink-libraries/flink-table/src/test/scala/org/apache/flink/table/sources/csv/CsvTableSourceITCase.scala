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

package org.apache.flink.table.sources.csv

import java.io.File
import java.util.Collections

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.util.DateTimeTestUtil._
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

class CsvTableSourceITCase extends BatchTestBase {

  def createTmpCsvFile(content: String): String = {
    val file = File.createTempFile("csvTest", ".csv")
    file.deleteOnExit()
    val filePath = file.getAbsolutePath

    import java.nio.file._
    Files.write(Paths.get(filePath), content.getBytes("UTF-8"))

    filePath
  }

  // NOTE: our csv parser is quite fussy about spaces and newlines.
  // try to adhere to the format of  (line LF)*

  private val cars_csv =
    """2012,"Tesla","S","No comment"
      |1997,Ford,E350,"Go get one now they are going fast"
      |2015,Chevy,Volt,""
      |""".stripMargin

  private val cars_csv_null =
    """2012,"Tesla","S","No comment"
      |,Ford,,"fast"
      |2015,Chevy,Volt,""
      |""".stripMargin

  def checkQuery(
      csvContent: String,
      conf: CsvTableSource.Builder => CsvTableSource.Builder,
      sql: String,
      expected: String)
    : Unit = {

    val csvSource = conf(CsvTableSource.builder())
      .path(createTmpCsvFile(csvContent))
      .field("yr", DataTypes.INT)
      .field("make", DataTypes.STRING)
      .field("model", DataTypes.STRING)
      .field("comment", DataTypes.STRING)
      .build()
    tEnv.registerTableSource("cars", csvSource)

    val results = tEnv.sqlQuery(sql).collect()

    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testTemporalJoinCsv(): Unit = {
    val csvSource = CsvTableSource.builder()
      .path(createTmpCsvFile(cars_csv_null))
      .field("yr", DataTypes.INT)
      .field("make", DataTypes.STRING)
      .field("model", DataTypes.STRING)
      .field("comment", DataTypes.STRING)
      .enableEmptyColumnAsNull()
      .build()
    tEnv.registerTableSource("cars", csvSource)

    val csvTemporal = CsvTableSource.builder()
      .path(createTmpCsvFile(cars_csv))
      .field("yr", DataTypes.INT)
      .field("make", DataTypes.STRING)
      .field("model", DataTypes.STRING)
      .field("comment", DataTypes.STRING)
      .uniqueKeys(Collections.singleton(Collections.singleton("make")))
      .build()
    tEnv.registerTableSource("carsTemporal", csvTemporal)

    val sql =
      """
        |SELECT C.*, T.*
        |FROM cars AS C
        |LEFT JOIN carsTemporal FOR SYSTEM_TIME AS OF PROCTIME() AS T
        |ON C.make = T.make
      """.stripMargin

    val results = tEnv.sqlQuery(sql).collect()
    val expected = List(
      "2012,\"Tesla\",\"S\",\"No comment\",2012,\"Tesla\",\"S\",\"No comment\"",
      "null,Ford,null,\"fast\",1997,Ford,E350,\"Go get one now they are going fast\"",
      "2015,Chevy,Volt,\"\",2015,Chevy,Volt,\"\"")
    Assert.assertEquals(results.map(_.toString).sorted, expected.sorted)
  }

  @Test
  def testSimpleCsv(): Unit = {
    checkQuery(
      cars_csv,
      _.quoteCharacter('"'),
      "select yr, make from cars",
      "2012,Tesla\n1997,Ford\n2015,Chevy")
  }

  @Test
  def testSimpleCsvWithLimitPushdown(): Unit = {
    checkQuery(
      cars_csv,
      _.quoteCharacter('"'),
      "select yr, make from cars limit 1",
      "2012,Tesla")
  }

  @Test
  def testEmptyCsv(): Unit = {
    val empty_csv = ""
    checkQuery(
      empty_csv,
      _.quoteCharacter('"'),
      "select yr, make from cars",
      "\n")
  }

  @Test
  def testWriteNullValueCsv(): Unit = {
    val csvSource = CsvTableSource.builder()
      .quoteCharacter('"')
      .path(createTmpCsvFile(cars_csv_null))
      .field("yr", DataTypes.INT)
      .field("make", DataTypes.STRING)
      .field("model", DataTypes.STRING)
      .field("comment", DataTypes.STRING)
      .build()
    tEnv.registerTableSource("cars", csvSource)

    val results = tEnv.sqlQuery("select yr, make, comment from cars")

    val outPath = {
      val tmpFile = File.createTempFile("testCsvTableSink", ".csv")
      tmpFile.deleteOnExit()
      tmpFile.toURI.toString
    }

    val sink = new CsvTableSink(
      outPath, Some(","), None, None, None, Some(WriteMode.OVERWRITE), None, None)
    results.writeToSink(sink)
    tEnv.execute()

    val expected = "2012,Tesla,No comment" +
      "\n,Ford,fast\n2015,Chevy,"
    TestBaseUtils.compareResultsByLinesInMemory(expected, outPath)
  }

  @Test // quotes in file are part of string value
  def testNotQuoted(): Unit = {
    checkQuery(
      cars_csv,
      b => b,
      "select yr, make from cars",
      "2012,\"Tesla\"\n1997,Ford\n2015,Chevy")
  }

  @Test
  def testQuoteAndDelimiter(): Unit = {
    val cars_alternative_csv =
      """2012|'Tesla'|'S'| 'No comment'
        |1997|Ford|E350|'Go get one now they are going fast'
        |2015|Chevy
        |""".stripMargin
    checkQuery(
      cars_alternative_csv,
      _.quoteCharacter('\'').fieldDelimiter("|"),
      "select yr, make from cars",
      "2012,Tesla\n1997,Ford\n2015,Chevy")
  }

  @Test // more columns in file than schema
  def testExtraColumnsInFile(): Unit = {
    val cars_malformed_csv =
      """~ All the rows here are malformed having tokens more than the schema (header).
        |2012,"Tesla","S","No comment",,null,null
        |1997,Ford,E350,"Go get one now they are going fast",,null,null
        |2015,Chevy,,,,
        |""".stripMargin
    checkQuery(
      cars_malformed_csv,
      _.commentPrefix("~").quoteCharacter('"'),
      "select yr, make from cars",
      "2012,Tesla\n1997,Ford\n2015,Chevy")
  }

  @Test
  def testSimpleSave(): Unit = {
    val csvSource = CsvTableSource.builder()
      .quoteCharacter('"')
      .path(createTmpCsvFile(cars_csv))
      .field("yr", DataTypes.INT)
      .field("make", DataTypes.STRING)
      .field("model", DataTypes.STRING)
      .field("comment", DataTypes.STRING)
      .build()
    tEnv.registerTableSource("cars", csvSource)

    val results = tEnv.sqlQuery("select yr, make, comment from cars")

    val outPath = {
      val tmpFile = File.createTempFile("testCsvTableSink", ".csv")
      tmpFile.deleteOnExit()
      tmpFile.toURI.toString
    }

    val sink = new CsvTableSink(
      outPath, Some(","), None, None, None, Some(WriteMode.OVERWRITE), None, None)
    results.writeToSink(sink)
    tEnv.execute()

    val expected = "2012,Tesla,No comment" +
      "\n1997,Ford,Go get one now they are going fast\n2015,Chevy,"
    TestBaseUtils.compareResultsByLinesInMemory(expected, outPath)
  }

  @Test
  def testNumericToTimestamp(): Unit = {
    val timestamp_csv =
      """1
        |""".stripMargin

    val csvSource = CsvTableSource.builder()
      .path(createTmpCsvFile(timestamp_csv))
      .field("c0", DataTypes.TIMESTAMP)
      .build()
    tEnv.registerTableSource("times", csvSource)

    {
      val results = tEnv.sqlQuery("select * from times").collect()

      Assert.assertEquals(results.head.getField(0).getClass, classOf[java.sql.Timestamp])

      Assert.assertEquals(
        results.head.getField(0),
        UTCTimestamp("1970-01-01 00:00:01.0"))
    }
  }

  // read/write java.sql.Date,Time,Timestamp
  // note: our Timestamp is only precise to ms, due to serialization
  @Test
  def testTimeFields(): Unit = {
    val timestamp_csv =
      """2001-02-03,11:12:13,2001-02-03 11:12:13.456
        |""".stripMargin

    val csvSource = CsvTableSource.builder()
      .path(createTmpCsvFile(timestamp_csv))
      .field("c0", DataTypes.DATE)
      .field("c1", DataTypes.TIME)
      .field("c2", DataTypes.TIMESTAMP)
      .build()
    tEnv.registerTableSource("times", csvSource)

    {
      val results = tEnv.sqlQuery("select * from times").collect()

      Assert.assertEquals(results.head.getField(0).getClass, classOf[java.sql.Date])
      Assert.assertEquals(results.head.getField(1).getClass, classOf[java.sql.Time])
      Assert.assertEquals(results.head.getField(2).getClass, classOf[java.sql.Timestamp])

      Assert.assertEquals(results.head.getField(0), UTCDate("2001-02-03"))
      Assert.assertEquals(results.head.getField(1), UTCTime("11:12:13"))
      Assert.assertEquals(
        results.head.getField(2),
        UTCTimestamp("2001-02-03 11:12:13.456"))
    }

    {
      val results = tEnv.sqlQuery("select * from times limit 1")
      val outPath = {
        val tmpFile = File.createTempFile("testCsvTableSink", ".csv")
        tmpFile.deleteOnExit()
        tmpFile.toURI.toString
      }

      val sink = new CsvTableSink(
        outPath, Some(","), None, None, None, Some(WriteMode.OVERWRITE), None, None)
      results.writeToSink(sink)
      tEnv.execute()

      val expected = "2001-02-03,11:12:13,2001-02-03 11:12:13.456"
      TestBaseUtils.compareResultsByLinesInMemory(expected, outPath)
    }
  }

  @Test
  def testEmptyColumnAsNull(): Unit = {

    def assertValues(results: Seq[Row], index: Int): Unit = {
      Assert.assertEquals(results.head.getField(0), "hehe")
      Assert.assertEquals(results.head.getField(1), 5)
      Assert.assertEquals(results.head.getField(2), 5.2)
      Assert.assertEquals(results.head.getField(3), UTCDate("2001-02-03"))
      Assert.assertEquals(results.head.getField(4), UTCTime("11:12:13"))
      Assert.assertEquals(results.head.getField(5), UTCTimestamp("2001-02-03 11:12:13.456"))
      Assert.assertEquals(results.head.getField(6), 5.asInstanceOf[Short])
      Assert.assertEquals(results.head.getField(7), true)
      Assert.assertEquals(results.head.getField(8), 2.2.asInstanceOf[Float])
      Assert.assertEquals(results.head.getField(9), 55L)
      Assert.assertEquals(results.head.getField(10), 5.asInstanceOf[Byte])
      Assert.assertEquals(results.head.getField(11).toString, "5.500000000000000000")
    }

    def assertNull(results: Seq[Row], index: Int): Unit = {
      Assert.assertEquals(results(index).getField(0), null)
      Assert.assertEquals(results(index).getField(1), null)
      Assert.assertEquals(results(index).getField(2), null)
      Assert.assertEquals(results(index).getField(3), null)
      Assert.assertEquals(results(index).getField(4), null)
      Assert.assertEquals(results(index).getField(5), null)
      Assert.assertEquals(results(index).getField(6), null)
      Assert.assertEquals(results(index).getField(7), null)
      Assert.assertEquals(results(index).getField(8), null)
      Assert.assertEquals(results(index).getField(9), null)
      Assert.assertEquals(results(index).getField(10), null)
      Assert.assertEquals(results(index).getField(11), null)
    }

    def assertEmptyString(results: Seq[Row], index: Int): Unit = {
      Assert.assertEquals(results(index).getField(0), "")
      Assert.assertEquals(results(index).getField(1), null)
      Assert.assertEquals(results(index).getField(2), null)
      Assert.assertEquals(results(index).getField(3), null)
      Assert.assertEquals(results(index).getField(4), null)
      Assert.assertEquals(results(index).getField(5), null)
      Assert.assertEquals(results(index).getField(6), null)
      Assert.assertEquals(results(index).getField(7), null)
      Assert.assertEquals(results(index).getField(8), null)
      Assert.assertEquals(results(index).getField(9), null)
      Assert.assertEquals(results(index).getField(10), null)
      Assert.assertEquals(results(index).getField(11), null)
    }

    def buildFields(csv: String, builder: CsvTableSource.Builder): CsvTableSource.Builder = {
      builder.path(createTmpCsvFile(csv))
          .field("f0", DataTypes.STRING)
          .field("f1", DataTypes.INT)
          .field("f2", DataTypes.DOUBLE)
          .field("f3", DataTypes.DATE)
          .field("f4", DataTypes.TIME)
          .field("f5", DataTypes.TIMESTAMP)
          .field("f6", DataTypes.SHORT)
          .field("f7", DataTypes.BOOLEAN)
          .field("f8", DataTypes.FLOAT)
          .field("f9", DataTypes.LONG)
          .field("f10", DataTypes.BYTE)
          .field("f11", DecimalType.SYSTEM_DEFAULT)
      builder
    }

    val csv =
      """hehe,5,5.2,2001-02-03,11:12:13,2001-02-03 11:12:13.456,5,true,2.2,55,5,5.5
        |,,,,,,,,,,,
        |""".stripMargin

    val csvSource1 = buildFields(csv, CsvTableSource.builder())
        .enableEmptyColumnAsNull()
        .build()
    val csvSource2 = buildFields(csv, CsvTableSource.builder())
        .build()
    tEnv.registerTableSource("t1", csvSource1)
    tEnv.registerTableSource("t2", csvSource2)

    var results = tEnv.sqlQuery("select * from t1").collect()
    assertValues(results, 0)
    assertNull(results, 1)

    results = tEnv.sqlQuery("select * from t2").collect()
    assertValues(results, 0)
    assertEmptyString(results, 1)
  }

  @Test
  def testEmptyColumnAsNullWithFirstRow(): Unit = {

    def assertNull(results: Seq[Row], index: Int): Unit = {
      Assert.assertEquals(results(index).getField(0), null)
      Assert.assertEquals(results(index).getField(1), null)
      Assert.assertEquals(results(index).getField(2), null)
      Assert.assertEquals(results(index).getField(3), null)
      Assert.assertEquals(results(index).getField(4), null)
      Assert.assertEquals(results(index).getField(5), null)
      Assert.assertEquals(results(index).getField(6), null)
      Assert.assertEquals(results(index).getField(7), null)
      Assert.assertEquals(results(index).getField(8), null)
      Assert.assertEquals(results(index).getField(9), null)
      Assert.assertEquals(results(index).getField(10), null)
      Assert.assertEquals(results(index).getField(11), null)
    }

    def assertEmptyString(results: Seq[Row], index: Int): Unit = {
      Assert.assertEquals(results(index).getField(0), "")
      Assert.assertEquals(results(index).getField(1), null)
      Assert.assertEquals(results(index).getField(2), null)
      Assert.assertEquals(results(index).getField(3), null)
      Assert.assertEquals(results(index).getField(4), null)
      Assert.assertEquals(results(index).getField(5), null)
      Assert.assertEquals(results(index).getField(6), null)
      Assert.assertEquals(results(index).getField(7), null)
      Assert.assertEquals(results(index).getField(8), null)
      Assert.assertEquals(results(index).getField(9), null)
      Assert.assertEquals(results(index).getField(10), null)
      Assert.assertEquals(results(index).getField(11), null)
    }

    def buildFields(csv: String, builder: CsvTableSource.Builder): CsvTableSource.Builder = {
      builder.path(createTmpCsvFile(csv))
          .field("f0", DataTypes.STRING)
          .field("f1", DataTypes.INT)
          .field("f2", DataTypes.DOUBLE)
          .field("f3", DataTypes.DATE)
          .field("f4", DataTypes.TIME)
          .field("f5", DataTypes.TIMESTAMP)
          .field("f6", DataTypes.SHORT)
          .field("f7", DataTypes.BOOLEAN)
          .field("f8", DataTypes.FLOAT)
          .field("f9", DataTypes.LONG)
          .field("f10", DataTypes.BYTE)
          .field("f11", DecimalType.SYSTEM_DEFAULT)
      builder
    }

    val csv =
      """,,,,,,,,,,,
        |""".stripMargin

    val csvSource1 = buildFields(csv, CsvTableSource.builder())
        .enableEmptyColumnAsNull()
        .build()
    val csvSource2 = buildFields(csv, CsvTableSource.builder())
        .build()
    tEnv.registerTableSource("t1", csvSource1)
    tEnv.registerTableSource("t2", csvSource2)

    var results = tEnv.sqlQuery("select * from t1").collect()
    assertNull(results, 0)

    results = tEnv.sqlQuery("select * from t2").collect()
    assertEmptyString(results, 0)
  }


  @Test
  def testLastEmptyColumnAsNull(): Unit = {
    val csv =
      """1,
        |""".stripMargin

    def lastColumnAsNull(fieldType: InternalType): Unit = {
      val tableName = s"table${Math.abs(fieldType.hashCode)}"
      val csvSource = CsvTableSource.builder().path(createTmpCsvFile(csv))
          .field("f0", DataTypes.INT)
          .field("f1", fieldType).enableEmptyColumnAsNull()
          .build()
      tEnv.registerTableSource(tableName, csvSource)
      val results = tEnv.sqlQuery("select * from " + tableName).collect()
      Assert.assertEquals(results.head.getField(1), null)
    }

    lastColumnAsNull(DataTypes.STRING)
    lastColumnAsNull(DataTypes.INT)
    lastColumnAsNull(DataTypes.DOUBLE)
    lastColumnAsNull(DataTypes.DATE)
    lastColumnAsNull(DataTypes.TIME)
    lastColumnAsNull(DataTypes.TIMESTAMP)
    lastColumnAsNull(DataTypes.SHORT)
    lastColumnAsNull(DataTypes.BOOLEAN)
    lastColumnAsNull(DataTypes.FLOAT)
    lastColumnAsNull(DataTypes.LONG)
    lastColumnAsNull(DataTypes.BYTE)
    lastColumnAsNull(DecimalType.SYSTEM_DEFAULT)
  }

  @Test
  def testLastTwoEmptyColumnAsNull2(): Unit = {
    val csv =
      """,
        |""".stripMargin

    def lastTwoColumnAsNull(fieldType: InternalType): Unit = {
      val tableName = s"table${Math.abs(fieldType.hashCode)}"
      val csvSource = CsvTableSource.builder().path(createTmpCsvFile(csv))
          .field("f0", DataTypes.INT)
          .field("f1", fieldType).enableEmptyColumnAsNull()
          .build()
      tEnv.registerTableSource(tableName, csvSource)
      val results = tEnv.sqlQuery("select * from " + tableName).collect()
      Assert.assertEquals(results.head.getField(0), null)
      Assert.assertEquals(results.head.getField(1), null)
    }

    lastTwoColumnAsNull(DataTypes.STRING)
    lastTwoColumnAsNull(DataTypes.INT)
    lastTwoColumnAsNull(DataTypes.DOUBLE)
    lastTwoColumnAsNull(DataTypes.DATE)
    lastTwoColumnAsNull(DataTypes.TIME)
    lastTwoColumnAsNull(DataTypes.TIMESTAMP)
    lastTwoColumnAsNull(DataTypes.SHORT)
    lastTwoColumnAsNull(DataTypes.BOOLEAN)
    lastTwoColumnAsNull(DataTypes.FLOAT)
    lastTwoColumnAsNull(DataTypes.LONG)
    lastTwoColumnAsNull(DataTypes.BYTE)
    lastTwoColumnAsNull(DecimalType.SYSTEM_DEFAULT)
  }
}
