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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.types.{DataTypes, DecimalType, InternalType, TypeConverters}
import org.apache.flink.table.api.{RichTableSchema, TableSchema}
import org.apache.flink.table.catalog._
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.types.Row

import org.apache.calcite.avatica.util.DateTimeUtils

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.math.BigDecimal
import java.sql.Timestamp
import java.{lang, util}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.Random

object CommonTestData {

  def getCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    val tempFilePath = writeToTempFile(csvRecords.mkString("$"), "csv-test", "tmp")
    CsvTableSource.builder()
      .path(tempFilePath)
      .fields(Array("first", "id", "score", "last"), Array(
        DataTypes.STRING,
        DataTypes.INT,
        DataTypes.DOUBLE,
        DataTypes.STRING
      ))
      .fieldDelimiter("#")
      .lineDelimiter("$")
      .ignoreFirstLine()
      .commentPrefix("%")
      .build()
  }

  def get3NullDataRow(): (mutable.MutableList[Row], RowTypeInfo) = {
    val (data3, type3) = get3DataRow()
    val row1 = new Row(3)
    row1.setField(0, null)
    row1.setField(1, 999L)
    row1.setField(2, "NullTuple")
    data3.+=(row1)
    data3.+=(row1)
    (data3, type3)
  }

  def get3DataRow(): (mutable.MutableList[Row], RowTypeInfo) = {
    (get3Data().map {
      case (f0, f1, f2) =>
        val row = new Row(3)
        row.setField(0, f0)
        row.setField(1, f1)
        row.setField(2, f2)
        row
    }, TypeConverters.createExternalTypeInfoFromDataType(DataTypes.createRowType(
      DataTypes.INT, DataTypes.LONG, DataTypes.STRING))
        .asInstanceOf[RowTypeInfo])
  }

  def get3Data(): mutable.MutableList[(Integer, lang.Long, String)] = {
    var data = new mutable.MutableList[(Integer, lang.Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "Hello world, how are you?"))
    data.+=((5, 3L, "I am fine."))
    data.+=((6, 3L, "Luke Skywalker"))
    data.+=((7, 4L, "Comment#1"))
    data.+=((8, 4L, "Comment#2"))
    data.+=((9, 4L, "Comment#3"))
    data.+=((10, 4L, "Comment#4"))
    data.+=((11, 5L, "Comment#5"))
    data.+=((12, 5L, "Comment#6"))
    data.+=((13, 5L, "Comment#7"))
    data.+=((14, 5L, "Comment#8"))
    data.+=((15, 5L, "Comment#9"))
    data.+=((16, 6L, "Comment#10"))
    data.+=((17, 6L, "Comment#11"))
    data.+=((18, 6L, "Comment#12"))
    data.+=((19, 6L, "Comment#13"))
    data.+=((20, 6L, "Comment#14"))
    data.+=((21, 6L, "Comment#15"))
    data = Random.shuffle(data)
    data
  }

  def get3Source(names: Array[String] = Array("_1", "_2", "_3")): CsvTableSource = {

    val data = get3Data()

    val csvRecords = data.map { case (f1, f2, f3) => s"$f1\\|$f2\\|$f3" }

    val tempFilePath = writeToTempFile(csvRecords.mkString("$"), "get3TupleCsvTableSource", "tmp")

    new TestCsvTableSourceWithStableStats(
      tempFilePath,
      names,
      Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING),
      Array(true, true, true),
      "\\|",
      "$",
      tableStats = Some(TableStats(data.size.toLong)))
  }

  def getSmall3Data: mutable.MutableList[(Integer, lang.Long, String)] = {
    var data = new mutable.MutableList[(Integer, lang.Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    data = Random.shuffle(data)
    data
  }

  def getSmall3Source(
      names: Array[String] = Array("_1", "_2", "_3"),
      uniqueKeys: Set[Set[String]] = null): CsvTableSource = {

    val data = getSmall3Data

    val csvRecords = data.map { case (f1, f2, f3) => s"$f1\\|$f2\\|$f3" }

    val tempFilePath = writeToTempFile(
      csvRecords.mkString("$"), "getSmall3TupleCsvTableSource", "tmp")

    new TestCsvTableSourceWithStableStats(
      tempFilePath,
      names,
      Array(DataTypes.INT, DataTypes.LONG, DataTypes.STRING),
      Array(true, true, true),
      "\\|",
      "$",
      tableStats = Some(TableStats(data.size.toLong)),
      uniqueKeySet = if(uniqueKeys != null) uniqueKeys.map(_.asJava).asJava else null)
  }

  def getSmall5Data: mutable.MutableList[(Integer, lang.Long, Integer, String, lang.Long)] = {
    var data = new mutable.MutableList[(Integer, lang.Long, Integer, String, lang.Long)]
    data.+=((1, 1L, 0, "Hallo", 1L))
    data.+=((2, 2L, 1, "Hallo Welt", 2L))
    data.+=((2, 3L, 2, "Hallo Welt wie", 1L))
    data = Random.shuffle(data)
    data
  }

  def getSmall5Source(
    names: Array[String] = Array("_1", "_2", "_3", "_4", "_5"),
    uniqueKeys: Set[Set[String]] = null): CsvTableSource = {

    val data = getSmall5Data

    val csvRecords = data.map { case (f1, f2, f3, f4, f5) => s"$f1\\|$f2\\|$f3\\|$f4\\|$f5" }

    val tempFilePath = writeToTempFile(
      csvRecords.mkString("$"), "getSmall5TupleCsvTableSource", "tmp")

    new TestCsvTableSourceWithStableStats(
      tempFilePath,
      names,
      Array(DataTypes.INT, DataTypes.LONG, DataTypes.INT, DataTypes.STRING, DataTypes.LONG),
      Array(true, true, true, true, true),
      "\\|",
      "$",
      tableStats = Some(TableStats(data.size.toLong)),
      uniqueKeySet = if(uniqueKeys != null) uniqueKeys.map(_.asJava).asJava else null)
  }

  def get5NullDataRow(): (mutable.MutableList[Row], RowTypeInfo) = {
    val (data5, type5) = get5DataRow()
    val row1 = new Row(5)
    row1.setField(0, null)
    row1.setField(1, 999L)
    row1.setField(2, 999)
    row1.setField(3, "NullTuple")
    row1.setField(4, 999L)
    data5.+=(row1)
    data5.+=(row1)
    (data5, type5)
  }

  def get5DataRow(): (mutable.MutableList[Row], RowTypeInfo) = {
    (get5Data().map {
      case (f0, f1, f2, f3, f4) =>
        val row = new Row(5)
        row.setField(0, f0)
        row.setField(1, f1)
        row.setField(2, f2)
        row.setField(3, f3)
        row.setField(4, f4)
        row
    }, TypeConverters.createExternalTypeInfoFromDataType(DataTypes.createRowType(
      DataTypes.INT, DataTypes.LONG, DataTypes.INT, DataTypes.STRING, DataTypes.LONG))
        .asInstanceOf[RowTypeInfo])
  }

  def get5Data(): mutable.MutableList[(Integer, lang.Long, Integer, String, lang.Long)] = {
    var data = new mutable.MutableList[(Integer, lang.Long, Integer, String, lang.Long)]
    data.+=((1, 1L, 0, "Hallo", 1L))
    data.+=((2, 2L, 1, "Hallo Welt", 2L))
    data.+=((2, 3L, 2, "Hallo Welt wie", 1L))
    data.+=((3, 4L, 3, "Hallo Welt wie gehts?", 2L))
    data.+=((3, 5L, 4, "ABC", 2L))
    data.+=((3, 6L, 5, "BCD", 3L))
    data.+=((4, 7L, 6, "CDE", 2L))
    data.+=((4, 8L, 7, "DEF", 1L))
    data.+=((4, 9L, 8, "EFG", 1L))
    data.+=((4, 10L, 9, "FGH", 2L))
    data.+=((5, 11L, 10, "GHI", 1L))
    data.+=((5, 12L, 11, "HIJ", 3L))
    data.+=((5, 13L, 12, "IJK", 3L))
    data.+=((5, 14L, 13, "JKL", 2L))
    data.+=((5, 15L, 14, "KLM", 2L))
    data = Random.shuffle(data)
    data
  }

  def get5Source(
      names: Array[String] = Array("_1", "_2", "_3", "_4", "_5")): CsvTableSource = {

    val data = get5Data()

    val csvRecords = data.map { case (f1, f2, f3, f4, f5) => s"$f1\\|$f2\\|$f3\\|$f4\\|$f5" }

    val tempFilePath = writeToTempFile(csvRecords.mkString("$"), "get5TupleCsvTableSource", "tmp")

    new TestCsvTableSourceWithStableStats(
      tempFilePath,
      names,
      Array(DataTypes.INT, DataTypes.LONG, DataTypes.INT, DataTypes.STRING, DataTypes.LONG),
      Array(true, true, true, true, true),
      "\\|",
      "$",
      tableStats = Some(TableStats(data.size.toLong)))
  }

  def getTestFlinkInMemoryCatalog: ReadableCatalog = {
    getTestFlinkInMemoryCatalog(true)
  }

  def getTestFlinkInMemoryCatalog(isStreaming: Boolean): ReadableCatalog = {
    val csvRecord1 = Seq(
      "1#1#Hi",
      "2#2#Hello",
      "3#2#Hello world"
    )
    val tempFilePath1 = writeToTempFile(csvRecord1.mkString("$"), "csv-test1", "tmp")
    val properties1 = new util.HashMap[String, String]()
    properties1.put("path", tempFilePath1)
    properties1.put("fieldDelim", "#")
    properties1.put("rowDelim", "$")
    val catalogTable1 = new CatalogTable(
      "CSV",
      new TableSchema(
        Array("a", "b", "c"),
        Array(
          DataTypes.INT,
          DataTypes.LONG,
          DataTypes.STRING)),
      properties1,
      new RichTableSchema(
        Array("a", "b", "c"),
        Array(
          DataTypes.INT,
          DataTypes.LONG,
          DataTypes.STRING)),
      null,
      "",
      null,
      false,
      null,
      null,
      -1,
      System.currentTimeMillis(),
      System.currentTimeMillis(),
      isStreaming
    )

    val csvRecord2 = Seq(
      "1#1#0#Hallo#1",
      "2#2#1#Hallo Welt#2",
      "2#3#2#Hallo Welt wie#1",
      "3#4#3#Hallo Welt wie gehts?#2",
      "3#5#4#ABC#2",
      "3#6#5#BCD#3",
      "4#7#6#CDE#2",
      "4#8#7#DEF#1",
      "4#9#8#EFG#1",
      "4#10#9#FGH#2",
      "5#11#10#GHI#1",
      "5#12#11#HIJ#3",
      "5#13#12#IJK#3",
      "5#14#13#JKL#2",
      "5#15#14#KLM#2"
    )
    val tempFilePath2 = writeToTempFile(csvRecord2.mkString("$"), "csv-test2", "tmp")
    val properties2 = new util.HashMap[String, String]()
    properties2.put("path", tempFilePath2)
    properties2.put("fieldDelim", "#")
    properties2.put("rowDelim", "$")
    val catalogTable2 = new CatalogTable(
      "CSV",
      new TableSchema(
        Array("d", "e", "f", "g", "h"),
        Array(
          DataTypes.INT,
          DataTypes.LONG,
          DataTypes.INT,
          DataTypes.STRING,
          DataTypes.LONG)
      ),
      properties2,
      new RichTableSchema(
        Array("d", "e", "f", "g", "h"),
        Array(
          DataTypes.INT,
          DataTypes.LONG,
          DataTypes.INT,
          DataTypes.STRING,
          DataTypes.LONG)
      ),
      null,
      "",
      null,
      false,
      null,
      null,
      -1,
      System.currentTimeMillis(),
      System.currentTimeMillis(),
      isStreaming
    )
    val catalog = new FlinkInMemoryCatalog("test")
    val db1 = new CatalogDatabase()
    val db2 = new CatalogDatabase()
    catalog.createDatabase("db1", db1, false)
    catalog.createDatabase("db2", db2, false)

    // Register the table with both catalogs
    catalog.createTable(new ObjectPath("db1", "tb1"), catalogTable1, false)
    catalog.createTable(new ObjectPath("db2", "tb2"), catalogTable2, false)
    catalog
  }

  def getWithDecimalCsvTableSource: CsvTableSource = {
    val records = Seq(
      Seq(Decimal.castFrom(0, 7, 2),
        Decimal.castFrom(0, 9, 0),
        Decimal.castFrom(0, 9, 4),
        Decimal.castFrom(0, 9, 9),
        Decimal.castFrom(0, 10, 0),
        Decimal.castFrom(0, 10, 1),
        Decimal.castFrom(0, 10, 10),
        Decimal.castFrom(0, 18, 0),
        Decimal.castFrom(0, 18, 5),
        Decimal.castFrom(0, 18, 18),
        Decimal.castFrom(0, 19, 0),
        Decimal.castFrom(0, 38, 0),
        Decimal.castFrom(0, 38, 3)),
      Seq(Decimal.castFrom(1.23, 7, 2),
        Decimal.castFrom(1e9 - 1, 9, 0),
        Decimal.castFrom(12345.67, 9, 4),
        Decimal.castFrom(1e-9, 9, 9),
        Decimal.castFrom(Int.MaxValue.toLong + 1, 10, 0),
        Decimal.castFrom(12345678.9, 10, 1),
        Decimal.castFrom(1e-10, 10, 10),
        Decimal.fromBigDecimal(BigDecimal.valueOf(1e18).subtract(BigDecimal.ONE), 18, 0),
        Decimal.castFrom(9876543210.98765, 18, 5),
        Decimal.castFrom(1e-18, 18, 18),
        Decimal.fromBigDecimal(BigDecimal.valueOf(Long.MaxValue).add(BigDecimal.ONE), 19, 0),
        Decimal.castFrom("9" * 38, 38, 0),
        Decimal.castFrom("123456789012345678901.234", 38, 3)),
      Seq(Decimal.castFrom(-1.23, 7, 2),
        Decimal.castFrom(-(1e9 - 1), 9, 0),
        Decimal.castFrom(-12345.67, 9, 4),
        Decimal.castFrom(-1e-9, 9, 9),
        Decimal.castFrom(Int.MinValue.toLong - 1, 10, 0),
        Decimal.castFrom(-12345678.9, 10, 1),
        Decimal.castFrom(-1e-10, 10, 10),
        Decimal.fromBigDecimal(BigDecimal.valueOf(-1e18).add(BigDecimal.ONE), 18, 0),
        Decimal.castFrom(-9876543210.98765, 18, 5),
        Decimal.castFrom(-1e-18, 18, 18),
        Decimal.fromBigDecimal(BigDecimal.valueOf(Long.MinValue).subtract(BigDecimal.ONE), 19, 0),
        Decimal.castFrom("-" + "9" * 38, 38, 0),
        Decimal.castFrom("-123456789012345678901.234", 38, 3))
    )

    val tempFile = writeToTempFile(records.map(_.mkString("#")).mkString("$"), "csv-test", "tmp")

    CsvTableSource.builder()
      .path(tempFile)
      .field("a", DecimalType.of(7, 2))
      .field("b", DecimalType.of(9, 0))
      .field("c", DecimalType.of(9, 4))
      .field("d", DecimalType.of(9, 9))
      .field("e", DecimalType.of(10, 0))
      .field("f", DecimalType.of(10, 1))
      .field("g", DecimalType.of(10, 10))
      .field("h", DecimalType.of(18, 0))
      .field("i", DecimalType.of(18, 5))
      .field("j", DecimalType.of(18, 18))
      .field("k", DecimalType.of(19, 0))
      .field("l", DecimalType.of(38, 0))
      .field("m", DecimalType.of(38, 3))
      .fieldDelimiter("#")
      .lineDelimiter("$")
      .build()
  }

  def createCsvTableSource(
      data: Seq[Row],
      fieldNames: Array[String],
      fieldTypes: Array[InternalType],
      fieldDelim: String = CsvInputFormat.DEFAULT_FIELD_DELIMITER,
      rowDelim: String = CsvInputFormat.DEFAULT_LINE_DELIMITER,
      enableEmptyColumnAsNull: Boolean = true,
      uniqueKeys: Set[Set[String]] = null): CsvTableSource = {
    val contents = data.map { r =>
      (0 until r.getArity).map(i => r.getField(i)).map {
        case null => ""
        case t: Timestamp => DateTimeUtils.unixTimestampToString(t.getTime)
        case f => f.toString
      }.mkString(fieldDelim)
    }.mkString(rowDelim)
    val tempFilePath = writeToTempFile(contents, "testCsvTableSource", "tmp")
    val builder = CsvTableSource.builder()
      .path(tempFilePath)
      .fields(fieldNames, fieldTypes)
      .fieldDelimiter(fieldDelim)
      .lineDelimiter(rowDelim)
    if (enableEmptyColumnAsNull) {
      builder.enableEmptyColumnAsNull()
    }
    if (uniqueKeys != null) {
      builder.uniqueKeys(uniqueKeys.map(_.asJava).asJava)
    }
    builder.build()
  }

  def writeToTempFile(
      contents: String,
      filePrefix: String,
      fileSuffix: String,
      charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tmpWriter.write(contents)
    tmpWriter.close()
    tempFile.getAbsolutePath
  }

  class Person(var firstName: String, var lastName: String, var address: Address) {
    def this() {
      this(null, null, null)
    }
  }

  class Address(var street: String, var city: String) {
    def this() {
      this(null, null)
    }
  }

  /**
    * A csvTableSource for test, which specifies tableStats.
    */
  class TestCsvTableSourceWithStableStats(
      private val path: String,
      private val fieldNames: Array[String],
      private val fieldTypes: Array[InternalType],
      private val fieldNullables: Array[Boolean],
      private val fieldDelim: String,
      private val rowDelim: String,
      private val quoteCharacter: Character = null,
      private val ignoreFirstLine: Boolean = false,
      private val ignoreComments: String = null,
      private val lenient: Boolean = false,
      private val charset: String = null,
      private val emptyColumnAsNull: Boolean = false,
      private val tableStats: Option[TableStats] = None,
      private var isLimitPushdown: Boolean = false,
      private var limit: Long = Long.MaxValue,
      private var uniqueKeySet: java.util.Set[java.util.Set[String]] = null
  ) extends CsvTableSource (path, fieldNames, fieldTypes, fieldNullables, fieldDelim, rowDelim,
    quoteCharacter, ignoreFirstLine, ignoreComments, lenient, charset, emptyColumnAsNull,
    isLimitPushdown, limit, uniqueKeySet) {

    override def getTableStats: TableStats = {
      tableStats match {
        case Some(ts) => ts
        case _ => super.getTableStats
      }
    }

  }

  class NonPojo {
    val x = new java.util.HashMap[String, String]()

    override def toString: String = x.toString

    override def hashCode(): Int = super.hashCode()

    override def equals(obj: scala.Any): Boolean = super.equals(obj)
  }
}
