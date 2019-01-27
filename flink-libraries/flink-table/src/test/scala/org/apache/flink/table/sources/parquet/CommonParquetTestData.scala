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

package org.apache.flink.table.sources.parquet

import java.io.File
import java.lang.{Double => JDouble, Integer => JInt}
import java.sql.Date
import java.math.BigDecimal

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.dataformat.{Decimal, GenericRow}
import org.apache.flink.table.dataformat.GenericRow.of
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions.toInt
import org.apache.flink.table.sinks.parquet.RowParquetOutputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object CommonParquetTestData {

  def getParquetVectorizedColumnRowTableSource: ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      of("Mike", 1: JInt, 12.3d: JDouble, "Smith"),
      of("Bob", 2: JInt, 45.6d: JDouble, "Taylor"),
      of("Sam", 3: JInt, 7.89d: JDouble, "Miller"),
      of("Peter", 4: JInt, 0.12d: JDouble, "Smith"),
      of("Liz", 5: JInt, 34.5d: JDouble, "Williams"),
      of("Sally", 6: JInt, 6.78d: JDouble, "Miller"),
      of("Alice", 7: JInt, 90.1d: JDouble, "Smith"),
      of("Kelly", 8: JInt, 2.34d: JDouble, "Williams")
    )

    val names = Array("first", "id", "score", "last")
    val types: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.INT,
      DataTypes.DOUBLE,
      DataTypes.STRING
    )
    val tempFilePath = writeToTempFile(records, types, names, "parquet-test", "tmp")
    new ParquetVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true
    )
  }

  def getWithDecimalVectorizedTableSource(enableDictionary: Boolean):
  ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      of(
        Decimal.castFrom(0, 7, 2),
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
      of(
        Decimal.castFrom(1.23, 7, 2),
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
      of(Decimal.castFrom(-1.23, 7, 2),
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
        Decimal.castFrom("-123456789012345678901.234", 38, 3)),
      of(Decimal.castFrom(0, 7, 2),
        Decimal.castFrom(-(1e9 - 1), 9, 0),
        Decimal.castFrom(-12345.67, 9, 4),
        Decimal.castFrom(0, 9, 9),
        Decimal.castFrom(Int.MaxValue.toLong + 1, 10, 0),
        Decimal.castFrom(0.9, 10, 1),
        Decimal.castFrom(-1e-10, 10, 10),
        Decimal.fromBigDecimal(BigDecimal.valueOf(-1e18).add(BigDecimal.ONE), 18, 0),
        Decimal.castFrom(-9876543210.98765, 18, 5),
        Decimal.castFrom(-1e-18, 18, 18),
        Decimal.fromBigDecimal(BigDecimal.valueOf(Long.MinValue).subtract(BigDecimal.ONE), 19, 0),
        Decimal.castFrom("-" + "9" * 38, 38, 0),
        Decimal.castFrom("123456789012345678901.234", 38, 3))
    )

    val names = Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m")
    val types: Array[InternalType] = Array(
      DataTypes.createDecimalType(7, 2),
      DataTypes.createDecimalType(9, 0),
      DataTypes.createDecimalType(9, 4),
      DataTypes.createDecimalType(9, 9),
      DataTypes.createDecimalType(10, 0),
      DataTypes.createDecimalType(10, 1),
      DataTypes.createDecimalType(10, 10),
      DataTypes.createDecimalType(18, 0),
      DataTypes.createDecimalType(18, 5),
      DataTypes.createDecimalType(18, 18),
      DataTypes.createDecimalType(19, 0),
      DataTypes.createDecimalType(38, 0),
      DataTypes.createDecimalType(38, 3)
    )
    val tempFilePath = writeToTempFile(records, types, names,
      "parquet-test", "tmp", enableDictionary)
    new ParquetVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true
    )
  }

  def getWithSQLDateParquetVectorizedColumnRowTableSource: ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      of("Mike", 1: JInt, 12.3d: JDouble, "Smith", toInt(new Date(1507884781368L)): JInt),
      of("Bob", 2: JInt, 45.6d: JDouble, "Taylor", toInt(new Date(1507884781368L)): JInt),
      of("Sam", 3: JInt, 7.89d: JDouble, "Miller", toInt(new Date(1507884781368L)): JInt),
      of("Peter", 4: JInt, 0.12d: JDouble, "Smith", toInt(new Date(1507884781368L)): JInt),
      of("Liz", 5: JInt, 34.5d: JDouble, "Williams", toInt(new Date(1507884781368L)): JInt),
      of("Sally", 6: JInt, 6.78d: JDouble, "Miller", toInt(new Date(1507884781368L)): JInt),
      of("Alice", 7: JInt, 90.1d: JDouble, "Smith", toInt(new Date(1507884781368L)): JInt),
      of("Kelly", 8: JInt, 2.34d: JDouble, "Williams", toInt(new Date(1507884781368L)): JInt)
    )

    val names = Array("first", "id", "score", "last", "birthday")
    val types: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.INT,
      DataTypes.DOUBLE,
      DataTypes.STRING,
      DataTypes.DATE
    )

    val tempFilePath = writeToTempFile(records, types, names, "parquet-test", "tmp")
    new ParquetVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true
    )
  }

  def getWithParallelismParquetVectorizedColumnRowTableSource:
   ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      of("Mike", 1: JInt, 12.3d: JDouble, "Smith", toInt(new Date(1507884781368L)): JInt),
      of("Bob", 2: JInt, 45.6d: JDouble, "Taylor", toInt(new Date(1507884781368L)): JInt),
      of("Sam", 3: JInt, 7.89d: JDouble, "Miller", toInt(new Date(1507884781368L)): JInt),
      of("Peter", 4: JInt, 0.12d: JDouble, "Smith", toInt(new Date(1507884781368L)): JInt),
      of("Liz", 5: JInt, 34.5d: JDouble, "Williams", toInt(new Date(1507884781368L)): JInt),
      of("Sally", 6: JInt, 6.78d: JDouble, "Miller", toInt(new Date(1507884781368L)): JInt),
      of("Alice", 7: JInt, 90.1d: JDouble, "Smith", toInt(new Date(1507884781368L)): JInt),
      of("Kelly", 8: JInt, 2.34d: JDouble, "Williams", toInt(new Date(1507884781368L)): JInt)
    )

    val names = Array("first", "id", "score", "last", "birthday")
    val types: Array[InternalType] = Array(
      DataTypes.STRING,
      DataTypes.INT,
      DataTypes.DOUBLE,
      DataTypes.STRING,
      DataTypes.DATE
    )

    val tempFilePath = writeToTempFile(records, types, names, "parquet-test", "tmp")
    new ParquetVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true
    )
  }

  private def writeToTempFile(
                               contents: Seq[GenericRow],
                               fieldTypes: Array[InternalType],
                               fieldNames: Array[String],
                               filePrefix: String,
                               fileSuffix: String): String = {
    writeToTempFile(contents, fieldTypes, fieldNames, filePrefix, fileSuffix, false)
  }

  private def writeToTempFile(
      contents: Seq[GenericRow],
      fieldTypes: Array[InternalType],
      fieldNames: Array[String],
      filePrefix: String,
      fileSuffix: String,
      enableDictionary: Boolean
  ): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.delete()
    val outFormat = new RowParquetOutputFormat(tempFile.getAbsolutePath, fieldTypes, fieldNames,
      CompressionCodecName.UNCOMPRESSED, 128 * 1024 * 1024, true)
    outFormat.open(1, 1)
    contents.foreach(outFormat.writeRecord(_))
    outFormat.close()
    tempFile.deleteOnExit()
    tempFile.getAbsolutePath
  }
}
