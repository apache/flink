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
import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions.toInt
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions.toLong
import org.apache.flink.table.sinks.parquet.RowParquetOutputFormat
import org.apache.flink.table.util.DateTimeTestUtil._

object CommonParquetTestSqlTimeTypeData {

  def getWithSQLTimestampParquetVectorizedColumnRowTableSource:
    ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      GenericRow.of(1: JInt, toLong(UTCTimestamp("2017-07-11 7:23:19")): JLong),
      GenericRow.of(2: JInt, toLong(UTCTimestamp("2017-07-12 11:45:00")): JLong),
      GenericRow.of(3: JInt, toLong(UTCTimestamp("2017-07-13 11:45:01")): JLong),
      GenericRow.of(4: JInt, toLong(UTCTimestamp("2017-07-14 12:14:23")): JLong),
      GenericRow.of(5: JInt, toLong(UTCTimestamp("2017-07-13 13:33:12")): JLong)
    )

    val names = Array("id", "ts")
    val types: Array[InternalType] = Array(
      DataTypes.INT,
      DataTypes.TIMESTAMP
    )
    val tempFilePath = writeToTempFile(records, types, names, "parquet-test", "tmp")
    new ParquetVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true
    )
  }

  def getWithSQLTimeParquetVectorizedColumnRowTableSource:
      ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      GenericRow.of(1: JInt, toInt(UTCTime("7:23:19")): JInt),
      GenericRow.of(2: JInt, toInt(UTCTime("11:45:00")): JInt),
      GenericRow.of(3: JInt, toInt(UTCTime("11:45:01")): JInt),
      GenericRow.of(4: JInt, toInt(UTCTime("12:14:23")): JInt),
      GenericRow.of(5: JInt, toInt(UTCTime("13:33:12")): JInt)
    )

    val names = Array("id", "time_val")
    val types: Array[InternalType] = Array(
      DataTypes.INT,
      DataTypes.TIME
    )
    val tempFilePath = writeToTempFile(records, types, names, "parquet-test", "tmp")
    new ParquetVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true
    )
  }

  def getWithSQLDateParquetVectorizedColumnRowTableSource:
      ParquetVectorizedColumnRowTableSource = {
    val records = Seq(
      GenericRow.of(23: JInt, toInt(UTCDate("2017-04-23")): JInt),
      GenericRow.of(24: JInt, toInt(UTCDate("2017-04-24")): JInt),
      GenericRow.of(25: JInt, toInt(UTCDate("2017-04-25")): JInt),
      GenericRow.of(26: JInt, toInt(UTCDate("2017-04-26")): JInt)
    )

    val names = Array("id", "date_val")
    val types: Array[InternalType] = Array(
      DataTypes.INT,
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
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.delete()
    val outFormat = new RowParquetOutputFormat(tempFile.getAbsolutePath, fieldTypes, fieldNames)
    outFormat.open(1, 1)
    contents.foreach(outFormat.writeRecord(_))
    outFormat.close()
    tempFile.deleteOnExit()
    tempFile.getAbsolutePath
  }
}
