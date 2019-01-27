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

package org.apache.flink.table.sources.orc

import java.io.File
import java.lang.{Double => JDouble, Integer => JInt}

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.types._
import org.apache.flink.table.dataformat.GenericRow
import org.apache.flink.table.dataformat.GenericRow.of
import org.apache.flink.table.sinks.orc.RowOrcOutputFormat
import org.apache.orc.CompressionKind

object CommonOrcTestData {
  def getOrcVectorizedColumnRowTableSource(copyToFlink: Boolean):
      OrcVectorizedColumnRowTableSource = {
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
      StringType.INSTANCE,
      IntType.INSTANCE,
      DoubleType.INSTANCE,
      StringType.INSTANCE
    )
    val tempFilePath = writeToTempFile(records, types, names, "orc-test", "tmp")
    new OrcVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true,
      copyToFlink
    )
  }

  def getBigOrcVectorizedColumnRowTableSource(copyToFlink: Boolean):
      OrcVectorizedColumnRowTableSource = {
    val random = scala.util.Random
    val records = for {
      i <- 1 until 2000
    } yield of("Mike" + i, i: JInt, random.nextDouble(): JDouble, "Smith" + i)

    val names = Array("first", "id", "score", "last")
    val types: Array[InternalType] = Array(
      StringType.INSTANCE,
      IntType.INSTANCE,
      DoubleType.INSTANCE,
      StringType.INSTANCE
    )
    val tempFilePath = writeToTempFile(records, types, names, "orc-test", "tmp")
    new OrcVectorizedColumnRowTableSource(
      new Path(tempFilePath),
      types,
      names,
      true,
      copyToFlink
    )
  }

  def getOrcVectorizedColumnRowTableSourceFromPeopleFile(copyToFlink: Boolean):
      OrcVectorizedColumnRowTableSource = {
    val names = Array("age", "name")
    val types: Array[InternalType] = Array(
      IntType.INSTANCE,
      StringType.INSTANCE
    )

    val filePath = getClass.getClassLoader.getResource("test-data.orc/people.orc").getPath
    val tableSource = new OrcVectorizedColumnRowTableSource(
      new Path(filePath),
      types,
      names,
      true,
      copyToFlink
    )
    tableSource
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
      enableDictionary: Boolean): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    tempFile.delete()
    val outFormat = new RowOrcOutputFormat(
      fieldTypes, fieldNames, tempFile.getAbsolutePath, CompressionKind.NONE, "orc-", 1000)
    outFormat.open(1, 1)
    contents.foreach(outFormat.writeRecord(_))
    outFormat.close()
    tempFile.deleteOnExit()
    tempFile.getAbsolutePath
  }
}
