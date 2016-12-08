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

package org.apache.flink.table.utils

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.sources.CsvTableSource

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

    val tempFile = File.createTempFile("csv-test", "tmp")
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8")
    tmpWriter.write(csvRecords.mkString("$"))
    tmpWriter.close()

    new CsvTableSource(
      tempFile.getAbsolutePath,
      Array("first", "id", "score", "last"),
      Array(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "#",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }
}
