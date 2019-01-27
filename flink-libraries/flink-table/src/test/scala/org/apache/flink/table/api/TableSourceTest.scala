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

package org.apache.flink.table.api

import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.sources.csv.CsvTableSource
import org.junit.{Assert, Test}

class TableSourceTest {

  // csv builder

  @Test
  def testCsvTableSourceBuilder(): Unit = {
    val source1 = CsvTableSource.builder()
        .path("/path/to/csv")
        .field("myfield", DataTypes.STRING, isNullable = false)
        .field("myfield2", DataTypes.INT)
        .quoteCharacter(';')
        .fieldDelimiter("#")
        .lineDelimiter("\r\n")
        .commentPrefix("%%")
        .ignoreFirstLine()
        .ignoreParseErrors()
        .charset("utf-8")
        .build()

    val source2 = new CsvTableSource(
      "/path/to/csv",
      Array("myfield", "myfield2"),
      Array(DataTypes.STRING, DataTypes.INT),
      Array(false, true),
      "#",
      "\r\n",
      ';',
      true,
      "%%",
      true,
      "utf-8",
      false)

    Assert.assertEquals(source1, source2)
  }
}
