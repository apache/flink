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

package org.apache.flink.api.table.sink

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.Collections

import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.sinks.CsvTableSink
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}
import org.junit.Assert._

class CsvTableSinkTest {

  val _folder = new TemporaryFolder

  @Rule
  def folder = _folder

  @Test
  def testSaveDataSetToCsvFileWithDefaultDelimiter(): Unit = {
    val file = new File(folder.getRoot, "test.csv")
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds = createDataset(env)

    val sink = new CsvTableSink(file.getAbsolutePath)
    writeToCsv(env, ds, sink)

    val lines = Files.readAllLines(file.toPath, Charset.defaultCharset())
    assertEquals(Collections.singletonList("1,str,false"), lines)
  }

  @Test
  def testSaveDataSetToCsvFileWithCustomDelimiter(): Unit = {
    val file = new File(folder.getRoot, "test.csv")
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds = createDataset(env)

    val sink = new CsvTableSink(file.getAbsolutePath, "|")
    writeToCsv(env, ds, sink)

    val lines = Files.readAllLines(file.toPath, Charset.defaultCharset())
    assertEquals(Collections.singletonList("1|str|false"), lines)
  }

  def writeToCsv(env: ExecutionEnvironment, ds: DataSource[Row], sink: CsvTableSink): Unit = {
    sink.emitDataSet(ds)
    env.execute("job")
  }

  def createDataset(env: ExecutionEnvironment): DataSource[Row] = {
    val row = new Row(3)
    row.setField(0, 1)
    row.setField(1, "str")
    row.setField(2, false)
    env.fromCollection(Collections.singletonList(row))
  }
}
