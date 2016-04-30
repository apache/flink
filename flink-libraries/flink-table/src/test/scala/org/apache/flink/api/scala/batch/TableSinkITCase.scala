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

package org.apache.flink.api.scala.batch

import java.io.File

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.api.table.sinks.{CsvTableSink, TableSink, BatchTableSink}
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized


@RunWith(classOf[Parameterized])
class TableSinkITCase(
    mode: TestExecutionMode)
  extends MultipleProgramsTestBase(mode) {

  @Test
  def testBatchTableSink(): Unit = {

    val tmpFile = File.createTempFile("flink-table-sink-test", ".tmp")
    tmpFile.deleteOnExit()
    val path = "file:///" + tmpFile.getAbsolutePath

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(4)

    val input = CollectionDataSets.get3TupleDataSet(env)
      .map(x => x).setParallelism(4) // increase DOP to 4

    val results = input.toTable(tEnv, 'a, 'b, 'c)
      .where('a < 5 || 'a > 17)
      .select('c, 'b)
      .toSink(new CsvTableSink(path, fieldDelim = "|"))

    env.execute()

    val expected = Seq(
      "Hi|1", "Hello|2", "Hello world|2", "Hello world, how are you?|3",
      "Comment#12|6", "Comment#13|6", "Comment#14|6", "Comment#15|6").mkString("\n")

    TestBaseUtils.compareResultsByLinesInMemory(expected, path)
  }

}
