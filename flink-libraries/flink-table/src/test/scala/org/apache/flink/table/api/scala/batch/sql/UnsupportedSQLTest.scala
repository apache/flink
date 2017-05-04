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

package org.apache.flink.table.api.scala.batch.sql
import java.io.File

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.utils.CsvSQLTableSink
import org.junit._

class UnsupportedSQLTest {

  /** test unsupported partial insert **/
  @Test(expected = classOf[TableException])
  def testUnsupportedPartialInsert(): Unit = {
    val tmpFile = File.createTempFile("flink-sql-table-sink-test2", ".tmp")
    tmpFile.deleteOnExit()
    val path = tmpFile.toURI.toString

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("sourceTable", t)

    val fieldTypes = tEnv.scan("sourceTable").getSchema.getTypes
    val fieldNames = Seq("d", "e", "f").toArray
    val sink = new CsvSQLTableSink(path, fieldTypes, fieldNames, ",")
    tEnv.registerTableSink("targetTable", sink)

    val sql = "INSERT INTO targetTable (d, f) SELECT a, c FROM sourceTable"
    tEnv.sql(sql)
  }
}
