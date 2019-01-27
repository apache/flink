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

package org.apache.flink.table.tpc

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.sinks.parquet.ParquetTableSink
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.util.NodeResourceUtil.InferMode
import org.junit.Ignore

@Ignore
class ParquetTool extends BatchTestBase{

  val csvPath = "/Users/zhixin/data/tpch/SF1"
  val parquetPath = "/Users/zhixin/data/tpch/parquet"

  @org.junit.Test
  def test(): Unit = {
    for ((tableName, schema) <- TpcHSchemaProvider.schemaMap) {
      val schema = TpcHSchemaProvider.schemaMap(tableName)
      val csvTableSource = CsvTableSource.builder()
          .path(s"$csvPath/$tableName")
          .fields(schema.getFieldNames, schema.getFieldTypes)
          .fieldDelimiter("|")
          .lineDelimiter("\n").build()
      val tableSourceName = s"csv_$tableName"
      tEnv.registerTableSource(tableSourceName, csvTableSource)

      tEnv.getConfig.getConf.setString(
        TableConfigOptions.SQL_RESOURCE_INFER_MODE, InferMode.NONE.toString)
      tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 1)
      tEnv.sqlQuery(s"SELECT * from $tableSourceName")
          .writeToSink(new ParquetTableSink(s"$parquetPath/$tableName"))
      tEnv.execute()
    }
  }

}
