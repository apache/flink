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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.sinks.CollectRowTableSink
import org.junit._

class InsertITCase extends BatchTestBase {

  @Test
  def testInsertWithOrderBy(): Unit = {
    val csvTable = CommonTestData.get3Source()
    tEnv.registerTableSource("MyTable", csvTable)
    val fieldNames = Array("d", "e", "f")
    val fieldTypes = tEnv.scan("MyTable").getSchema.getTypes.asInstanceOf[Array[DataType]]
    val sink = new CollectRowTableSink
    tEnv.registerTableSink("targetTable", fieldNames, fieldTypes, sink)
    val sql = "INSERT INTO targetTable SELECT * FROM MyTable order by _1"
    tEnv.sqlUpdate(sql)
  }
}
