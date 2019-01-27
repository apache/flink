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

import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TestPartitionableTableSource
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test

import scala.collection.JavaConverters._

class TableSourceITCase extends BatchTestBase {

  @Test
  def testCsvTableSourceWithProjectionWithCountStar(): Unit = {
    val csvTable = CommonTestData.getCsvTableSource

    tEnv.registerTableSource("csvTable", csvTable)

    val results = tEnv.sqlQuery("SELECT COUNT(*) FROM csvTable").collect()

    val expected = "8"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testPartitionableTableSourceWithStringPartitionEqualsToInt(): Unit = {

    tEnv.registerTableSource("partitionable_table", new TestPartitionableTableSource)
    val results = tEnv.sqlQuery(
      "select * from partitionable_table where is_ok and part = 2").collect()

    val expected = Seq("3,John,2,part=2,true", "4,nosharp,2,part=2,true").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
