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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.test.util.TestBaseUtils

import org.junit.Test

import scala.collection.JavaConverters._

class OrcTableSourceITCase extends BatchTestBase {

  @Test
  def testBatchExecOrcTableSource(): Unit = {
    val expected = Seq(
      "1,Mike,Smith,12.3",
      "2,Bob,Taylor,45.6",
      "3,Sam,Miller,7.89",
      "4,Peter,Smith,0.12",
      "5,Liz,Williams,34.5",
      "6,Sally,Miller,6.78",
      "7,Alice,Smith,90.1",
      "8,Kelly,Williams,2.34").mkString("\n")

    val vectorColumnRowTable =
      CommonOrcTestData.getOrcVectorizedColumnRowTableSource(false)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last`, score FROM vectorColumnRowTable"
    checkBatchExecOrcSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecOrcPushDownProject(): Unit = {
    val expected = Seq(
      "1,Mike,Smith",
      "2,Bob,Taylor",
      "3,Sam,Miller",
      "4,Peter,Smith",
      "5,Liz,Williams",
      "6,Sally,Miller",
      "7,Alice,Smith",
      "8,Kelly,Williams").mkString("\n")

    val vectorColumnRowTable =
      CommonOrcTestData.getOrcVectorizedColumnRowTableSource(false)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql = "SELECT id, `first`, `last` FROM vectorColumnRowTable"
    checkBatchExecOrcSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecOrcPushDownFilter(): Unit = {
    val expected = Seq(
      "1,Mike1,Smith1",
      "2,Mike2,Smith2",
      "3,Mike3,Smith3").mkString("\n")

    // The reader will fetch first 1000 rows for Row stride must be at least 1000
    val vectorColumnRowTable =
      CommonOrcTestData.getBigOrcVectorizedColumnRowTableSource(false)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val vectorColumnRowSql =
      "SELECT id, `first`, `last` FROM vectorColumnRowTable " +
        "WHERE id < 4 AND substring(`first`, 1, 4) = 'Mike'"
    checkBatchExecOrcSource(vectorColumnRowTable,
      vectorColumnRowTableName, vectorColumnRowSql, expected)
  }

  @Test
  def testBatchExecHiveOrc(): Unit = {
    val expected = Seq(
      "Justin"
    ).mkString("\n")

    val vectorColumnRowTable =
      CommonOrcTestData.getOrcVectorizedColumnRowTableSourceFromPeopleFile(false)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val sql = "SELECT name FROM vectorColumnRowTable WHERE age BETWEEN 13 AND 19"
    checkBatchExecOrcSource(vectorColumnRowTable,
      vectorColumnRowTableName, sql, expected)
  }

  @Test
  def testBatchExecHiveOrcPushDownProject(): Unit = {
    val expected = Seq(
      "Michael",
      "Andy",
      "Justin"
    ).mkString("\n")

    val vectorColumnRowTable =
      CommonOrcTestData.getOrcVectorizedColumnRowTableSourceFromPeopleFile(false)
    val vectorColumnRowTableName = "vectorColumnRowTable"
    val sql = "SELECT name FROM vectorColumnRowTable"
    checkBatchExecOrcSource(vectorColumnRowTable,
      vectorColumnRowTableName, sql, expected)
  }

  private def checkBatchExecOrcSource(
      table: OrcTableSource[_],
      name: String,
      sql: String,
      expected: String): Unit = {
    conf.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_INFER_SOURCE_PARALLELISM_MAX, 1)
    tEnv.registerTableSource(name, table)
    val results = tEnv.sqlQuery(sql).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}
