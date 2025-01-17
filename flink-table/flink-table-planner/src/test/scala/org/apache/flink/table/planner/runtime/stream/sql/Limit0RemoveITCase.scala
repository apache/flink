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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils._

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import scala.collection.JavaConversions._

class Limit0RemoveITCase extends StreamingTestBase() {

  @Test
  def testSimpleLimitRemove(): Unit = {
    val ds = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable", table)

    val sql = "SELECT * FROM MyTable LIMIT 0"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.insertOnly())
    result.executeInsert(sinkName).await()

    assertThat(TestValuesTableFactory.getResults(sinkName).size()).isZero
  }

  @Test
  def testLimitRemoveWithOrderBy(): Unit = {
    val ds = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable", table)

    val sql = "SELECT * FROM MyTable ORDER BY a LIMIT 0"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.insertOnly())
    result.executeInsert(sinkName).await()

    assertThat(TestValuesTableFactory.getResults(sinkName).size()).isZero
  }

  @Test
  def testLimitRemoveWithSelect(): Unit = {
    val ds = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table = ds.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable", table)

    val sql = "select a2 from (select cast(a as int) a2 from MyTable limit 0)"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.insertOnly())
    result.executeInsert(sinkName).await()

    assertThat(TestValuesTableFactory.getResults(sinkName).size()).isZero
  }

  @Test
  def testLimitRemoveWithIn(): Unit = {
    val ds1 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable1", table1)

    val ds2 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE a IN (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.insertOnly())
    result.executeInsert(sinkName).await()

    assertThat(TestValuesTableFactory.getResults(sinkName).size()).isZero
  }

  @Test
  def testLimitRemoveWithNotIn(): Unit = {
    val ds1 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable1", table1)

    val ds2 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE a NOT IN (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.insertOnly())
    result.executeInsert(sinkName).await()

    val expected = Seq("+I[1]", "+I[2]", "+I[3]", "+I[4]", "+I[5]", "+I[6]")
    assertThat(
      TestValuesTableFactory
        .getResultsAsStrings(sinkName)
        .sorted).isEqualTo(expected)
  }

  @Test
  def testLimitRemoveWithExists(): Unit = {
    val ds1 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable1", table1)

    val ds2 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE EXISTS (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.all())
    result.executeInsert(sinkName).await()

    assertThat(TestValuesTableFactory.getRawResults(sinkName).size()).isZero
  }

  @Test
  def testLimitRemoveWithNotExists(): Unit = {
    val ds1 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable1", table1)

    val ds2 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a)
    tEnv.createTemporaryView("MyTable2", table2)

    val sql = "SELECT * FROM MyTable1 WHERE NOT EXISTS (SELECT a FROM MyTable2 LIMIT 0)"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.all())
    result.executeInsert(sinkName).await()

    val expected = Seq("+I[1]", "+I[2]", "+I[3]", "+I[4]", "+I[5]", "+I[6]")
    assertThat(
      TestValuesTableFactory
        .getResultsAsStrings(sinkName)
        .sorted).isEqualTo(expected)
  }

  @Test
  def testLimitRemoveWithJoin(): Unit = {
    val ds1 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3, 4, 5, 6))
    val table1 = ds1.toTable(tEnv, 'a1)
    tEnv.createTemporaryView("MyTable1", table1)

    val ds2 = StreamingEnvUtil.fromCollection(env, Seq(1, 2, 3))
    val table2 = ds2.toTable(tEnv, 'a2)
    tEnv.createTemporaryView("MyTable2", table2)

    val sql = "SELECT a1 FROM MyTable1 INNER JOIN (SELECT a2 FROM MyTable2 LIMIT 0) ON true"

    val result = tEnv.sqlQuery(sql)

    val sinkName = "MySink"
    TestSinkUtil.addValuesSink(tEnv, sinkName, result, ChangelogMode.insertOnly())
    result.executeInsert(sinkName).await()

    assertThat(TestValuesTableFactory.getResults(sinkName).size()).isZero
  }
}
