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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData.numericType
import org.junit.{Before, Test}

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.Seq

class FlinkLimitRemoveRuleITCase extends BatchTestBase {
  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)

    lazy val numericData = Seq(
      row(null, 1L, 1.0f, 1.0d, JBigDecimal.valueOf(1)),
      row(2, null, 2.0f, 2.0d, JBigDecimal.valueOf(2)),
      row(3, 3L, null, 3.0d, JBigDecimal.valueOf(3)),
      row(3, 3L, 4.0f, null, JBigDecimal.valueOf(3))
    )

    registerCollection(
      "t1",
      numericData,
      numericType,
      "a, b, c, d, e",
      Seq(true, true, true, true, true))
    registerCollection(
      "t2",
      numericData,
      numericType,
      "a, b, c, d, e",
      Seq(true, true, true, true, true))
  }

  @Test
  def testSimpleLimitRemove(): Unit = {
    val sqlQuery = "select * from t1 limit 0"
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithOrderBy(): Unit = {
    val sqlQuery = "select * from t1 order by a limit 0"
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithJoin(): Unit = {
    val sqlQuery =
      """
        |select * from t1 inner join
        |(select * from t2 limit 0) on true
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithIn(): Unit = {
    val sqlQuery =
      """
        |select * from t1 where a in
        |(select a from t2 limit 0)
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithExists(): Unit = {
    val sqlQuery =
      """
        |select * from t1 where exists
        |(select a from t2 limit 0)
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }

  @Test
  def testLimitRemoveWithSelect(): Unit = {
    val sqlQuery =
      """
        |select * from (select a from t2 limit 0)
      """.stripMargin
    checkResult(sqlQuery, Seq())
  }
}
