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

package org.apache.flink.table.runtime.batch.sql.joins

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.junit.{Before, Test}

import scala.collection.Seq

class NonKeyJoinITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    registerCollection("testData", intStringData, INT_STRING, "a, b", nullablesOfIntStringData)
    registerCollection("testData2", intIntData2, INT_INT, "c, d", nullablesOfIntIntData2)
    registerCollection("testData3", intIntData3, INT_INT, "e, f", nullablesOfIntIntData3)
    registerCollection(
      "leftT",
      SemiJoinITCase.leftT, INT_DOUBLE,
      "a, b",
      SemiJoinITCase.nullablesOfLeftT)
    registerCollection(
      "rightT",
      SemiJoinITCase.rightT,
      INT_DOUBLE,
      "c, d",
      SemiJoinITCase.nullablesOfRightT)
  }

  @Test
  def testInner(): Unit = {
    checkResult(
      """
          SELECT b, c, d FROM testData, testData2 WHERE a = 2
        """.stripMargin,
      row("2", 1, 1) ::
          row("2", 1, 2) ::
          row("2", 2, 1) ::
          row("2", 2, 2) ::
          row("2", 3, 1) ::
          row("2", 3, 2) :: Nil)

    checkResult(
      """
          SELECT b, c, d FROM testData, testData2 WHERE a < c
        """.stripMargin,
      row("1", 2, 1) ::
          row("1", 2, 2) ::
          row("1", 3, 1) ::
          row("1", 3, 2) ::
          row("2", 3, 1) ::
          row("2", 3, 2) :: Nil)

    checkResult(
      """
          SELECT b, c, d FROM testData JOIN testData2 ON a < c
        """.stripMargin,
      row("1", 2, 1) ::
          row("1", 2, 2) ::
          row("1", 3, 1) ::
          row("1", 3, 2) ::
          row("2", 3, 1) ::
          row("2", 3, 2) :: Nil)
  }

  @Test
  def testInnerExpr(): Unit = {
    checkResult(
      "SELECT * FROM testData2, testData3 WHERE c - e = 0",
      Seq(
        row(1, 1, 1, null),
        row(1, 2, 1, null),
        row(2, 1, 2, 2),
        row(2, 2, 2, 2)
      ))

    checkResult(
      "SELECT * FROM testData2, testData3 WHERE c - e = 1",
      Seq(
        row(2, 1, 1, null),
        row(2, 2, 1, null),
        row(3, 1, 2, 2),
        row(3, 2, 2, 2)
      ))
  }

  @Test
  def testNonKeySemi(): Unit = {
    checkResult(
      "SELECT * FROM testData3 WHERE EXISTS (SELECT * FROM testData2)",
      row(1, null) :: row(2, 2) :: Nil)
    checkResult(
      "SELECT * FROM testData3 WHERE NOT EXISTS (SELECT * FROM testData2)",
      Nil)
    checkResult(
      """
        |SELECT e FROM testData3
        |WHERE
        |  EXISTS (SELECT * FROM testData)
        |OR
        |  EXISTS (SELECT * FROM testData2)""".stripMargin,
      row(1) :: row(2) :: Nil)
    checkResult(
      """
        |SELECT a FROM testData
        |WHERE
        |  a IN (SELECT c FROM testData2)
        |OR
        |  a IN (SELECT e FROM testData3)""".stripMargin,
      row(1) :: row(2) :: row(3) :: Nil)
  }

  @Test
  def testComposedNonEqualConditionLeftAnti(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a < c AND b < d)",
      Seq(row(3, 3.0), row(6, null), row(null, 5.0), row(null, null)))
  }

  @Test
  def testComposedNonEqualConditionLeftSemi(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a < c AND b < d)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0)))
  }
}
