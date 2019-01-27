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

package org.apache.flink.table.codegen

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.batch.sql.TestData._
import org.junit.{Before, Ignore, Test}

import scala.collection.Seq

/**
  * copying test cases which can not pass the test during code split development
  * set 'sql.codegen.maxLength' to 1 to verify these test cases
  */
class CodeSplitBatchITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX, 1)
    registerCollection("Table3", nullData3, type3, nullablesOfData3, 'a, 'b, 'c)
    registerCollection("Table5", data5, type5, nullablesOfData5, 'd, 'e, 'f, 'g, 'h)
    registerCollection("Table4", data3, type3, nullablesOfData3, 'a, 'b, 'c)
    registerCollection("NullTable3", nullData3, type3, nullablesOfNullData3, 'a, 'b, 'c)
    registerCollection("NullTable5", nullData5, type5, nullablesOfNullData5, 'd, 'e, 'f, 'g, 'h)
    registerCollection("testTable", buildInData, buildInType, 'a,'b,'c,'d,'e,'f,'g,'h,'i,'j)

  }

  @Test
  def testBitOperateFunction(): Unit = {
    checkResult(
      "SELECT BITOR(3, 4), BITOR(-3, 2)," +
        "BITXOR(3, 4), BITXOR(-3, 2)," +
        "BITNOT(-3), BITNOT(3)," +
        "BITAND(3, 2), BITAND(-3, -2)," +
        "BIN(2), BIN(-7), BIN(-1) FROM testTable WHERE c = 2",
      Seq(row(7, -1, 7, -1, 2, -4, 2, -4, "10",
        "1111111111111111111111111111111111111111111111111111111111111001",
        "1111111111111111111111111111111111111111111111111111111111111111"
      )))
  }

  @Test
  def testLike(): Unit = {
    checkResult(
      "SELECT a FROM Table3 WHERE c LIKE '%llo%'",
      Seq(row(2), row(3), row(4)))

    checkResult(
      "SELECT a FROM Table3 WHERE CAST(a as VARCHAR(10)) LIKE CAST(b as VARCHAR(10))",
      Seq(row(1), row(2)))

    checkResult(
      "SELECT a FROM Table3 WHERE c NOT LIKE '%Comment%' AND c NOT LIKE '%Hello%'",
      Seq(row(1), row(5), row(6), row(null), row(null)))
  }

  @Test
  def testJoinWithJoinFilter(): Unit = {
    checkResult(
      "SELECT c, g FROM Table4, Table5 WHERE b = e AND a < 6",
      Seq(
        row("Hi", "Hallo"),
        row("Hello", "Hallo Welt"),
        row("Hello world", "Hallo Welt"),
        row("Hello world, how are you?", "Hallo Welt wie"),
        row("I am fine.", "Hallo Welt wie")
      ))
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    checkResult(
      "SELECT c, g FROM Table4, Table5 WHERE b = e AND a < 6 AND h < b",
      Seq(
        row("Hello world, how are you?", "Hallo Welt wie"),
        row("I am fine.", "Hallo Welt wie")
      ))
  }

  @Test
  def testAggregationAfterProjection(): Unit = {
    checkResult(
      "SELECT c, count(a) FROM " +
        "(SELECT d as a, f as b, h as c FROM Table5) GROUP BY c",
      Seq(
        row(1, 5),
        row(2, 7),
        row(3, 3)
      )
    )
  }
  
  @Ignore  // [BLINK-14928444]
  @Test
  def testWindowAggregationNormalWindowAgg(): Unit = {

    checkResult(
      "SELECT d, e, row_number() over (partition by d order by e desc), " +
        "rank() over (partition by d order by e desc)," +
        "dense_rank() over (partition by d order by e desc), " +
        "sum(e) over (partition by d order by e desc)," +
        "count(*) over (partition by d order by e desc)," +
        "avg(e) over (partition by d order by e desc)," +
        "max(e) over (partition by d order by e)," +
        "max(e) over (partition by d order by e desc)," +
        "min(e) over (partition by d order by e)," +
        "min(e) over (partition by d order by e desc) FROM Table5",
      Seq(
        // d  e  r  r  d  s  c  a  ma m mi m
        row( 1, 1, 1, 1, 1, 1, 1, 1.0, 1, 1, 1, 1),

        row( 2, 2, 2, 2, 2, 5, 2, 2.5, 2, 3, 2, 2),
        row( 2, 3, 1, 1, 1, 3, 1, 3.0, 3, 3, 2, 3),

        row( 3, 4, 3, 3, 3,15, 3, 5.0, 4, 6, 4, 4),
        row( 3, 5, 2, 2, 2,11, 2, 5.5, 5, 6, 4, 5),
        row( 3, 6, 1, 1, 1, 6, 1, 6.0, 6, 6, 4, 6),

        row( 4, 7, 4, 4, 4,34, 4, 8.5, 7,10, 7, 7),
        row( 4, 8, 3, 3, 3,27, 3, 9.0, 8,10, 7, 8),
        row( 4, 9, 2, 2, 2,19, 2, 9.5, 9,10, 7, 9),
        row( 4,10, 1, 1, 1,10, 1,10.0,10,10, 7,10),

        row( 5,11, 5, 5, 5,65, 5,13.0,11,15,11,11),
        row( 5,12, 4, 4, 4,54, 4,13.5,12,15,11,12),
        row( 5,13, 3, 3, 3,42, 3,14.0,13,15,11,13),
        row( 5,14, 2, 2, 2,29, 2,14.5,14,15,11,14),
        row( 5,15, 1, 1, 1,15, 1,15.0,15,15,11,15)
      )
    )
  }

}
