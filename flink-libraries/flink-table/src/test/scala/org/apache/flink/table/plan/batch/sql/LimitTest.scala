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

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.{TableTestBase, TestLimitableTableSource}
import org.junit.Test


class LimitTest extends TableTestBase {

  private val util = batchTestUtil()
  private val typeInfos: Array[TypeInformation[_]] = Array(
    INT_TYPE_INFO, LONG_TYPE_INFO, STRING_TYPE_INFO)
  private val names = Array("a", "b", "c")
  private val rowType = new RowTypeInfo(typeInfos, names)
  util.addTable("LimitTable", new TestLimitableTableSource(null, rowType))

  @Test
  def testLimit(): Unit = {
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT a, c FROM MyTable LIMIT 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLimitWithOffset(): Unit = {
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT a, c FROM MyTable LIMIT 10 OFFSET 1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFetchWithOffset(): Unit = {
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT a, c FROM MyTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFetchWithLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable FETCH FIRST 10 ROWS ONLY"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLimitWithLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable LIMIT 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLimitWithOffsetAndLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable LIMIT 10 OFFSET 1"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFetchWithOffsetAndLimitSource(): Unit = {
    val sqlQuery = "SELECT a, c FROM LimitTable OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFetch(): Unit = {
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT a, c FROM MyTable FETCH FIRST 10 ROWS ONLY"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testOnlyOffset(): Unit = {
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery = "SELECT a, c FROM MyTable OFFSET 10 ROWS "
    util.printSql(sqlQuery)
  }
}
