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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
  * Created by suez on 5/5/17.
  */
@RunWith(classOf[Parameterized])
class UnnestITCase(configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  @Test
  def testUnnestSQL(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.getSmall3TupleDataSetWithArray(env).toTable(tEnv, 'a, 'b, 'c)

    tEnv.registerTable("MyTable", ds)

    val sqlQuery = "SELECT a, s FROM MyTable as mt, unnest(mt.c) as T(s)"

    val result = tEnv.sql(sqlQuery)

    val expected = List("1,Hi", "1,w", "2,Hello", "2,k", "3,Hello world", "3,x")
    val results = result.toDataSet[Row].collect().toList
    assertEquals(expected.toString(), results.sortWith(_.toString < _.toString).toString())
  }

  @Test
  def testUnnestSQLWithTuple(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val ds = CollectionDataSets.getSmall3TupleDataSetWithTupleArray(env).toTable(tEnv, 'a, 'b)

    tEnv.registerTable("MyTable", ds)

    val sqlQuery = "SELECT a, b, s, t FROM MyTable as mt, unnest(mt.b) as T(s, t) where s > 13"

    val result = tEnv.sql(sqlQuery)

    val expected = List("2,[(13,41.6), (14,45.2136)],14,45.2136",
      "3,[(18,42.6)],18,42.6")
    val results = result.toDataSet[Row].collect().toList
    assertEquals(expected.toString(), results.sortWith(_.toString < _.toString).toString())
  }
}
