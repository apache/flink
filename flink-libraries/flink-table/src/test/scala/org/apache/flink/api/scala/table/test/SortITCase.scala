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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class SortITCase(
    mode: TestExecutionMode,
    configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testOrderByDesc(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    
    val t = env.fromElements((1, "First"), (3, "Third"), (2, "Second")).toTable(tEnv)
      .orderBy('_1.desc)

    val expected = "3,Third\n2,Second\n1,First"
    val results = t.toDataSet[Row].setParallelism(1).collect()
    compareOrderedResultAsText(expected, results)
  }

  @Test
  def testOrderByAsc(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    
    val t = env.fromElements((1, "First"), (3, "Third"), (2, "Second")).toTable(tEnv)
      .orderBy('_1.asc)

    val expected = "1,First\n2,Second\n3,Third"
    val results = t.toDataSet[Row].setParallelism(1).collect()
    compareOrderedResultAsText(expected, results)
  }

  @Test
  def testOrderByMultipleFieldsDifferentDirections(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)
    
    env.setParallelism(2)
    val t = env.fromElements((1, 3, "Third"), (1, 2, "Fourth"), (1, 4, "Second"),
      (2, 1, "Sixth"), (1, 5, "First"), (1, 1, "Fifth"))
      .toTable(tEnv).orderBy('_1.asc, '_2.desc)

    val expected = "1,5,First\n1,4,Second\n1,3,Third\n1,2,Fourth\n1,1,Fifth\n2,1,Sixth"
    val results = t.toDataSet[Row].mapPartition(rows => Seq(rows.toSeq)).collect()

    implicit def rowOrdering = Ordering.by((x : Row) => (x.productElement(0).asInstanceOf[Int],
      - x.productElement(1).asInstanceOf[Int]))

    val result = results.sortBy(p => p.min).reduceLeft(_ ++ _)

    compareOrderedResultAsText(expected, result)
  }

  @Test
  def testOrderByMultipleFieldsWithSql(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC, _2 DESC"

    val t = env.fromElements((1, 1, "First"), (2, 3, "Fourth"), (1, 2, "Second"),
      (2, 1, "Third")).toTable(tEnv)
    tEnv.registerDataSet("MyTable", t)

    val queryResult = tEnv.sql(sqlQuery)

    val expected = "2,3,Fourth\n2,1,Third\n1,2,Second\n1,1,First"
    val results = queryResult.toDataSet[Row].setParallelism(1).collect()
    compareOrderedResultAsText(expected, results)
  }

  private def compareOrderedResultAsText[T](expected: String, results: Seq[T]) = {
    if (configMode == TableConfigMode.EFFICIENT) {
      results match {
        case x if x.exists(_.isInstanceOf[Tuple]) =>
          TestBaseUtils.compareOrderedResultAsText(results.asJava, expected, true)
        case x if x.exists(_.isInstanceOf[Product]) =>
          TestBaseUtils.compareOrderedResultAsText(results.asInstanceOf[Seq[Product]]
            .map(_.productIterator.mkString(",")).asJava, expected)
        case _ => TestBaseUtils.compareOrderedResultAsText(results.asJava, expected)
      }
    } else {
      TestBaseUtils.compareOrderedResultAsText(results.asJava, expected)
    }
  }

}
