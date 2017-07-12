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
package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

/**
  * Test for testing aggregate plans.
  */
class AggregationsTest extends TableTestBase {

  @Test
  def testGroupAggregateWithFilter(): Unit = {

    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val resultTable = sourceTable.groupBy('a)
      .select('a, 'a.avg, 'b.sum, 'c.count)
      .where('a === 1)

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "b", "c"),
      term("where", "=(a, 1)")
    )

    val expected = unaryNode(
      "DataSetAggregate",
      calcNode,
      term("groupBy", "a"),
      term("select",
        "a",
        "AVG(a) AS TMP_0",
        "SUM(b) AS TMP_1",
        "COUNT(c) AS TMP_2")
    )

    util.verifyTable(resultTable,expected)
  }

  @Test
  def testAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.select('a.avg,'b.sum,'c.count)

    val setValues = unaryNode(
      "DataSetValues",
      batchTableNode(0),
      tuples(List(null,null,null)),
      term("values","a","b","c")
    )
    val union = unaryNode(
      "DataSetUnion",
      setValues,
      term("union","a","b","c")
    )

    val expected = unaryNode(
      "DataSetAggregate",
      union,
      term("select",
        "AVG(a) AS TMP_0",
        "SUM(b) AS TMP_1",
        "COUNT(c) AS TMP_2")
    )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testAggregateWithFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val resultTable = sourceTable.select('a,'b,'c).where('a === 1)
      .select('a.avg,'b.sum,'c.count)

    val calcNode = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      // ReduceExpressionsRule will add cast for Project node by force
      // if the input of the Project node has constant expression.
      term("select", "CAST(1) AS a", "b", "c"),
      term("where", "=(a, 1)")
    )

    val setValues =  unaryNode(
      "DataSetValues",
      calcNode,
      tuples(List(null,null,null)),
      term("values","a","b","c")
    )

    val union = unaryNode(
      "DataSetUnion",
      setValues,
      term("union","a","b","c")
    )

    val expected = unaryNode(
      "DataSetAggregate",
      union,
      term("select",
        "AVG(a) AS TMP_0",
        "SUM(b) AS TMP_1",
        "COUNT(c) AS TMP_2")
    )

    util.verifyTable(resultTable, expected)
  }
}
