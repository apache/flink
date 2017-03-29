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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.utils.TableTestUtil._

class GroupAggregationsTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testGroupingOnNonExistentField(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val ds = table
             // must fail. '_foo is not a valid field
             .groupBy('_foo)
             .select('a.avg)
  }

  @Test(expected = classOf[ValidationException])
  def testGroupingInvalidSelection(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val ds = table
             .groupBy('a, 'b)
             // must fail. 'c is not a grouping key or aggregation
             .select('c)
  }

  @Test
  def testGroupbyWithoutWindow() = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
                      .groupBy('b)
                      .select('a.count)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b")
          ),
          term("groupBy", "b"),
          term("select", "b", "COUNT(a) AS TMP_0")
        ),
        term("select", "TMP_0")
      )
    util.verifyTable(resultTable, expected)
  }


  @Test
  def testGroupAggregateWithConstant1(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
            .select('a, 4 as 'four, 'b)
            .groupBy('four, 'a)
            .select('four, 'b.sum)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "4 AS four", "b", "a")
          ),
          term("groupBy", "four", "a"),
          term("select", "four", "a", "SUM(b) AS TMP_0")
        ),
        term("select", "4 AS four", "TMP_0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithConstant2(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
            .select('b, 4 as 'four, 'a)
            .groupBy('b, 'four)
            .select('four, 'a.sum)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "4 AS four", "a", "b")
          ),
          term("groupBy", "four", "b"),
          term("select", "four", "b", "SUM(a) AS TMP_0")
        ),
        term("select", "4 AS four", "TMP_0")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithExpressionInSelect(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
            .select('a as 'a, 'b % 3 as 'd, 'c as 'c)
            .groupBy('d)
            .select('c.min, 'a.avg)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "c", "a", "MOD(b, 3) AS d")
          ),
          term("groupBy", "d"),
          term("select", "d", "MIN(c) AS TMP_0", "AVG(a) AS TMP_1")
        ),
        term("select", "TMP_0", "TMP_1")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
            .groupBy('b)
            .select('b, 'a.sum)
            .where('b === 2)

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "b", "a")
          ),
          term("groupBy", "b"),
          term("select", "b", "SUM(a) AS TMP_0")
        ),
        term("select", "b", "TMP_0"),
        term("where", "=(b, 2)")
      )
    util.verifyTable(resultTable, expected)
  }

  @Test
  def testGroupAggregateWithAverage(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)

    val resultTable = table
            .groupBy('b)
            .select('b, 'a.cast(BasicTypeInfo.DOUBLE_TYPE_INFO).avg)

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "b", "a", "CAST(a) AS a0")
        ),
        term("groupBy", "b"),
        term("select", "b", "AVG(a0) AS TMP_0")
      )

    util.verifyTable(resultTable, expected)
  }

}
