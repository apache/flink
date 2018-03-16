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

package org.apache.flink.table.api.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{binaryNode, streamTableNode, term, unaryNode}
import org.junit.Test

class SetOperatorsTest extends TableTestBase {

  @Test
  def testFilterUnionTranspose(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.unionAll(right)
      .where('a > 0)
      .groupBy('b)
      .select('a.sum as 'a, 'b as 'b, 'c.count as 'c)

    val expected = unaryNode(
      "DataStreamCalc",
        unaryNode(
          "DataStreamGroupAggregate",
          binaryNode(
            "DataStreamUnion",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(0),
              term("select", "a", "b", "c"),
              term("where", ">(a, 0)")
            ),
            unaryNode(
              "DataStreamCalc",
              streamTableNode(1),
              term("select", "a", "b", "c"),
              term("where", ">(a, 0)")
            ),
            term("union all", "a", "b", "c")
          ),
          term("groupBy", "b"),
          term("select", "b", "SUM(a) AS TMP_0", "COUNT(c) AS TMP_1")
        ),
      term("select", "TMP_0 AS a", "b", "TMP_1 AS c")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testProjectUnionTranspose(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Int, Long, String)]("left", 'a, 'b, 'c)
    val right = util.addTable[(Int, Long, String)]("right", 'a, 'b, 'c)

    val result = left.select('a, 'b, 'c)
      .unionAll(right.select('a, 'b, 'c))
      .select('b, 'c)

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "b", "c")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(1),
        term("select", "b", "c")
      ),
      term("union all", "b", "c")
    )

    util.verifyTable(result, expected)
  }

}
