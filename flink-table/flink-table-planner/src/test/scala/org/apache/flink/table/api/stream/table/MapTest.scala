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
import org.apache.flink.table.expressions.utils.{Func1, Func23, Func24}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil.{streamTableNode, term, unaryNode}
import org.junit.Test

class MapTest extends TableTestBase {

  @Test
  def testSimpleMap(): Unit = {
    val util = streamTestUtil()

    val resultTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
      .map(Func23('a, 'b, 'c))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "Func23$(a, b, c).f0 AS _c0, Func23$(a, b, c).f1 AS _c1, " +
        "Func23$(a, b, c).f2 AS _c2, Func23$(a, b, c).f3 AS _c3")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testScalarResult(): Unit = {

    val util = streamTestUtil()

    val resultTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
      .map(Func1('a))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "Func1$(a) AS _c0")
    )

    util.verifyTable(resultTable, expected)
  }

  @Test
  def testMultiMap(): Unit = {
    val util = streamTestUtil()

    val resultTable = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
      .map(Func23('a, 'b, 'c))
      .map(Func24('_c0, '_c1, '_c2, '_c3))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select",
           "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f0 AS _c0, " +
             "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f1 AS _c1, " +
             "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f2 AS _c2, " +
             "Func24$(Func23$(a, b, c).f0, Func23$(a, b, c).f1, " +
             "Func23$(a, b, c).f2, Func23$(a, b, c).f3).f3 AS _c3")
    )

    util.verifyTable(resultTable, expected)
  }
}
