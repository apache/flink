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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class SetOperatorsTest extends TableTestBase {

  @Test
  def testMinusWithNestedTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Long, (Int, String), Array[Boolean])]("MyTable", 'a, 'b, 'c)

    val expected = binaryNode(
      "DataSetMinus",
      batchTableNode(0),
      batchTableNode(0),
      term("minus", "a", "b", "c")
    )

    val result = t.minus(t)

    util.verifyTable(result, expected)
  }

  @Test
  def testExists(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Int, String)]("A", 'a_long, 'a_int, 'a_string)
    util.addTable[(Long, Int, String)]("B", 'b_long, 'b_int, 'b_string)

    val expected = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        batchTableNode(0),
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            binaryNode(
              "DataSetJoin",
              unaryNode(
                "DataSetCalc",
                batchTableNode(1),
                term("select", "b_long")
              ),
              unaryNode(
                "DataSetAggregate",
                unaryNode(
                  "DataSetCalc",
                  batchTableNode(0),
                  term("select", "a_long")
                ),
                term("groupBy", "a_long"),
                term("select", "a_long")
              ),
              term("where", "=(a_long, b_long)"),
              term("join", "b_long", "a_long"),
              term("joinType", "InnerJoin")
            ),
            term("select", "true AS $f0", "a_long")
          ),
          term("groupBy", "a_long"),
          term("select", "a_long", "MIN($f0) AS $f1")
        ),
        term("where", "=(a_long, a_long0)"),
        term("join", "a_long", "a_int", "a_string", "a_long0", "$f1"),
        term("joinType", "InnerJoin")
      ),
      term("select", "a_int", "a_string")
    )

    util.verifySql(
      "SELECT a_int, a_string FROM A WHERE EXISTS(SELECT * FROM B WHERE a_long = b_long)",
      expected
    )
  }

}
