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

/**
  * Test class for testing DataSetJoin plans.
  */
class DataSetJoinTest extends TableTestBase {

  @Test
  def testJoinLeftAndRightOuterJoin(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Int)]("A", 'a1, 'a2)
    util.addTable[(Int, Int)]("B", 'b1, 'b2)

    def queryAndExpected(joinType:String) = {
      val query = s"SELECT a2 FROM A $joinType JOIN (SELECT b1 FROM B) AS x ON a1 < b1"
      val expected =
        unaryNode(
          "DataSetCalc",
          unaryNode(
            "DataSetJoin",
            batchTableNode(0),
            term("where", "<(a1, b1)"),
            term("join", "a1", "a2", "b1"),
            term("joinType", s"${joinType.capitalize}OuterJoin")
          ),
          term("select", "a2")
        ) + "\n" +
          unaryNode(
            "DataSetCalc",
            batchTableNode(1),
            term("select", "b1")
          )
      (query, expected)
    }

    val left = queryAndExpected("left")
    val right = queryAndExpected("right")

    util.verifySql(left._1, left._2)
    util.verifySql(right._1, right._2)
  }

}
