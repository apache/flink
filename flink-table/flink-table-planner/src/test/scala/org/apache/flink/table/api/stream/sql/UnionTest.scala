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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api._
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.junit.Test

class UnionTest extends TableTestBase {

  @Test
  def testUnionAllNullableCompositeType() = {
    val streamUtil = streamTestUtil()
    val table = streamUtil.addTable[((Int, String), (Int, String), Int)]("A", 'a, 'b, 'c)

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "CASE(>(c, 0), b, null:RecordType:peek_no_expand(INTEGER _1, " +
          "VARCHAR(65536) _2)) AS EXPR$0")
      ),
      term("all", "true"),
      term("union all", "a")
    )

    streamUtil.verifySql(
      "SELECT a FROM A UNION ALL SELECT CASE WHEN c > 0 THEN b ELSE NULL END FROM A",
      expected
    )
  }

  @Test
  def testUnionAnyType(): Unit = {
    val streamUtil = streamTestUtil()
    val typeInfo = Types.ROW(
      new GenericTypeInfo(classOf[NonPojo]),
      new GenericTypeInfo(classOf[NonPojo]))
    val table = streamUtil.addJavaTable(typeInfo, "A", $("a"), $("b"))

    val expected = binaryNode(
      "DataStreamUnion",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "a")
      ),
      unaryNode(
        "DataStreamCalc",
        streamTableNode(table),
        term("select", "b")
      ),
      term("all", "true"),
      term("union all", "a")
    )

    streamUtil.verifyJavaSql("SELECT a FROM A UNION ALL SELECT b FROM A", expected)
  }
}
