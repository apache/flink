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

package org.apache.flink.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.scala._
import org.apache.flink.table.CompositeFlatteningTest.{TestCaseClass, giveMeCaseClass}
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test


class CompositeFlatteningTest extends TableTestBase {

  @Test(expected = classOf[ValidationException])
  def testDuplicateFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    table.select('a.flatten(), 'a.flatten())
  }

  @Test
  def testMultipleFlatteningsTable(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    val result = table.select('a.flatten(), 'c, 'b.flatten())

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select",
        "a._1 AS a$_1",
        "a._2 AS a$_2",
        "c",
        "b._1 AS b$_1",
        "b._2 AS b$_2"
      )
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testMultipleFlatteningsSql(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select",
        "a._1 AS _1",
        "a._2 AS _2",
        "c",
        "b._1 AS _10",
        "b._2 AS _20"
      )
    )

    util.verifySql(
      "SELECT MyTable.a.*, c, MyTable.b.* FROM MyTable",
      expected)
  }

  @Test
  def testNestedFlattenings(): Unit = {
    val util = batchTestUtil()
    val table = util
      .addTable[((((String, TestCaseClass), Boolean), String), String)]("MyTable", 'a, 'b)

    val result = table.select('a.flatten(), 'b.flatten())

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select",
        "a._1 AS a$_1",
        "a._2 AS a$_2",
        "b"
      )
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testScalarFunctionAccess(): Unit = {
    val util = batchTestUtil()
    val table = util
      .addTable[(String, Int)]("MyTable", 'a, 'b)

    val result = table.select(
      giveMeCaseClass().get("my"),
      giveMeCaseClass().get("clazz"),
      giveMeCaseClass().flatten())

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select",
        "org.apache.flink.table.CompositeFlatteningTest.giveMeCaseClass$().my AS _c0",
        "org.apache.flink.table.CompositeFlatteningTest.giveMeCaseClass$().clazz AS _c1",
        "org.apache.flink.table.CompositeFlatteningTest.giveMeCaseClass$().my AS _c2",
        "org.apache.flink.table.CompositeFlatteningTest.giveMeCaseClass$().clazz AS _c3"
      )
    )

    util.verifyTable(result, expected)
  }

}

object CompositeFlatteningTest {

  case class TestCaseClass(my: String, clazz: Int)

  object giveMeCaseClass extends ScalarFunction {
    def eval(): TestCaseClass = {
      TestCaseClass("hello", 42)
    }

    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
      createTypeInformation[TestCaseClass]
    }
  }
}
