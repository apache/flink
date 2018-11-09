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
package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.runtime.stream.table.TestAppendSink
import org.apache.flink.table.utils.{ObjectTableFunction, TableFunc1, TableFunc2, TableTestBase}
import org.junit.Assert.{assertTrue, fail}
import org.junit.Test

class CorrelateValidationTest extends TableTestBase {

  @Test
  def testRegisterFunctionException(): Unit ={
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]('a, 'b, 'c)

    // check scala object is forbidden
    expectExceptionThrown(
      util.tableEnv.registerFunction("func3", ObjectTableFunction), "Scala object")
    expectExceptionThrown(
      util.javaTableEnv.registerFunction("func3", ObjectTableFunction), "Scala object")

    expectExceptionThrown(t.join(ObjectTableFunction('a, 1)), "Scala object")
  }

  @Test
  def testInvalidTableFunctions(): Unit = {
    val util = streamTestUtil()

    val func1 = new TableFunc1
    util.javaTableEnv.registerFunction("func1", func1)
    util.javaTableEnv.registerTableSink(
      "testSink", new TestAppendSink().configure(
        Array[String]("f"), Array[TypeInformation[_]](Types.INT)))

    // table function call select
    expectExceptionThrown(
      func1('c).select("f0"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call select
    expectExceptionThrown(
      func1('c).select('f0),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call insertInto
    expectExceptionThrown(
      func1('c).insertInto("testSink"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call distinct
    expectExceptionThrown(
      func1('c).distinct(),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call filter
    expectExceptionThrown(
      func1('c).filter('f0 === "?"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call filter
    expectExceptionThrown(
      func1('c).filter("f0 = '?'"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call limit
    expectExceptionThrown(
      func1('c).orderBy('f0).offset(3),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call limit
    expectExceptionThrown(
      func1('c).orderBy('f0).fetch(3),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call orderBy
    expectExceptionThrown(
      func1('c).orderBy("f0"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call orderBy
    expectExceptionThrown(
      func1('c).orderBy('f0),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call where
    expectExceptionThrown(
      func1('c).where("f0 = '?'"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

    // table function call where
    expectExceptionThrown(
      func1('c).where('f0 === "?"),
      "TableFunction can only be used in join and leftOuterJoin."
    )

  }

  @Test
  def testInvalidTableFunction(): Unit = {
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    //=================== check scala object is forbidden =====================
    // Scala table environment register
    expectExceptionThrown(util.addFunction("udtf", ObjectTableFunction), "Scala object")
    // Java table environment register
    expectExceptionThrown(
      util.tableEnv.registerFunction("udtf", ObjectTableFunction), "Scala object")
    // Scala Table API directly call
    expectExceptionThrown(t.join(ObjectTableFunction('a, 1)), "Scala object")


    //============ throw exception when table function is not registered =========
    // Java Table API call
    expectExceptionThrown(
      t.join(new Table(util.tableEnv, "nonexist(a)")
      ), "Undefined function: NONEXIST")
    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(nonexist(a))"),
      "No match found for function signature nonexist(<NUMERIC>)")


    //========= throw exception when the called function is a scalar function ====
    util.tableEnv.registerFunction("func0", Func0)

    // Java Table API call
    expectExceptionThrown(
      t.join(new Table(util.tableEnv, "func0(a)")),
      "only accept String that define table function",
      classOf[TableException])
    // SQL API call
    // NOTE: it doesn't throw an exception but an AssertionError, maybe a Calcite bug
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(func0(a))"),
      null,
      classOf[AssertionError])

    //========== throw exception when the parameters is not correct ===============
    // Java Table API call
    util.addFunction("func2", new TableFunc2)
    expectExceptionThrown(
      t.join(new Table(util.tableEnv, "func2(c, c)")),
      "Given parameters of function 'FUNC2' do not match any signature")
    // SQL API call
    expectExceptionThrown(
      util.tableEnv.sqlQuery("SELECT * FROM MyTable, LATERAL TABLE(func2(c, c))"),
      "Given parameters of function 'func2' do not match any signature.")
  }

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test (expected = classOf[ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result = table.leftOuterJoin(function('c) as 's, 'c === 's).select('c, 's).where('a > 10)

    util.verifyTable(result, "")
  }

  // ----------------------------------------------------------------------------------------------

  private def expectExceptionThrown(
      function: => Unit,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException])
    : Unit = {
    try {
      function
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The exception message '${e.getMessage}' doesn't contain keyword '$keywords'",
            e.getMessage.contains(keywords))
        }
      case e: Throwable => fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }
}
