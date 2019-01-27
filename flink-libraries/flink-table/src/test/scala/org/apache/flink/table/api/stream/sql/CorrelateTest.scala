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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.batch.sql.StringSplit
import org.apache.flink.table.expressions.utils.Func19
import org.apache.flink.table.runtime.utils.JavaUserDefinedTableFunctions.JavaVarsArgTableFunc0
import org.apache.flink.table.util._
import org.apache.flink.types.Row

import org.junit.Test

class CorrelateTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCrossJoinWithOverloading(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    // func1 has overloading eval function
    util.addFunction("func1", func1)

    val sql = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(c, '$')) AS T(s)"

    util.verifyPlan(sql)
  }

  @Test
  def testLeftOuterJoinWithLiteralTrue(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON TRUE"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinAsSubQuery(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("MyTable2", 'a2, 'b2, 'c2)
    util.addFunction("func1", func1)

    val sqlQuery =
      """
        | SELECT *
        | FROM MyTable2 LEFT OUTER JOIN
        |  (SELECT c, s
        |   FROM MyTable LEFT OUTER JOIN LATERAL TABLE(func1(c)) AS T(s) on true)
        | ON c2 = s """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test(expected = classOf[org.apache.calcite.tools.ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON c = s"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCustomType(): Unit = {
    val util = streamTestUtil()
    val func2 = new TableFunc2
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, name, len FROM MyTable, LATERAL TABLE(func2(c)) AS T(name, len)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testHierarchyType(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new HierarchyTableFunction
    util.addFunction("hierarchy", function)

    val sqlQuery = "SELECT c, T.* FROM MyTable, LATERAL TABLE(hierarchy(c)) AS T(name, adult, len)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPojoType(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new PojoTableFunc
    util.addFunction("pojo", function)

    val sqlQuery = "SELECT c, name, age FROM MyTable, LATERAL TABLE(pojo(c))"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRowType(): Unit = {
    val util = streamTestUtil()
    val rowType = Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT))
    util.addTable[Row]("MyTable", 'a, 'b, 'c)(rowType)
    val function = new TableFunc6
    util.addFunction("tableFunc6", function)

    val sqlQuery = "SELECT c, tf.f2 FROM MyTable, LATERAL TABLE(tableFunc6(c)) AS tf"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFilter(): Unit = {
    val util = streamTestUtil()
    val func2 = new TableFunc2
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, name, len FROM MyTable, LATERAL TABLE(func2(c)) AS T(name, len) " +
      "WHERE len > 2"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testScalarFunction(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(SUBSTRING(c, 2))) AS T(s)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTableFunctionWithJavaVariableArguments(): Unit = {
    val util = streamTestUtil()
    val func1 = new JavaVarsArgTableFunc0
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1('hello', 'world', c)) AS T(s)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTableFunctionWithScalaVariableArguments(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    // test scala var arg function
    val func2 = new VarArgsFunc0
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func2('hello', 'world', c)) AS T(s)"

    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDynamicTypeWithSQL(): Unit = {
    val util = streamTestUtil()
    val funcDyna0 = new UDTFWithDynamicType0
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("funcDyna0", funcDyna0)
    val sqlQuery = "SELECT c,name,len0,len1,name1,len10 FROM MyTable JOIN " +
        "LATERAL TABLE(funcDyna0(c, 'string,int,int')) AS T1(name,len0,len1) ON TRUE JOIN " +
        "LATERAL TABLE(funcDyna0(c, 'string,int')) AS T2(name1,len10) ON TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDynamicTypeWithSQLAndVariableArgs(): Unit = {
    val util = streamTestUtil()
    val funcDyna0 = new UDTFWithDynamicTypeAndVariableArgs
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("funcDyna0", funcDyna0)
    val sqlQuery = "SELECT c,name,len0,len1,name1,len10 FROM MyTable JOIN " +
        "LATERAL TABLE(funcDyna0(c, 'string,int,int', 'a', 'b', 'c')) " +
        "AS T1(name,len0,len1) ON TRUE JOIN " +
        "LATERAL TABLE(funcDyna0(c, 'string,int', 'a', 'b', 'c')) AS T2(name1,len10) ON TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDynamicTypeWithSQLAndVariableArgsWithMultiEval(): Unit = {
    val util = streamTestUtil()
    val funcDyna0 = new UDTFWithDynamicTypeAndVariableArgs
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("funcDyna0", funcDyna0)
    val sqlQuery = "SELECT a, b, c, d, e FROM MyTable JOIN " +
        "LATERAL TABLE(funcDyna0(a)) AS T1(d, e) ON TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCorrelate(): Unit = {
    val util = streamTestUtil()
    util.tableEnv.registerFunction("str_split", new StringSplit())
    util.addTable[(Int, Long, String, Array[Byte])]("MyTable2", 'a, 'b, 'c, 'd)
    val sqlQuery = "SELECT a, d, s FROM MyTable2, LATERAL TABLE(str_split(d)) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCorrelateProjectable(): Unit = {
    val util = streamTestUtil()
    util.addTable[(String, Int, Array[Byte])]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc5
    util.addFunction("parser", function)
    util.addFunction("objHash", Func19)

    val sqlQuery = "SELECT len, objHash(c, len) as hash FROM MyTable, LATERAL TABLE(parser(a)) AS" +
      " T(name, len) where objHash(c, len) > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputAllProjectable(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte])]("MyTable", 'a)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT name, len FROM MyTable, LATERAL TABLE(parser(a)) AS T(name, len)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputAllProjectable2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte])]("MyTable", 'a)
    val function = new TableFunc5
    util.addFunction("parser", function)
    util.addFunction("objHash", Func19)

    val sqlQuery = "SELECT name, objHash(name), len FROM MyTable, LATERAL TABLE(parser(a)) AS T" +
      "(name, len) where objHash(name) > 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputAllProjectableWithRowTime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte], Long)]("MyTable", 'a, 'b.rowtime)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT name, len FROM MyTable, LATERAL TABLE(parser(a)) AS T(name, len)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputAllProjectableWithProcTime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte])]("MyTable", 'a)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT name, len  FROM (select a, proctime() as proctime from " +
      "MyTable), LATERAL TABLE(parser(a)) AS T(name, len)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputNotAllProjectable(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte])]("MyTable", 'a)
    util.addFunction("objHash", Func19)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT objHash(a) hash_a, name, len FROM MyTable, LATERAL TABLE(parser(a)) AS" +
      " T(name, len)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputNotAllProjectable2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte], String, Int)]("MyTable", 'a, 'b, 'c)
    util.addFunction("objHash", Func19)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT name, len, c FROM MyTable, LATERAL TABLE(parser(a)) AS" +
      " T(name, len) where objHash(a, len) <> 0"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputNotAllProjectableWithRowTime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte], Long)]("MyTable", 'a, 'b.rowtime)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT name, len, b FROM MyTable, LATERAL TABLE(parser(a)) AS T(name, len)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftInputNotAllProjectableWithProcTime(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte])]("MyTable", 'a)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT name, len, proc  FROM (select a, proctime() as proc from " +
      "MyTable), LATERAL TABLE(parser(a)) AS T(name, len)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCountStarOnCorrelate(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Array[Byte])]("MyTable", 'a)
    val function = new TableFunc5
    util.addFunction("parser", function)

    val sqlQuery = "SELECT count(*) FROM MyTable, LATERAL TABLE(parser(a)) AS T(name, len)"
    util.verifyPlan(sqlQuery)
  }
}
