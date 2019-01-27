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
package org.apache.flink.table.plan.batch.sql

import org.apache.calcite.tools.ValidationException
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.util._
import org.junit.{Before, Test}

class TableFunctionTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.tableEnv.registerFunction("str_split", new StringSplit())
    util.tableEnv.registerFunction("funcDyn", new UDTFWithDynamicType0)

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  }

  @Test
  def testOnlyConstantTableFunc_EmptyParameters(): Unit = {
    val sqlQuery = "SELECT * FROM LATERAL TABLE(str_split()) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyConstantTableFunc_OneParameter(): Unit = {
    val sqlQuery = "SELECT * FROM LATERAL TABLE(str_split('a,b,c')) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyConstantTableFunc_MultiUDTFs(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T1(s), " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyConstantTableFunc_WithDynamicType(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "LATERAL TABLE(funcDyn('test#Hello world#Hi', 'string,int,int')) AS T1(name0,len0,len1), " +
      "LATERAL TABLE(funcDyn('abc#defijk', 'string,int')) AS T2(name1,len2) " +
      "WHERE name0 = 'test'"
    util.verifyPlan(sqlQuery)
  }

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test(expected = classOf[ValidationException])
  def testOnlyConstantTableFunc_LeftJoin(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T1(s) LEFT JOIN " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x) ON x = s"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyConstantTableFunc_RightJoin(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T1(s) RIGHT JOIN " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x) ON TRUE WHERE s = x"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testOnlyConstantTableFunc_FullJoin(): Unit = {
    val sqlQuery = "SELECT * FROM " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T1(s) FULL JOIN " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x) ON s = x"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_EmptyParameters(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable, LATERAL TABLE(str_split()) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_OneParameter(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable, LATERAL TABLE(str_split('a,b,c')) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_SameTypeParameters(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable, LATERAL TABLE(str_split('a,b,c', ',')) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_DiffTypeParameters(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable, LATERAL TABLE(str_split('a,b,c', ',', 1)) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_WithUDF(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable, " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_MultiUDTFs1(): Unit = {
    val sqlQuery = "SELECT a, s, x FROM MyTable, " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T1(s), " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_MultiUDTFs2(): Unit = {
    val sqlQuery = "SELECT a, s, x FROM " +
      "LATERAL TABLE(str_split(SUBSTRING('a,b,c', 1, 3), ',')) as T1(s), " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T2(x)," +
      "MyTable where a > 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_WithDynamicType(): Unit = {
    val sqlQuery = "SELECT c, name0, len0, len1, name1, len2 FROM MyTable JOIN " +
      "LATERAL TABLE(funcDyn('test', 'string,int,int')) AS T1(name0,len0,len1) ON TRUE JOIN " +
      "LATERAL TABLE(funcDyn('abc', 'string,int')) AS T2(name1,len2) ON TRUE " +
      "WHERE c = 'Anna#44' AND name0 = 'test'"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_WithJoinCondition(): Unit = {
    val sqlQuery =
      "SELECT a, s FROM MyTable, LATERAL TABLE(str_split('a,b,c', ',')) as T(s) WHERE s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_InnerJoin(): Unit = {
    val sqlQuery =
      "SELECT a, s FROM LATERAL TABLE(str_split('a,b,c', ',')) as T(s), MyTable WHERE s = c"
    util.verifyPlan(sqlQuery)
  }

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test(expected = classOf[ValidationException])
  def testConstantTableFunc_LeftJoin1(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable LEFT JOIN " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T(s) ON s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_LeftJoin2(): Unit = {
    val sqlQuery = "SELECT a, s FROM LATERAL TABLE(str_split('a,b,c', ',')) as T(s) " +
      "LEFT JOIN MyTable ON s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_RightJoin1(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable RIGHT JOIN " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T(s) ON s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_RightJoin2(): Unit = {
    val sqlQuery = "SELECT a, s FROM LATERAL TABLE(str_split('a,b,c', ',')) as T(s) " +
      "RIGHT JOIN MyTable ON s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_FullJoin1(): Unit = {
    val sqlQuery = "SELECT a, s FROM MyTable FULL JOIN " +
      "LATERAL TABLE(str_split('a,b,c', ',')) as T(s) ON s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConstantTableFunc_FullJoin2(): Unit = {
    val sqlQuery = "SELECT a, s FROM LATERAL TABLE(str_split('a,b,c', ',')) as T(s) " +
      "FULL JOIN MyTable ON s = c"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCorrelate(): Unit = {
    util.addTable[(Int, Long, String, Array[Byte])]("MyTable2", 'a, 'b, 'c, 'd)
    val sqlQuery = "SELECT a, d, s FROM MyTable2, LATERAL TABLE(str_split(d)) as T(s)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinAsSubQuery(): Unit = {
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable2", 'a2, 'b2, 'c2)
    util.tableEnv.registerFunction("func1", func1)
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
  @Test(expected = classOf[ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val func1 = new TableFunc1
    util.tableEnv.registerFunction("func1", func1)
    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON c = s"
    util.verifyPlan(sqlQuery)
  }
}

class StringSplit extends TableFunction[String] {
  def eval(): Unit = {
    Array("a", "b", "c").foreach(collect)
  }

  def eval(str: String): Unit = {
    if (null != str) {
      StringUtils.split(str, ",").foreach(collect)
    }
  }

  def eval(str: String, splitChar: String): Unit = {
    if (null != str && null != splitChar) {
      StringUtils.split(str, splitChar).foreach(collect)
    }
  }

  def eval(str: String, splitChar: String, startIndex: Int): Unit = {
    if (null != str && null != splitChar) {
      val result = StringUtils.split(str, splitChar)
      val idx = if (startIndex <= 0) 0 else startIndex
      if (idx < result.length) {
        result.drop(idx).foreach(collect)
      }
    }
  }

  def eval(varbinary: Array[Byte]): Unit = {
    if (null != varbinary) {
      StringUtils.split(new String(varbinary), ",").foreach(collect)
    }
  }

  override def getResultType(arguments: Array[AnyRef], argTypes: Array[Class[_]]): DataType = {
    DataTypes.STRING
  }
}
