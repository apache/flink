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
package org.apache.flink.api.scala.batch

import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => ScalaExecutionEnv, _}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{DataSet => JDataSet, ExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.api.table.expressions.utils.{HierarchyTableFunction, PojoTableFunc, TableFunc1, TableFunc2}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.utils.TableTestBase
import org.apache.flink.api.table.utils.TableTestUtil._
import org.apache.flink.api.table.{Row, TableEnvironment, Types}
import org.junit.Test
import org.mockito.Mockito._


class UserDefinedTableFunctionTest extends TableTestBase {

  @Test
  def testTableAPI(): Unit = {
    // mock
    val ds = mock(classOf[DataSet[Row]])
    val jDs = mock(classOf[JDataSet[Row]])
    val typeInfo: TypeInformation[Row] = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING))
    when(ds.javaSet).thenReturn(jDs)
    when(jDs.getType).thenReturn(typeInfo)

    // Scala environment
    val env = mock(classOf[ScalaExecutionEnv])
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val in1 = ds.toTable(tableEnv).as('a, 'b, 'c)

    // Java environment
    val javaEnv = mock(classOf[JavaExecutionEnv])
    val javaTableEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val in2 = javaTableEnv.fromDataSet(jDs).as("a, b, c")
    javaTableEnv.registerTable("MyTable", in2)

    // test cross apply
    val func1 = new TableFunc1
    javaTableEnv.registerFunction("func1", func1)
    var scalaTable = in1.crossApply(func1('c) as ('s)).select('c, 's)
    var javaTable = in2.crossApply("func1(c) as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test outer apply
    scalaTable = in1.outerApply(func1('c) as ('s)).select('c, 's)
    javaTable = in2.outerApply("func1(c) as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test overloading
    scalaTable = in1.crossApply(func1('c, "$") as ('s)).select('c, 's)
    javaTable = in2.crossApply("func1(c, '$') as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    javaTableEnv.registerFunction("func2", func2)
    scalaTable = in1.crossApply(func2('c) as ('name, 'len)).select('c, 'name, 'len)
    javaTable = in2.crossApply("func2(c) as (name, len)").select("c, name, len")
    verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    javaTableEnv.registerFunction("hierarchy", hierarchy)
    scalaTable = in1.crossApply(hierarchy('c) as ('name, 'adult, 'len))
      .select('c, 'name, 'len, 'adult)
    javaTable = in2.crossApply("hierarchy(c) as (name, adult, len)")
      .select("c, name, len, adult")
    verifyTableEquals(scalaTable, javaTable)

    // test pojo type
    val pojo = new PojoTableFunc
    javaTableEnv.registerFunction("pojo", pojo)
    scalaTable = in1.crossApply(pojo('c))
      .select('c, 'name, 'age)
    javaTable = in2.crossApply("pojo(c)")
      .select("c, name, age")
    verifyTableEquals(scalaTable, javaTable)

    // test with filter
    scalaTable = in1.crossApply(func2('c) as ('name, 'len))
      .select('c, 'name, 'len).filter('len > 2)
    javaTable = in2.crossApply("func2(c) as (name, len)")
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)

    // test with scalar function
    scalaTable = in1.crossApply(func1('c.substring(2)) as ('s))
      .select('a, 'c, 's)
    javaTable = in2.crossApply("func1(substring(c, 2)) as (s)")
      .select("a, c, s")
    verifyTableEquals(scalaTable, javaTable)
  }

  @Test
  def testSQLWithCrossApply(): Unit = {
    val util = batchTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func1($cor0.c)"),
        term("function", func1.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) f0)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "f0 AS s")
    )

    util.verifySql(sqlQuery, expected)

    // test overloading

    val sqlQuery2 = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(c, '$')) AS T(s)"

    val expected2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func1($cor0.c, '$')"),
        term("function", func1.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) f0)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "f0 AS s")
    )

    util.verifySql(sqlQuery2, expected2)
  }

  @Test
  def testSQLWithOuterApply(): Unit = {
    val util = batchTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON TRUE"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func1($cor0.c)"),
        term("function", func1.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) f0)"),
        term("joinType", "LEFT")
      ),
      term("select", "c", "f0 AS s")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSQLWithCustomType(): Unit = {
    val util = batchTestUtil()
    val func2 = new TableFunc2
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, name, len FROM MyTable, LATERAL TABLE(func2(c)) AS T(name, len)"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func2($cor0.c)"),
        term("function", func2.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, " +
               "VARCHAR(2147483647) f0, INTEGER f1)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "f0 AS name", "f1 AS len")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSQLWithHierarchyType(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new HierarchyTableFunction
    util.addFunction("hierarchy", function)

    val sqlQuery = "SELECT c, T.* FROM MyTable, LATERAL TABLE(hierarchy(c)) AS T(name, adult, len)"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "hierarchy($cor0.c)"),
        term("function", function.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c," +
               " VARCHAR(2147483647) f0, BOOLEAN f1, INTEGER f2)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "f0 AS name", "f1 AS adult", "f2 AS len")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSQLWithPojoType(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new PojoTableFunc
    util.addFunction("pojo", function)

    val sqlQuery = "SELECT c, name, age FROM MyTable, LATERAL TABLE(pojo(c))"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "pojo($cor0.c)"),
        term("function", function.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c," +
               " INTEGER age, VARCHAR(2147483647) name)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "name", "age")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testSQLWithFilter(): Unit = {
    val util = batchTestUtil()
    val func2 = new TableFunc2
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, name, len FROM MyTable, LATERAL TABLE(func2(c)) AS T(name, len) " +
      "WHERE len > 2"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func2($cor0.c)"),
        term("function", func2.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, " +
               "VARCHAR(2147483647) f0, INTEGER f1)"),
        term("joinType", "INNER"),
        term("condition", ">($1, 2)")
      ),
      term("select", "c", "f0 AS name", "f1 AS len")
    )

    util.verifySql(sqlQuery, expected)
  }


  @Test
  def testSQLWithScalarFunction(): Unit = {
    val util = batchTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(SUBSTRING(c, 2))) AS T(s)"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func1(SUBSTRING($cor0.c, 2))"),
        term("function", func1.getClass.getCanonicalName),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) f0)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "f0 AS s")
    )

    util.verifySql(sqlQuery, expected)
  }
}
