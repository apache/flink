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
package org.apache.flink.table.api.scala.batch.table

import java.sql.Timestamp

import org.apache.flink.api.java.{DataSet => JDataSet, ExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => ScalaExecutionEnv, _}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{PojoTableFunc, TableFunc2, _}
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.junit.Test
import org.mockito.Mockito._

class UserDefinedTableFunctionTest extends TableTestBase {

  @Test
  def testJavaScalaTableAPIEquality(): Unit = {
    // mock
    val ds = mock(classOf[DataSet[Row]])
    val jDs = mock(classOf[JDataSet[Row]])
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
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

    // test cross join
    val func1 = new TableFunc1
    javaTableEnv.registerFunction("func1", func1)
    var scalaTable = in1.join(func1('c) as 's).select('c, 's)
    var javaTable = in2.join("func1(c).as(s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test left outer join
    scalaTable = in1.leftOuterJoin(func1('c) as 's).select('c, 's)
    javaTable = in2.leftOuterJoin("as(func1(c), s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test overloading
    scalaTable = in1.join(func1('c, "$") as 's).select('c, 's)
    javaTable = in2.join("func1(c, '$') as (s)").select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    javaTableEnv.registerFunction("func2", func2)
    scalaTable = in1.join(func2('c) as ('name, 'len)).select('c, 'name, 'len)
    javaTable = in2.join("func2(c).as(name, len)").select("c, name, len")
    verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    javaTableEnv.registerFunction("hierarchy", hierarchy)
    scalaTable = in1.join(hierarchy('c) as ('name, 'adult, 'len))
      .select('c, 'name, 'len, 'adult)
    javaTable = in2.join("AS(hierarchy(c), name, adult, len)")
      .select("c, name, len, adult")
    verifyTableEquals(scalaTable, javaTable)

    // test pojo type
    val pojo = new PojoTableFunc
    javaTableEnv.registerFunction("pojo", pojo)
    scalaTable = in1.join(pojo('c))
      .select('c, 'name, 'age)
    javaTable = in2.join("pojo(c)")
      .select("c, name, age")
    verifyTableEquals(scalaTable, javaTable)

    // test with filter
    scalaTable = in1.join(func2('c) as ('name, 'len))
      .select('c, 'name, 'len).filter('len > 2)
    javaTable = in2.join("func2(c) as (name, len)")
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)

    // test with scalar function
    scalaTable = in1.join(func1('c.substring(2)) as 's)
      .select('a, 'c, 's)
    javaTable = in2.join("func1(substring(c, 2)) as (s)")
      .select("a, c, s")
    verifyTableEquals(scalaTable, javaTable)
  }

  @Test
  def testCrossJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result1 = table.join(function('c) as 's).select('c, 's)

    val expected1 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("function", function),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result1, expected1)

    // test overloading

    val result2 = table.join(function('c, "$") as 's).select('c, 's)

    val expected2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2, '$$')"),
        term("function", function),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result2, expected2)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = util.addFunction("func1", new TableFunc1)

    val result = table.leftOuterJoin(function('c) as 's).select('c, 's)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("function", function),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "LEFT")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testDynamicSchema(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn = util.addFunction("funcDyn", new DynamicSchema)
    val result = table
      .join(funcDyn('c, 1) as 'name)
      .select('c, 'name)
    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn.functionIdentifier}($$2, 1)"),
        term("function", funcDyn),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) name)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "name")
    )
    util.verifyTable(result, expected)

    val result1 = table
      .join(funcDyn('c, 2) as ('name, 'len0))
      .select('c, 'name, 'len0)
    val expected1 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn.functionIdentifier}($$2, 2)"),
        term("function", funcDyn),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) name, " +
            "INTEGER len0)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "name", "len0")
    )
    util.verifyTable(result1, expected1)

    val result2 = table
      .join(funcDyn('c, 3) as ('name, 'len0, 'len1))
      .select('c, 'name, 'len0, 'len1)
    val expected2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn.functionIdentifier}($$2, 3)"),
        term("function", funcDyn),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) name, " +
            "INTEGER len0, INTEGER len1)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "name", "len0", "len1")
    )
    util.verifyTable(result2, expected2)

    val result3 = table
      .join(funcDyn('c, 3) as ('name, 'len0, 'len1))
      .select('c, 'name, 'len0, 'len1)
      .join(funcDyn('c, 2) as ('name1, 'len10))
      .select('c, 'name, 'len0, 'len1, 'name1, 'len10)
    val expected3 = unaryNode(
      "DataSetCorrelate",
      unaryNode(
        "DataSetCalc",
        unaryNode(
          "DataSetCorrelate",
          batchTableNode(0),
          term("invocation", s"${funcDyn.functionIdentifier}($$2, 3)"),
          term("function", funcDyn),
          term("rowType",
            "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) name, " +
              "INTEGER len0, INTEGER len1)"),
          term("joinType", "INNER")
        ),
        term("select", "c", "name", "len0", "len1")
      ),
      term("invocation", s"${funcDyn.functionIdentifier}($$0, 2)"),
      term("function", funcDyn),
      term("rowType",
        "RecordType(VARCHAR(2147483647) c, VARCHAR(2147483647) name, " +
          "INTEGER len0, INTEGER len1, VARCHAR(2147483647) name1, INTEGER len10)"),
      term("joinType", "INNER")
    )
    util.verifyTable(result3, expected3)

    val funcDyn1 = new DynamicSchema1
    val result4 = table.join(funcDyn1("string") as 'col)
      .select('col)
    val expected4 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn1.functionIdentifier}('string')"),
        term("function", funcDyn1),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) col)"),
        term("joinType", "INNER")
      ),
      term("select", "col")
    )
    util.verifyTable(result4, expected4)

    val result5 = table.join(funcDyn1("int") as 'col)
      .select('col)
    val expected5 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn1.functionIdentifier}('int')"),
        term("function", funcDyn1),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, INTEGER col)"),
        term("joinType", "INNER")
      ),
      term("select", "col")
    )
    util.verifyTable(result5, expected5)

    val result6 = table.join(funcDyn1("double") as 'col)
      .select('col)
    val expected6 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn1.functionIdentifier}('double')"),
        term("function", funcDyn1),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, DOUBLE col)"),
        term("joinType", "INNER")
      ),
      term("select", "col")
    )
    util.verifyTable(result6, expected6)

    val result7 = table.join(funcDyn1("boolean") as 'col)
      .select('col)
    val expected7 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn1.functionIdentifier}('boolean')"),
        term("function", funcDyn1),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, BOOLEAN col)"),
        term("joinType", "INNER")
      ),
      term("select", "col")
    )
    util.verifyTable(result7, expected7)

    val result8 = table.join(funcDyn1("timestamp") as 'col)
      .select('col)
    val expected8 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn1.functionIdentifier}('timestamp')"),
        term("function", funcDyn1),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, TIMESTAMP(3) col)"),
        term("joinType", "INNER")
      ),
      term("select", "col")
    )
    util.verifyTable(result8, expected8)
  }

  @Test
  def testDynamicSchemaWithExpressionParser(): Unit = {
    val util = batchTestUtil()
    val in = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyna0 = util.addFunction("funcDyna0", new DynamicSchema0)
    val result = in.join("funcDyna0(c, 'string,int,int') as (name, len0, len1)")
      .join("funcDyna0(c, 'string,int') as (name1, len10)")
      .select("c,name,len0,len1,name1,len10")
    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        unaryNode(
          "DataSetCorrelate",
          batchTableNode(0),
          term("invocation", s"${funcDyna0.functionIdentifier}($$2, 'string,int,int')"),
          term("function", funcDyna0),
          term("rowType",
            "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) name," +
              " INTEGER len0, INTEGER len1)"),
          term("joinType", "INNER")
        ),
        term("invocation", s"${funcDyna0.functionIdentifier}($$2, 'string,int')"),
        term("function", funcDyna0),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, " +
            "VARCHAR(2147483647) name, INTEGER len0, INTEGER len1, " +
            "VARCHAR(2147483647) name1, INTEGER len10)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "name", "len0", "len1", "name1", "len10")
    )
    util.verifyTable(result, expected)
  }

  @Test
  def testDynamicSchemaWithRexNodes: Unit = {
    val util = batchTestUtil()
    val in = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val funcDyn = util.addFunction("funcDyn", new DynamicSchemaWithRexNodes)
    val result = in
      .join(funcDyn('c, 1, 2, 3, 4.0, 5.0, 6.0, true, new Timestamp(888))
        as ('name, 'c1, 'c2, 'c3, 'c4, 'c5, 'c6, 'c7, 'c8))
      .select('name, 'c1, 'c2, 'c3, 'c4, 'c5, 'c6, 'c7, 'c8)
    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${funcDyn.functionIdentifier}($$2, 1, 2, 3, 4.0E0, 5.0E0, 6.0E0, " +
          s"true, 1970-01-01 08:00:00.888)"),
        term("function", funcDyn),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) name, " +
            "INTEGER c1, INTEGER c2, INTEGER c3, DOUBLE c4, DOUBLE c5, DOUBLE c6, BOOLEAN c7, " +
            "TIMESTAMP(3) c8)"),
        term("joinType", "INNER")
      ),
      term("select", "name", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8")
    )
    util.verifyTable(result, expected)
  }
}
