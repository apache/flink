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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.{HierarchyTableFunction, PojoTableFunc, TableFunc2}
import org.apache.flink.table.utils._
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class UserDefinedTableFunctionTest extends TableTestBase {

  @Test
  def testCrossJoin(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(c)) AS T(s)"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
  def testLeftOuterJoin(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON TRUE"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
  def testCustomType(): Unit = {
    val util = streamTestUtil()
    val func2 = new TableFunc2
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, name, len FROM MyTable, LATERAL TABLE(func2(c)) AS T(name, len)"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
  def testHierarchyType(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new HierarchyTableFunction
    util.addFunction("hierarchy", function)

    val sqlQuery = "SELECT c, T.* FROM MyTable, LATERAL TABLE(hierarchy(c)) AS T(name, adult, len)"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
  def testPojoType(): Unit = {
    val util = streamTestUtil()
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new PojoTableFunc
    util.addFunction("pojo", function)

    val sqlQuery = "SELECT c, name, age FROM MyTable, LATERAL TABLE(pojo(c))"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
  def testFilter(): Unit = {
    val util = streamTestUtil()
    val func2 = new TableFunc2
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func2", func2)

    val sqlQuery = "SELECT c, name, len FROM MyTable, LATERAL TABLE(func2(c)) AS T(name, len) " +
      "WHERE len > 2"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
  def testScalarFunction(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable, LATERAL TABLE(func1(SUBSTRING(c, 2))) AS T(s)"

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
