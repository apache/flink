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
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{BatchTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

class QueryDecorrelationTest extends TableTestBase {

  val util: BatchTableTestUtil = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTable[(Int, String, String, Int, Int)]("emp", 'empno, 'ename, 'job, 'salary, 'deptno)
    util.addTable[(Int, String)]("dept", 'deptno, 'name)
  }

  @Test
  def testCorrelationScalarAggAndFilter(): Unit = {
    val sql = "SELECT e1.empno\n" +
        "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n" +
        "and e1.deptno < 10 and d1.deptno < 15\n" +
        "and e1.salary > (select avg(salary) from emp e2 where e1.empno = e2.empno)"

    val expectedQuery = unaryNode(
      "DataSetCalc",
      binaryNode(
        "DataSetJoin",
        binaryNode(
          "DataSetJoin",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "empno", "ename", "job", "salary", "deptno"),
            term("where", "<(deptno, 10)")
          ),
          unaryNode(
            "DataSetCalc",
            batchTableNode(1),
            term("select", "deptno", "name"),
            term("where", "<(deptno, 15)")
          ),
          term("where", "=(deptno, deptno0)"),
          term("join", "empno", "ename", "job", "salary", "deptno", "deptno0", "name"),
          term("joinType", "InnerJoin")
        ),
        unaryNode(
          "DataSetAggregate",
          unaryNode(
            "DataSetCalc",
            batchTableNode(0),
            term("select", "salary", "empno"),
            term("where", "IS NOT NULL(empno)")
          ),
          term("groupBy", "empno"),
          term("select", "empno", "AVG(salary) AS EXPR$0")
        ),
        term("where", "AND(=(empno, empno0), >(salary, EXPR$0))"),
        term("join", "empno", "ename", "job", "salary", "deptno",
          "deptno0", "name", "empno0", "EXPR$0"),
        term("joinType", "InnerJoin")
      ),
      term("select", "empno")
    )

    util.verifySql(sql, expectedQuery)
  }

  @Test
  def testDecorrelateWithMultiAggregate(): Unit = {
    val sql = "select sum(e1.empno) from emp e1, dept d1 " +
        "where e1.deptno = d1.deptno " +
        "and e1.salary > (" +
        "    select avg(e2.salary) from emp e2 where e2.deptno = d1.deptno" +
        ")"

    val expectedQuery = unaryNode(
      "DataSetAggregate",
      binaryNode(
        "DataSetUnion",
        values(
          "DataSetValues",
          tuples(List(null)),
          term("values", "empno")
        ),
        unaryNode(
          "DataSetCalc",
          binaryNode(
            "DataSetJoin",
            binaryNode(
              "DataSetJoin",
              batchTableNode(0),
              batchTableNode(1),
              term("where", "=(deptno, deptno0)"),
              term("join", "empno", "ename", "job", "salary", "deptno", "deptno0", "name"),
              term("joinType", "InnerJoin")
            ),
            unaryNode(
              "DataSetAggregate",
              unaryNode(
                "DataSetCalc",
                batchTableNode(0),
                term("select", "deptno", "salary"),
                term("where", "IS NOT NULL(deptno)")
              ),
              term("groupBy", "deptno"),
              term("select", "deptno", "AVG(salary) AS EXPR$0")
            ),
            term("where", "AND(=(deptno0, deptno1), >(salary, EXPR$0))"),
            term("join", "empno", "ename", "job", "salary", "deptno", "deptno0",
              "name", "deptno1", "EXPR$0"),
            term("joinType", "InnerJoin")
          ),
          term("select", "empno")
        ),
        term("union", "empno")
      ),
      term("select", "SUM(empno) AS EXPR$0")
    )

    util.verifySql(sql, expectedQuery)
  }
}
