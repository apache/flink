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

package org.apache.flink.table.plan

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class QueryDecorrelationTest extends TableTestBase {

  @Test
  def testCorrelationScalarAggAndFilter(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, String, String, Int, Int)]("emp", 'empno, 'ename, 'job, 'salary, 'deptno)
    util.addTable[(Int, String)]("dept", 'deptno, 'name)

    val sql = "SELECT e1.empno\n" +
        "FROM emp e1, dept d1 where e1.deptno = d1.deptno\n" +
        "and e1.deptno < 10 and d1.deptno < 15\n" +
        "and e1.salary > (select avg(salary) from emp e2 where e1.empno = e2.empno)"
    util.verifyPlan(sql)
  }

  @Test
  def testDecorrelateWithMultiAggregate(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, String, String, Int, Int)]("emp", 'empno, 'ename, 'job, 'salary, 'deptno)
    util.addTable[(Int, String)]("dept", 'deptno, 'name)

    val sql = "select sum(e1.empno) from emp e1, dept d1 " +
        "where e1.deptno = d1.deptno " +
        "and e1.salary > (" +
        "    select avg(e2.salary) from emp e2 where e2.deptno = d1.deptno" +
        ")"

    util.verifyPlan(sql)
  }
}
