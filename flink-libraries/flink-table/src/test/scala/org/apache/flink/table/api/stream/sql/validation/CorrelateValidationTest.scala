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

package org.apache.flink.table.api.stream.sql.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.calcite.tools.ValidationException
import org.apache.flink.table.api.TableException
import org.apache.flink.table.util.{StreamTableTestUtil, TableFunc1, TableTestBase}
import org.junit.Test

class CorrelateValidationTest extends TableTestBase {

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */

  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, Int)]("tbl_a", 'c1, 'c2)
  streamUtil.addTable[Int]("tbl_b", 'c1)

  val func1 = new TableFunc1
  streamUtil.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  streamUtil.addFunction("func1", func1)

  @Test(expected = classOf[TableException])
  def testWhereWithExists(): Unit = {
    val sql =
      """
        |SELECT l.c1, l.c2
        |FROM tbl_a l
        |WHERE EXISTS (SELECT 1 FROM tbl_b r WHERE l.c1 = l.c2) OR l.c2 < 2
        | """.stripMargin

    streamUtil.explainSql(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON c = s"
    streamUtil.explainSql(sqlQuery)
  }
}
