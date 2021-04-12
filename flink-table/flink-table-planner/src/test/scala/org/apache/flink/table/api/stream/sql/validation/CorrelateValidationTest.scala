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
import org.apache.flink.table.api._
import org.apache.flink.table.utils.{TableFunc1, TableTestBase}

import org.junit.Test

class CorrelateValidationTest extends TableTestBase{

  /**
    * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the
    * join predicate can only be empty or literal true (the restriction should be removed in
    * FLINK-7865).
    */
  @Test(expected = classOf[ValidationException])
  def testLeftOuterJoinWithPredicates(): Unit = {
    val util = streamTestUtil()
    val func1 = new TableFunc1
    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    util.addFunction("func1", func1)

    val sqlQuery = "SELECT c, s FROM MyTable LEFT JOIN LATERAL TABLE(func1(c)) AS T(s) ON c = s"

    util.verifySql(sqlQuery, "n/a")
  }
}
