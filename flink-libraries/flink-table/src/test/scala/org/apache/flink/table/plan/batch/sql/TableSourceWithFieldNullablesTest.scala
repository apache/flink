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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.table.util.{TableTestBase, TestTableSourceWithFieldNullables}
import org.junit.{Before, Test}

class TableSourceWithFieldNullablesTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTable("l", new TestTableSourceWithFieldNullables(
      Array("a", "b"),
      Array(INT_TYPE_INFO, LONG_TYPE_INFO),
      Array(false, true)))
    util.addTable("r", new TestTableSourceWithFieldNullables(
      Array("c", "d"),
      Array(INT_TYPE_INFO, LONG_TYPE_INFO),
      Array(false, true)))
  }

  @Test
  def testReduceFilterExpression(): Unit = {
    // remove the filter condition IS NOT NULL(a) because it's always true
    util.verifyPlan("SELECT * FROM l WHERE a IS NOT NULL")
  }

  @Test
  def testReduceJoinExpression(): Unit = {
    // simplify the join condition from [OR(=(a, c), IS NULL(=(a, c)))] to [=(a, c)] because
    // IS NULL(=(a, c))) is always false
    util.verifyPlan("SELECT * FROM l WHERE a NOT IN (SELECT c FROM r)")
  }

}

