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

package org.apache.flink.table.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class WindowAggregateTest extends TableTestBase {

  private val util = batchTestUtil()

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val table = util.addTable[(Long, Int, String, Long)]('rowtime, 'a, 'b, 'c)
    val windowedTable = table
        .window(Tumble over 15.minutes on 'rowtime as 'w)
        .groupBy('w)
        .select('c.varPop, 'c.varSamp, 'c.stddevPop, 'c.stddevSamp, 'w.start, 'w.end)

    util.verifyPlan(windowedTable)
  }
}
