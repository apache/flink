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

package org.apache.flink.table.plan.metadata

import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.junit.Assert._
import org.junit.Test

class FlinkRelMdSelectivityTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[RelMdMethodNotImplementedException])
  def testGetSelectivityOnRelNode(): Unit = {
    mq.getSelectivity(testRel, null)
  }

  @Test
  def testGetSelectivityOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getSelectivity(scan, null))
      // age = 16
      val condition1 = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      assertEquals(0.15, mq.getSelectivity(scan, condition1))

      // age = 16 AND score >= 4.0
      val condition2 = relBuilder.call(AND,
        relBuilder.call(EQUALS, relBuilder.field(3), relBuilder.literal(16)),
        relBuilder.call(GREATER_THAN_OR_EQUAL, relBuilder.field(4), relBuilder.literal(4.0)))
      assertEquals(0.075, mq.getSelectivity(scan, condition2))
    }
  }
}
