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

import org.apache.flink.table.plan.util.FlinkRelMdUtil

import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

class FlinkRelMdDistinctRowCountTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[RelMdMethodNotImplementedException])
  def testGetDistinctRowCountOnRelNode(): Unit = {
    mq.getDistinctRowCount(testRel, ImmutableBitSet.of(), null)
  }

  @Test
  def testGetDistinctRowCountOnTableScan(): Unit = {

    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), null))
      assertEquals(50.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), null))
      assertEquals(48.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), null))
      assertEquals(20.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2), null))
      assertEquals(7.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(3), null))
      assertEquals(35.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(4), null))
      assertEquals(50.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 3), null))
      assertEquals(40.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 5), null))

      // age = 16
      val condition = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(3), relBuilder.literal(16))
      val selectivity = 0.15
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 1.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(), condition), 1e-6)
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 50.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), condition), 1e-6)
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 48.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(1), condition), 1e-6)
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 20.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(2), condition), 1e-6)
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 7.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(3), condition), 1e-6)
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 35.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(4), condition))
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 50.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 3), condition))
      assertEquals(FlinkRelMdUtil.adaptNdvBasedOnSelectivity(50.0, 40.0, selectivity),
        mq.getDistinctRowCount(scan, ImmutableBitSet.of(2, 5), condition))
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), null))
      assertNull(mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), null))
      // empno = 1
      val condition = relBuilder.push(studentLogicalScan)
        .call(EQUALS, relBuilder.field(0), relBuilder.literal(1))
      assertEquals(1.0, mq.getDistinctRowCount(scan, ImmutableBitSet.of(), condition))
      assertNull(mq.getDistinctRowCount(scan, ImmutableBitSet.of(0), condition))
    }

  }
}
