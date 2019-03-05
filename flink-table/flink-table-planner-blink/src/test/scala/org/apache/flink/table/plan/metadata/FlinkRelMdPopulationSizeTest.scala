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

import org.apache.calcite.util.ImmutableBitSet
import org.junit.Assert._
import org.junit.Test

class FlinkRelMdPopulationSizeTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[RelMdMethodNotImplementedException])
  def testGetPopulationSizeOnRelNode(): Unit = {
    mq.getPopulationSize(testRel, ImmutableBitSet.of())
  }

  @Test
  def testGetPopulationSizeOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getPopulationSize(scan, ImmutableBitSet.of()))
      assertEquals(50.0, mq.getPopulationSize(scan, ImmutableBitSet.of(0)))
      assertEquals(48.0, mq.getPopulationSize(scan, ImmutableBitSet.of(1)))
      assertEquals(20.0, mq.getPopulationSize(scan, ImmutableBitSet.of(2)))
      assertEquals(7.0, mq.getPopulationSize(scan, ImmutableBitSet.of(3)))
      assertEquals(35.0, mq.getPopulationSize(scan, ImmutableBitSet.of(4)))
      assertEquals(50.0, mq.getPopulationSize(scan, ImmutableBitSet.of(2, 3)))
      assertEquals(40.0, mq.getPopulationSize(scan, ImmutableBitSet.of(2, 5)))
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(1.0, mq.getPopulationSize(scan, ImmutableBitSet.of()))
      assertNull(mq.getPopulationSize(scan, ImmutableBitSet.of(0)))
    }
  }

}
