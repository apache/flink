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

import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConversions._

class FlinkRelMdSizeTest extends FlinkRelMdHandlerTestBase {

  @Test(expected = classOf[RelMdMethodNotImplementedException])
  def testAverageRowSizeOnRelNode(): Unit = {
    mq.getAverageRowSize(testRel)
  }

  @Test(expected = classOf[RelMdMethodNotImplementedException])
  def testAverageColumnSizeOnRelNode(): Unit = {
    mq.getAverageColumnSizes(testRel)
  }

  @Test
  def testAverageRowSizeOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(32.2, mq.getAverageRowSize(scan))
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(64.0, mq.getAverageRowSize(scan))
    }
  }

  @Test
  def testAverageColumnSizeOnTableScan(): Unit = {
    Array(studentLogicalScan, studentBatchScan, studentStreamScan).foreach { scan =>
      assertEquals(Seq(4.0, 7.2, 8.0, 4.0, 8.0, 1.0),
        mq.getAverageColumnSizes(scan).toList)
    }

    Array(empLogicalScan, empBatchScan, empStreamScan).foreach { scan =>
      assertEquals(Seq(4.0, 12.0, 12.0, 4.0, 12.0, 8.0, 8.0, 4.0),
        mq.getAverageColumnSizes(scan).toList)
    }
  }

}
