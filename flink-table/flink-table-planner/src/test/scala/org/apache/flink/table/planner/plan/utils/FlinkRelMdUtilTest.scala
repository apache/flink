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
package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rel.metadata.RelMdUtil
import org.junit.{Assert, Test}

class FlinkRelMdUtilTest {

  @Test
  def testNumDistinctValsWithSmallInputs(): Unit = {
    Assert.assertEquals(
      RelMdUtil.numDistinctVals(1e5, 1e4),
      FlinkRelMdUtil.numDistinctVals(1e5, 1e4))

    Assert.assertEquals(
      BigDecimal(0.31606027941427883),
      BigDecimal.valueOf(FlinkRelMdUtil.numDistinctVals(0.5, 0.5)))

    // This case should be removed once CALCITE-4351 is fixed.
    Assert.assertEquals(Double.NaN, RelMdUtil.numDistinctVals(0.5, 0.5))
  }

  @Test
  def testNumDistinctValsWithLargeInputs(): Unit = {
    Assert.assertNotEquals(0.0, FlinkRelMdUtil.numDistinctVals(1e18, 1e10))
    Assert.assertEquals(9.99999993922529e9, FlinkRelMdUtil.numDistinctVals(1e18, 1e10), 1d)
    // this test will fail once CALCITE-4351 is fixed
    // in that case FlinkRelMdUtil#numDistinctVals should be removed
    // see FLINK-19780
    Assert.assertEquals(0.0, RelMdUtil.numDistinctVals(1e18, 1e10))
  }
}
