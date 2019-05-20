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

package org.apache.flink.table.plan.schema

import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}

import org.junit.Assert.assertEquals
import org.junit.Test

/**
 * Tests for TimeIndicatorRelDataType.
 */
class TimeIndicatorRelDataTypeTest {

  @Test
  def testGenerateTypeString() {
    val typeFactory = new FlinkTypeFactory(new FlinkTypeSystem)
    assertEquals(
      "TIME ATTRIBUTE(PROCTIME) NOT NULL",
      typeFactory.createProctimeIndicatorType().getFullTypeString)
    assertEquals(
      "TIME ATTRIBUTE(ROWTIME) NOT NULL",
      typeFactory.createRowtimeIndicatorType().getFullTypeString)
  }

}
