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

package org.apache.flink.table.api

import org.apache.flink.api.common.time.Time
import org.junit.Test

class QueryConfigTest {

  @Test(expected = classOf[IllegalArgumentException])
  def testMinBiggerThanMax(): Unit = {
    new StreamQueryConfig().withIdleStateRetentionTime(Time.hours(2), Time.hours(1))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMinEqualMax(): Unit = {
    new StreamQueryConfig().withIdleStateRetentionTime(Time.hours(1), Time.hours(1))
  }

  @Test
  def testMinMaxZero(): Unit = {
    new StreamQueryConfig().withIdleStateRetentionTime(Time.hours(0), Time.hours(0))
  }

  @Test
  def testPass(): Unit = {
    new StreamQueryConfig().withIdleStateRetentionTime(Time.minutes(1), Time.minutes(6))
  }
}
