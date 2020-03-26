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

package org.apache.flink.table.api.batch

import org.apache.flink.table.api.config.ParserConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData.{data3, nullablesOfData3, type3}
import org.junit.{Before, Test}

/**
 * Suite tests for validating identifiers case-sensitive in SQL.
 */
class ParserITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("table1", data3, type3, "a, B, c", nullablesOfData3)
    tEnv.getConfig.getConfiguration.setBoolean(
      ParserConfigOptions.TABLE_PARSER_CASE_SENSITIVE_ENABLED, false)
  }

  @Test
  def testNoneCaseSensitiveColumn(): Unit = {
    checkResult(
      "SELECT A,B,C FROM table",
      data3)
  }
}
