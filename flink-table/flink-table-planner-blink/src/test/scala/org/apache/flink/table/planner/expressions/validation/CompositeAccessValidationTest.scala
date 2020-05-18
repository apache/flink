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

package org.apache.flink.table.planner.expressions.validation

import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.CompositeTypeTestBase

import org.junit.Test

class CompositeAccessValidationTest extends CompositeTypeTestBase {

  @Test(expected = classOf[ValidationException])
  def testWrongSqlFieldFull(): Unit = {
    testSqlApi("testTable.f5.test", "13")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongSqlField(): Unit = {
    testSqlApi("f5.test", "13")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongIntKeyField(): Unit = {
    testTableApi('f0.get(555), "'fail'", "fail")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongIntKeyField2(): Unit = {
    testTableApi("fail", "f0.get(555)", "fail")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongStringKeyField(): Unit = {
    testTableApi('f0.get("fghj"), "'fail'", "fail")
  }

  @Test(expected = classOf[ValidationException])
  def testWrongStringKeyField2(): Unit = {
    testTableApi("fail", "f0.get('fghj')", "fail")
  }
}


