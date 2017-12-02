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

package org.apache.flink.table.expressions.validation

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.RowTypeTestBase
import org.junit.Test

class RowTypeValidationTest extends RowTypeTestBase {

  @Test(expected = classOf[ValidationException])
  def testEmptyRowType(): Unit = {
    testAllApis("FAIL", "row()", "Row()", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testNullRowType(): Unit = {
    testAllApis("FAIL", "row(null)", "Row(NULL)", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testSqlRowIllegalAccess(): Unit = {
    testAllApis('f5.get("f2"), "f5.get('f2')", "f5.f2", "FAIL")
  }
}
