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

package org.apache.flink.table.typeutils

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.{Types => ScalaTypes}
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.typeutils.TypeCheckUtils.validateStateType
import org.junit.Test

class TypeCheckUtilsTest {

  @Test
  def testValidateStateType(): Unit = {
    validateStateType(Types.STRING)
    validateStateType(Types.LONG)
    validateStateType(Types.SQL_TIMESTAMP)
    validateStateType(Types.ROW(Types.LONG, Types.DECIMAL))
    validateStateType(ScalaTypes.CASE_CLASS[(Long, Int)])
    validateStateType(Types.OBJECT_ARRAY(Types.LONG))
    validateStateType(Types.PRIMITIVE_ARRAY(Types.LONG))
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidType(): Unit = {
    validateStateType(ScalaTypes.NOTHING)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidType2(): Unit = {
    validateStateType(Types.ROW(ScalaTypes.NOTHING))
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidType3(): Unit = {
    validateStateType(Types.OBJECT_ARRAY[Nothing](ScalaTypes.NOTHING))
  }
}
