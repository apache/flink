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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.util.TestLogger
import org.junit.Test
import org.scalatest.junit.{JUnitSuiteLike, JUnitSuite}

class OptionTypeInfoTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testOptionTypeEquality: Unit = {
    val optionTypeInfo1 = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val optionTypeInfo2 = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)

    assert(optionTypeInfo1.equals(optionTypeInfo2))
    assert(optionTypeInfo1.hashCode == optionTypeInfo2.hashCode)
  }

  @Test
  def testOptionTypeInequality: Unit = {
    val optionTypeInfo1 = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val optionTypeInfo2 = new OptionTypeInfo[String, Option[String]](BasicTypeInfo.STRING_TYPE_INFO)

    assert(!optionTypeInfo1.equals(optionTypeInfo2))
  }

  @Test
  def testOptionTypeInequalityWithDifferentType: Unit = {
    val optionTypeInfo = new OptionTypeInfo[Integer, Option[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val genericTypeInfo = new GenericTypeInfo[Double](Double.getClass.asInstanceOf[Class[Double]])

    assert(!optionTypeInfo.equals(genericTypeInfo))
  }
}

