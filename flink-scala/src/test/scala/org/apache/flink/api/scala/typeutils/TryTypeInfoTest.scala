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
import org.scalatest.junit.JUnitSuiteLike

import scala.util.Try

class TryTypeInfoTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testTryTypeEquality(): Unit = {
    val TryTypeInfo1 = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val TryTypeInfo2 = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)

    assert(TryTypeInfo1.equals(TryTypeInfo2))
    assert(TryTypeInfo1.hashCode == TryTypeInfo2.hashCode)
  }

  @Test
  def testTryTypeInequality(): Unit = {
    val TryTypeInfo1 = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val TryTypeInfo2 = new TryTypeInfo[String, Try[String]](BasicTypeInfo.STRING_TYPE_INFO)

    //noinspection ComparingUnrelatedTypes
    assert(!TryTypeInfo1.equals(TryTypeInfo2))
  }

  @Test
  def testTryTypeInequalityWithDifferentType(): Unit = {
    val TryTypeInfo = new TryTypeInfo[Integer, Try[Integer]](BasicTypeInfo.INT_TYPE_INFO)
    val genericTypeInfo = new GenericTypeInfo[Double](Double.getClass.asInstanceOf[Class[Double]])

    //noinspection ComparingUnrelatedTypes
    assert(!TryTypeInfo.equals(genericTypeInfo))
  }
}
