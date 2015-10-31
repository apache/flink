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
import org.apache.flink.util.TestLogger
import org.junit.Test
import org.scalatest.junit.JUnitSuiteLike

class EitherTypeInfoTest extends TestLogger with JUnitSuiteLike {

  @Test
  def testEitherTypeEquality(): Unit = {
    val eitherTypeInfo1 = new EitherTypeInfo[Integer, String, Either[Integer, String]](
      classOf[Either[Integer,String]],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )
    val eitherTypeInfo2 = new EitherTypeInfo[Integer, String, Either[Integer, String]](
      classOf[Either[Integer,String]],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )

    assert(eitherTypeInfo1.equals(eitherTypeInfo2))
    assert(eitherTypeInfo1.hashCode() == eitherTypeInfo2.hashCode())
  }

  @Test
  def testEitherTypeInequality(): Unit = {
    val eitherTypeInfo1 = new EitherTypeInfo[Integer, Integer, Either[Integer, Integer]](
      classOf[Either[Integer,Integer]],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO
    )
    val eitherTypeInfo2 = new EitherTypeInfo[Integer, String, Either[Integer, String]](
      classOf[Either[Integer,String]],
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO
    )
    assert(!eitherTypeInfo1.equals(eitherTypeInfo2))
  }
}
