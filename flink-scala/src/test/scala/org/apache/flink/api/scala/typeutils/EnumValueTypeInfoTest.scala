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

import org.apache.flink.api.common.typeutils.TypeInformationTestBase
import org.apache.flink.api.scala.typeutils.AlternateEnumeration.AlternateEnumeration
import org.apache.flink.api.scala.typeutils.TestEnumeration.TestEnumeration

/**
  * Test for [[EnumValueTypeInfo]].
  */
class EnumValueTypeInfoTest extends TypeInformationTestBase[EnumValueTypeInfo[_]] {

  override protected def getTestData: Array[EnumValueTypeInfo[_]] = Array(
    new EnumValueTypeInfo[TestEnumeration.type](
      TestEnumeration,
      classOf[TestEnumeration]),
    new EnumValueTypeInfo[AlternateEnumeration.type](
      AlternateEnumeration,
      classOf[AlternateEnumeration])
  )
}

object TestEnumeration extends Enumeration {
  type TestEnumeration = Value
  val ONE = this.Value
}

object AlternateEnumeration extends Enumeration {
  type AlternateEnumeration = Value
  val TWO = this.Value
}
