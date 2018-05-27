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

package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.table.api.ValidationException
import org.junit.Test

import scala.collection.JavaConverters._

class PrimitiveTypeTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testMissingType(): Unit = {
    removePropertyAndVerify(descriptors().get(0), PrimitiveTypeValidator.PRIMITIVE_TYPE)
  }

  @Test(expected = classOf[ValidationException])
  def testMissingValue(): Unit = {
    removePropertyAndVerify(descriptors().get(0), PrimitiveTypeValidator.PRIMITIVE_VALUE)
  }

  override def descriptors(): util.List[Descriptor] = {
    val intDesc = PrimitiveTypeDescriptor().setType(BasicTypeInfo.INT_TYPE_INFO).setValue(1)
    val longDesc = PrimitiveTypeDescriptor().setType(BasicTypeInfo.LONG_TYPE_INFO).setValue(2L)
    val doubleDesc = PrimitiveTypeDescriptor().setType(BasicTypeInfo.DOUBLE_TYPE_INFO).setValue(3.0)
    val stringDesc = PrimitiveTypeDescriptor().setType(BasicTypeInfo.STRING_TYPE_INFO).setValue("4")
    val booleanDesc =
      PrimitiveTypeDescriptor().setType(BasicTypeInfo.BOOLEAN_TYPE_INFO).setValue(false)

    util.Arrays.asList(intDesc, longDesc, doubleDesc, stringDesc, booleanDesc)
  }

  override def validator(): DescriptorValidator = {
    new PrimitiveTypeValidator()
  }

  override def properties(): util.List[util.Map[String, String]] = {
    val intProps = Map(
      "type" -> "INT",
      "value" -> "1"
    )
    val longDesc = Map(
      "type" -> "BIGINT",
      "value" -> "2"
    )
    val doubleDesc = Map(
      "type" -> "DOUBLE",
      "value" -> "3.0"
    )
    val stringDesc = Map(
      "type" -> "VARCHAR",
      "value" -> "4"
    )
    val booleanDesc = Map(
      "type" -> "BOOLEAN",
      "value" -> "false"
    )
    util.Arrays.asList(
      intProps.asJava,
      longDesc.asJava,
      doubleDesc.asJava,
      stringDesc.asJava,
      booleanDesc.asJava)
  }
}
