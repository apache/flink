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

import java.util.{Arrays => JArrays, List => JList, Map => JMap}
import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.table.api.{Types, ValidationException}
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Tests for [[LiteralValue]].
  */
class LiteralValueTest extends DescriptorTestBase {

  @Test(expected = classOf[ValidationException])
  def testMissingValue(): Unit = {
    removePropertyAndVerify(descriptors().get(0), LiteralValueValidator.VALUE)
  }

  @Test(expected = classOf[ValidationException])
  def testWrongValue(): Unit = {
    // byte expected
    addPropertyAndVerify(descriptors().get(2), LiteralValueValidator.VALUE, "12.222")
  }

  override def descriptors(): JList[Descriptor] = {
    val bigDecimalDesc = LiteralValue().of(Types.DECIMAL).value(new JBigDecimal(1))
    val booleanDesc = LiteralValue().of(Types.BOOLEAN).value(false)
    val byteDesc = LiteralValue().of(Types.BYTE).value(4.asInstanceOf[Byte])
    val doubleDesc = LiteralValue().of(Types.DOUBLE).value(7.0)
    val floatDesc = LiteralValue().of(Types.FLOAT).value(8.0f)
    val intDesc = LiteralValue().of(Types.INT).value(9)
    val longDesc = LiteralValue().of(Types.LONG).value(10L)
    val shortDesc = LiteralValue().of(Types.SHORT).value(11.asInstanceOf[Short])
    val stringDesc = LiteralValue().of(Types.STRING).value("12")

    // for tests with implicit type see ClassInstanceTest because literal value are not
    // supported in the top level of a hierarchy

    JArrays.asList(
      bigDecimalDesc,
      booleanDesc,
      byteDesc,
      doubleDesc,
      floatDesc,
      intDesc,
      longDesc,
      shortDesc,
      stringDesc)
  }

  override def validator(): DescriptorValidator = {
    new LiteralValueValidator(HierarchyDescriptorValidator.EMPTY_PREFIX)
  }

  override def properties(): JList[JMap[String, String]] = {
    val bigDecimalProps = Map(
      "type" -> "DECIMAL",
      "value" -> "1"
    )
    val booleanDesc = Map(
      "type" -> "BOOLEAN",
      "value" -> "false"
    )
    val byteDesc = Map(
      "type" -> "TINYINT",
      "value" -> "4"
    )

    val doubleDesc = Map(
      "type" -> "DOUBLE",
      "value" -> "7.0"
    )
    val floatDesc = Map(
      "type" -> "FLOAT",
      "value" -> "8.0"
    )
    val intProps = Map(
      "type" -> "INT",
      "value" -> "9"
    )
    val longDesc = Map(
      "type" -> "BIGINT",
      "value" -> "10"
    )
    val shortDesc = Map(
      "type" -> "SMALLINT",
      "value" -> "11"
    )
    val stringDesc = Map(
      "type" -> "VARCHAR",
      "value" -> "12"
    )
    JArrays.asList(
      bigDecimalProps.asJava,
      booleanDesc.asJava,
      byteDesc.asJava,
      doubleDesc.asJava,
      floatDesc.asJava,
      intProps.asJava,
      longDesc.asJava,
      shortDesc.asJava,
      stringDesc.asJava
    )
  }
}
