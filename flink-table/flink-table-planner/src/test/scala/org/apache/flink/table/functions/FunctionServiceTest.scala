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

package org.apache.flink.table.functions

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.descriptors.{ClassInstance, FunctionDescriptor}
import org.apache.flink.table.functions.FunctionServiceTest.{MultiArgClass, NoArgClass, OneArgClass, PrivateClass}
import org.junit.Assert.{assertEquals, assertFalse}
import org.junit.Test

/**
  * Tests for [[FunctionService]].
  */
class FunctionServiceTest {

  @Test(expected = classOf[ValidationException])
  def testWrongArgsFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(new ClassInstance()
        .of(classOf[NoArgClass].getName)
        .parameterString("12"))

    FunctionService.createFunction(descriptor)
  }

  @Test(expected = classOf[ValidationException])
  def testPrivateFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(new ClassInstance().of(classOf[PrivateClass].getName))

    FunctionService.createFunction(descriptor)
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidClassFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(new ClassInstance().of("this.class.does.not.exist"))

    FunctionService.createFunction(descriptor)
  }

  @Test(expected = classOf[ValidationException])
  def testNotFunctionClassFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(new ClassInstance()
        .of(classOf[java.lang.String].getName)
        .parameterString("hello"))

    FunctionService.createFunction(descriptor)
  }

  @Test
  def testNoArgFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(new ClassInstance().of(classOf[NoArgClass].getName))

    assertEquals(classOf[NoArgClass], FunctionService.createFunction(descriptor).getClass)
  }

  @Test
  def testOneArgFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(
        new ClassInstance()
          .of(classOf[OneArgClass].getName)
          .parameterString("false"))

    val actualFunction = FunctionService.createFunction(descriptor)

    assertEquals(classOf[OneArgClass], actualFunction.getClass)
    assertFalse(actualFunction.asInstanceOf[OneArgClass].field)
  }

  @Test
  def testMultiArgFunctionCreation(): Unit = {
    val descriptor = new FunctionDescriptor()
      .fromClass(
        new ClassInstance()
          .of(classOf[MultiArgClass].getName)
          .parameter(new java.math.BigDecimal("12.0003"))
          .parameter(new ClassInstance()
            .of(classOf[java.math.BigInteger].getName)
            .parameter("111111111111111111111111111111111")))

    val actualFunction = FunctionService.createFunction(descriptor)

    assertEquals(classOf[MultiArgClass], actualFunction.getClass)
    assertEquals(
      new java.math.BigDecimal("12.0003"),
      actualFunction.asInstanceOf[MultiArgClass].field1)
    assertEquals(
      new java.math.BigInteger("111111111111111111111111111111111"),
      actualFunction.asInstanceOf[MultiArgClass].field2)
  }
}

object FunctionServiceTest {

  class NoArgClass
    extends ScalarFunction

  class OneArgClass(val field: java.lang.Boolean)
    extends ScalarFunction

  class MultiArgClass(val field1: java.math.BigDecimal, val field2: java.math.BigInteger)
    extends ScalarFunction

  class PrivateClass private() extends ScalarFunction
}
