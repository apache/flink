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
package org.apache.flink.table.planner.codegen

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.ScalarFunction

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.Test

/** Tests for [[CodeGeneratorContext]]. */
class CodeGeneratorContextTest {

  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testAddSameStatelessFunctionInstanceDedup(): Unit = {
    val ctx = new CodeGeneratorContext(new Configuration, classLoader)
    val udf = new StatelessTestFunction()

    val term1 = ctx.addReusableFunction(udf)
    val term2 = ctx.addReusableFunction(udf)

    assertEquals(term1, term2)
    assertEquals(1, ctx.references.size)
  }

  @Test
  def testAddSameStatefulFunctionInstancesDedup(): Unit = {
    val ctx = new CodeGeneratorContext(new Configuration, classLoader)
    val udf1 = new StatefulTestFunction(42)
    val udf2 = new StatefulTestFunction(42)

    val term1 = ctx.addReusableFunction(udf1)
    val term2 = ctx.addReusableFunction(udf2)

    assertEquals(term1, term2)
    assertEquals(1, ctx.references.size)
  }

  @Test
  def testAddDifferentStatelessFunctionInstancesDedup(): Unit = {
    val ctx = new CodeGeneratorContext(new Configuration, classLoader)
    val udf1 = new StatelessTestFunction()
    val udf2 = new StatelessTestFunction()

    val term1 = ctx.addReusableFunction(udf1)
    val term2 = ctx.addReusableFunction(udf2)

    assertEquals(term1, term2)
    assertEquals(1, ctx.references.size)
  }

  @Test
  def testAddDifferentStatefulFunctionInstances(): Unit = {
    val ctx = new CodeGeneratorContext(new Configuration, classLoader)
    val udf1 = new StatefulTestFunction(1)
    val udf2 = new StatefulTestFunction(2)

    val term1 = ctx.addReusableFunction(udf1)
    val term2 = ctx.addReusableFunction(udf2)

    assertNotEquals(term1, term2)
    assertEquals(2, ctx.references.size)
  }
}

@SerialVersionUID(1L)
class StatelessTestFunction extends ScalarFunction {
  def eval(a: Long): Long = a
}

@SerialVersionUID(1L)
class StatefulTestFunction(private val mode: Int) extends ScalarFunction {
  def eval(a: Long): Long = a + mode
}
