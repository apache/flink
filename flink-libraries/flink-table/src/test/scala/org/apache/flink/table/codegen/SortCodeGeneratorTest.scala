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
package org.apache.flink.table.codegen

import org.apache.flink.api.common.typeutils.TypeComparator
import org.apache.flink.api.common.typeutils.base._
import org.apache.flink.api.common.{ExecutionMode, InvalidProgramException}
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.codehaus.janino.SimpleCompiler
import org.junit.Test

/**
  * Test for [[SortCodeGenerator]].
  */
class SortCodeGeneratorTest {

  val cl = Thread.currentThread.getContextClassLoader

  @Test
  def test(): Unit = {
    val keys = Array(0, 2)
    val types = Array[InternalType](DataTypes.INT, DataTypes.LONG)
    val comparators = Array[TypeComparator[_ <: Any]](
      new IntComparator(true), new LongComparator(true))
    val generator = new SortCodeGenerator(
      keys, types, comparators, Array(true, true), Array(false, false))

    val computer = generator.generateNormalizedKeyComputer("SortTestComputer")
    SortCodeGeneratorTest.compile(cl, computer.name, computer.code)

    val comparator = generator.generateRecordComparator("SortTestComparator")
    SortCodeGeneratorTest.compile(cl, comparator.name, comparator.code)
  }

  @Test
  def testString(): Unit = {
    val keys = Array(0)
    val types = Array[InternalType](DataTypes.STRING)
    val comparators = Array[TypeComparator[_ <: Any]](new StringComparator(true))
    val generator = new SortCodeGenerator(keys, types, comparators, Array(true), Array(false))

    val computer = generator.generateNormalizedKeyComputer("SortTestStringComputer")
    SortCodeGeneratorTest.compile(cl, computer.name, computer.code)

    val comparator = generator.generateRecordComparator("SortTestStringComparator")
    SortCodeGeneratorTest.compile(cl, comparator.name, comparator.code)
  }

  @Test
  def testIntAndString(): Unit = {
    val keys = Array(0, 1)
    val types = Array[InternalType](DataTypes.INT, DataTypes.STRING)
    val comparators = Array[TypeComparator[_ <: Any]](
      new IntComparator(true), new StringComparator(true))
    val generator = new SortCodeGenerator(
      keys, types, comparators, Array(true, true), Array(false, false))

    val computer = generator.generateNormalizedKeyComputer("SortTestIntStringComputer")
    SortCodeGeneratorTest.compile(cl, computer.name, computer.code)

    val comparator = generator.generateRecordComparator("SortTestIntStringComparator")
    SortCodeGeneratorTest.compile(cl, comparator.name, comparator.code)
  }

  @Test
  def testGeneric(): Unit = {
    val keys = Array(0, 2)
    val types = Array[InternalType](
      DataTypes.INT, DataTypes.createGenericType(classOf[ExecutionMode]))
    val comparators = Array[TypeComparator[_ <: Any]](
      new IntComparator(true), new EnumComparator[ExecutionMode](true))
    val generator = new SortCodeGenerator(
      keys, types, comparators, Array(true, true), Array(false, false))

    val computer = generator.generateNormalizedKeyComputer("SortTestGenericComputer")
    SortCodeGeneratorTest.compile(cl, computer.name, computer.code)

    val comparator = generator.generateRecordComparator("SortTestGenericComputer")
    SortCodeGeneratorTest.compile(cl, comparator.name, comparator.code)
  }
}

object SortCodeGeneratorTest {
  def compile(cl: ClassLoader, name: String, code: String): Class[_] = {
    require(cl != null, "Classloader must not be null.")
    val compiler = new SimpleCompiler()
    compiler.setParentClassLoader(cl)
    try {
      compiler.cook(code)
    } catch {
      case t: Throwable =>
        throw new InvalidProgramException("Table program cannot be compiled. " +
            "This is a bug. Please file an issue.", t)
    }
    compiler.getClassLoader.loadClass(name)
  }
}
