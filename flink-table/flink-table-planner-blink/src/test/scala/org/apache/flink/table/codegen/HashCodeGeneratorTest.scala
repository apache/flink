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

import org.apache.flink.table.`type`.{InternalTypes, RowType}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.dataformat.GenericRow

import org.junit.{Assert, Test}

/**
  * Test for [[HashCodeGenerator]].
  */
class HashCodeGeneratorTest {

  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testHash(): Unit = {
    val hashFunc1 = HashCodeGenerator.generateRowHash(
      new CodeGeneratorContext(new TableConfig),
      new RowType(InternalTypes.INT, InternalTypes.LONG, InternalTypes.BINARY),
      "name",
      Array(1, 0)
    ).newInstance(classLoader)

    val hashFunc2 = HashCodeGenerator.generateRowHash(
      new CodeGeneratorContext(new TableConfig),
      new RowType(InternalTypes.INT, InternalTypes.LONG, InternalTypes.BINARY),
      "name",
      Array(1, 2, 0)
    ).newInstance(classLoader)

    val row = GenericRow.of(ji(5), jl(8), Array[Byte](1, 5, 6))
    Assert.assertEquals(637, hashFunc1.hashCode(row))
    Assert.assertEquals(136516167, hashFunc2.hashCode(row))
  }

  def ji(i: Int): Integer = {
    new Integer(i)
  }

  def jl(l: Long): java.lang.Long = {
    new java.lang.Long(l)
  }
}
