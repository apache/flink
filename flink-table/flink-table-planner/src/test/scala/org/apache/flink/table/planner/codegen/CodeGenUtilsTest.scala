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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

class CodeGenUtilsTest {
  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testNewName(): Unit = {
    // Use name counter in CodeGenUtils.
    assertEquals("name$i0", CodeGenUtils.newName(null, "name"))
    assertEquals("name$i1", CodeGenUtils.newName(null, "name"))
    assertEquals(ArrayBuffer("name$i2", "id$i2"), CodeGenUtils.newNames(null, "name", "id"))

    val context1 = new CodeGeneratorContext(new Configuration, classLoader)
    // Use name counter in context1, the index will start from zero.
    assertEquals("name$0", CodeGenUtils.newName(context1, "name"))
    assertEquals("name$1", CodeGenUtils.newName(context1, "name"))
    assertEquals("name$2", CodeGenUtils.newName(context1, "name"))
    assertEquals(ArrayBuffer("name$3", "id$3"), CodeGenUtils.newNames(context1, "name", "id"))
    assertEquals(ArrayBuffer("name$4", "id$4"), CodeGenUtils.newNames(context1, "name", "id"))

    val context2 = new CodeGeneratorContext(new Configuration, classLoader)
    // Use name counter in context2, the index will start from zero.
    assertEquals("name$0", CodeGenUtils.newName(context2, "name"))
    assertEquals("name$1", CodeGenUtils.newName(context2, "name"))
    assertEquals("name$2", CodeGenUtils.newName(context2, "name"))
    assertEquals(ArrayBuffer("name$3", "id$3"), CodeGenUtils.newNames(context2, "name", "id"))
    assertEquals(ArrayBuffer("name$4", "id$4"), CodeGenUtils.newNames(context2, "name", "id"))

    val context3 = new CodeGeneratorContext(new Configuration, classLoader, context1)
    val context4 = new CodeGeneratorContext(new Configuration, classLoader, context3)
    // Use context4 to get a new name, the ancestor of context4(which is context1) will be used.
    assertEquals("name$5", CodeGenUtils.newName(context4, "name"))
    assertEquals("name$6", CodeGenUtils.newName(context4, "name"))
    assertEquals(ArrayBuffer("name$7", "id$7"), CodeGenUtils.newNames(context4, "name", "id"))
    assertEquals(ArrayBuffer("name$8", "id$8"), CodeGenUtils.newNames(context4, "name", "id"))
  }
}
