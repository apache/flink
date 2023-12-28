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
  }
}
