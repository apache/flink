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
import org.apache.flink.table.api.config.TableConfigOptions

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

class CodeGenUtilsTest {
  private val classLoader = Thread.currentThread().getContextClassLoader

  @Test
  def testNewName(): Unit = {
    val configuration1 = new Configuration()
    configuration1.setBoolean(TableConfigOptions.INDEPENDENT_NAME_COUNTER_ENABLED, true)

    // Use name counter in CodeGenUtils.
    assertEquals("name$0", CodeGenUtils.newName(null, "name"))
    assertEquals("name$1", CodeGenUtils.newName(null, "name"))
    assertEquals(ArrayBuffer("name$2", "id$2"), CodeGenUtils.newNames(null, "name", "id"))

    val context1 = new CodeGeneratorContext(configuration1, classLoader)
    // Use name counter in context1, the index will start from zero.
    assertEquals("name$0", CodeGenUtils.newName(context1, "name"))
    assertEquals("name$1", CodeGenUtils.newName(context1, "name"))
    assertEquals("name$2", CodeGenUtils.newName(context1, "name"))
    assertEquals(ArrayBuffer("name$3", "id$3"), CodeGenUtils.newNames(context1, "name", "id"))
    assertEquals(ArrayBuffer("name$4", "id$4"), CodeGenUtils.newNames(context1, "name", "id"))

    // Use name counter in context2, the index will start from zero.
    val context2 = new CodeGeneratorContext(configuration1, classLoader)
    assertEquals("name$0", CodeGenUtils.newName(context2, "name"))
    assertEquals("name$1", CodeGenUtils.newName(context2, "name"))
    assertEquals("name$2", CodeGenUtils.newName(context2, "name"))
    assertEquals(ArrayBuffer("name$3", "id$3"), CodeGenUtils.newNames(context2, "name", "id"))
    assertEquals(ArrayBuffer("name$4", "id$4"), CodeGenUtils.newNames(context2, "name", "id"))

    val configuration2 = new Configuration()
    configuration2.setBoolean(TableConfigOptions.INDEPENDENT_NAME_COUNTER_ENABLED, false)

    // When we disable independent name counter, the one in CodeGenUtils will be used.
    val context3 = new CodeGeneratorContext(configuration2, classLoader)
    assertEquals("name$3", CodeGenUtils.newName(context3, "name"))
    assertEquals("name$4", CodeGenUtils.newName(context3, "name"))
    assertEquals("name$5", CodeGenUtils.newName(context3, "name"))
    assertEquals(ArrayBuffer("name$6", "id$6"), CodeGenUtils.newNames(context3, "name", "id"))
  }
}
