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

import org.junit.{Assert, Test}

/**
  * Test for [[CodeGenUtils]].
  */
class CodeGenUtilsTest {

  @Test
  def testNewNameSuffixShouldBeANonNegativeLong(): Unit = {
    val newName = CodeGenUtils.newName("AA")
    assertSuffixIsANonNegativeLong(newName)
  }

  @Test
  def testNewNamesSuffixShouldBeANonNegativeLong(): Unit = {
    val newNames = CodeGenUtils.newNames("AA", "BB", "CC")
    newNames.foreach(assertSuffixIsANonNegativeLong)
  }

  private def assertSuffixIsANonNegativeLong(newName: String): Unit = {
    val newNameSuffix = newName.split("\\$")
    Assert.assertEquals(2, newNameSuffix.length)
    val suffix = java.lang.Long.parseLong(newNameSuffix(1))
    Assert.assertTrue(suffix >= 0L)
  }
}
