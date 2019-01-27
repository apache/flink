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
package org.apache.flink.table.api

import org.apache.flink.table.sources.IndexKey
import org.apache.flink.table.util.TableTestBase

import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

class IndexKeyTest extends TableTestBase {

  @Test
  def testIndexKey(): Unit = {
    val indexKey = IndexKey.of(false, 1, 3, 5)
    val columns1 = Array(1, 3, 4, 5)
    val columns2 = Array(0, 1, 3)
    val columns3 = Array(3, 5)
    val columns4 = Array(1, 3, 5)

    assertFalse(indexKey.isUnique)
    assertEquals("{1, 3, 5}", indexKey.toString)
    assertTrue(indexKey.isIndex(columns1))
    assertFalse(indexKey.isIndex(columns2))
    assertFalse(indexKey.isIndex(columns3))
    assertTrue(indexKey.isIndex(columns4))

    val indexKey2 = IndexKey.of(false, 0)
    val keys = Array(indexKey, indexKey2)
    assertFalse(indexKey2.isUnique)
    assertEquals("{1, 3, 5}", indexKey.toString)
    assertTrue(keys.exists(_.isIndex(columns2)))
    assertFalse(keys.exists(_.isIndex(columns3)))
  }

  @Test
  def testUniqueIndexKey(): Unit = {
    val indexKey = IndexKey.of(true, 1, 3, 5)
    val columns1 = Array(1, 3, 4, 5)
    val columns2 = Array(0, 1, 3)
    val columns3 = Array(3, 5)
    val columns4 = Array(1, 3, 5)

    assertTrue(indexKey.isUnique)
    assertEquals("{1, 3, 5}", indexKey.toString)
    assertTrue(indexKey.isIndex(columns1))
    assertFalse(indexKey.isIndex(columns2))
    assertFalse(indexKey.isIndex(columns3))
    assertTrue(indexKey.isIndex(columns4))

    val indexKey2 = IndexKey.of(true, 0)
    val keys = Array(indexKey, indexKey2)
    assertTrue(indexKey2.isUnique)
    assertTrue(keys.exists(_.isIndex(columns2)))
    assertFalse(keys.exists(_.isIndex(columns3)))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidIndexKey(): Unit = {
    IndexKey.of(false)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testInvalidUniqueIndexKey(): Unit = {
    IndexKey.of(true)
  }

}
