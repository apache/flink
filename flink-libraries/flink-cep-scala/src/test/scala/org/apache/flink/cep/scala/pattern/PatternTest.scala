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
package org.apache.flink.cep.scala.pattern

import org.apache.flink.cep.pattern.SubtypeFilterFunction
import org.apache.flink.cep.scala._
import org.junit.Assert._
import org.junit.Test

class PatternTest {

  /**
    * These tests simply test that the pattern construction completes without failure
    */

  @Test
  def testStrictContiguity: Unit = {
    val pattern = Pattern.begin("start").next("next").next("end")
    val previous = pattern.getPrevious
    val preprevious = previous.getPrevious

    assertNotNull(previous)
    assertNotNull(preprevious)
    assertNull(preprevious.getPrevious)

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "next")
    assertEquals(preprevious.getName, "start")
  }


  @Test
  def testNonStrictContiguity: Unit = {
    val pattern  = Pattern.begin("start").followedBy("next").followedBy("end")
    val previous = pattern.getPrevious
    val preprevious = previous.getPrevious

    assertNotNull(pattern.getPrevious)
    assertNotNull(previous.getPrevious)
    assertNull(preprevious.getPrevious)

    assertTrue(pattern.isInstanceOf[FollowedByPattern[_, _]])
    assertTrue(previous.isInstanceOf[FollowedByPattern[_, _]])

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "next")
    assertEquals(preprevious.getName, "start")
  }

  @Test
  def testStrictContiguityWithCondition: Unit = {
    val pattern = Pattern.begin("start")
      .next("next")
      .where((value : Event) => value.name == "foobar")
      .next("end")
      .where((value : Event) => value.id == 42)

    val previous = pattern.getPrevious
    val preprevious = previous.getPrevious

    assertNotNull(pattern.getPrevious)
    assertNotNull(previous.getPrevious)
    assertNull(preprevious.getPrevious)

    assertNotNull(pattern.getFilterFunction)
    assertNotNull(previous.getFilterFunction)
    assertNull(preprevious.getFilterFunction)

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "next")
    assertEquals(preprevious.getName, "start")
  }

  @Test
  def testPatternWithSubtyping: Unit = {
    val pattern = Pattern.begin("start")
      .next("subevent")
      .subtype(classOf[SubEvent])
      .followedBy("end")

    val previous = pattern.getPrevious
    val preprevious = previous.getPrevious

    assertNotNull(pattern.getPrevious)
    assertNotNull(previous.getPrevious)
    assertNull(preprevious.getPrevious)

    assertNotNull(previous.getFilterFunction)
    assertTrue(previous.getFilterFunction.isInstanceOf[SubtypeFilterFunction[_]])

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "subevent")
    assertEquals(preprevious.getName, "start")
  }

  @Test
  def testPatternWithSubtypingAndFilter: Unit = {
    val pattern = Pattern.begin("start")
      .next("subevent")
      .subtype(classOf[SubEvent]).where((value: SubEvent) => false).followedBy("end")

    val previous = pattern.getPrevious
    val preprevious = previous.getPrevious

    assertNotNull(pattern.getPrevious)
    assertNotNull(previous.getPrevious)
    assertNull(preprevious.getPrevious)

    assertTrue(pattern.isInstanceOf[FollowedByPattern[_, _]])
    assertNotNull(previous.getFilterFunction)

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "subevent")
    assertEquals(preprevious.getName, "start")
  }

}
