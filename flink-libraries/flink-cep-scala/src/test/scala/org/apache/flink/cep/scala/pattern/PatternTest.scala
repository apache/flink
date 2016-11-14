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

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.cep.pattern.{AndFilterFunction, SubtypeFilterFunction, Pattern => JPattern}
import org.junit.Assert._
import org.junit.Test
import org.apache.flink.cep.Event
import org.apache.flink.cep.SubEvent

class PatternTest {

  /**
    * These tests simply check that the pattern construction completes without failure and that the
    * Scala API pattern is synchronous with its wrapped Java API pattern.
    */

  @Test
  def testStrictContiguity: Unit = {
    val pattern = Pattern.begin[Event]("start").next("next").next("end")
    val jPattern = JPattern.begin[Event]("start").next("next").next("end")


    assertTrue(checkCongruentRepresentations(pattern, jPattern))
    assertTrue(checkCongruentRepresentations(wrapPattern(jPattern).get, jPattern))

    assertTrue(checkCongruentRepresentations(pattern, pattern.wrappedPattern))
    val previous = pattern.getPrevious.orNull
    val preprevious = previous.getPrevious.orNull

    assertTrue(pattern.getPrevious.isDefined)
    assertTrue(previous.getPrevious.isDefined)
    assertFalse(preprevious.getPrevious.isDefined)

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "next")
    assertEquals(preprevious.getName, "start")
  }


  @Test
  def testNonStrictContiguity: Unit = {
    val pattern = Pattern.begin[Event]("start").followedBy("next").followedBy("end")
    val jPattern = JPattern.begin[Event]("start").followedBy("next").followedBy("end")

    assertTrue(checkCongruentRepresentations(pattern, jPattern))
    assertTrue(checkCongruentRepresentations(wrapPattern(jPattern).get, jPattern))
    val previous = pattern.getPrevious.orNull
    val preprevious = previous.getPrevious.orNull

    assertTrue(pattern.getPrevious.isDefined)
    assertTrue(previous.getPrevious.isDefined)
    assertFalse(preprevious.getPrevious.isDefined)

    assertTrue(pattern.isInstanceOf[FollowedByPattern[_, _]])
    assertTrue(previous.isInstanceOf[FollowedByPattern[_, _]])

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "next")
    assertEquals(preprevious.getName, "start")
  }

  @Test
  def testStrictContiguityWithCondition: Unit = {
    val pattern = Pattern.begin[Event]("start")
      .next("next")
      .where((value: Event) => value.getName() == "foobar")
      .next("end")
      .where((value: Event) => value.getId() == 42)

    val jPattern = JPattern.begin[Event]("start")
      .next("next")
      .where(new FilterFunction[Event]() {
        @throws[Exception]
        def filter(value: Event): Boolean = {
          return value.getName() == "foobar"
        }
      }).next("end")
      .where(new FilterFunction[Event]() {
        @throws[Exception]
        def filter(value: Event): Boolean = {
          return value.getId() == 42
        }
      })

    assertTrue(checkCongruentRepresentations(pattern, jPattern))
    assertTrue(checkCongruentRepresentations(wrapPattern(jPattern).get, jPattern))

    val previous = pattern.getPrevious.orNull
    val preprevious = previous.getPrevious.orNull

    assertTrue(pattern.getPrevious.isDefined)
    assertTrue(previous.getPrevious.isDefined)
    assertFalse(preprevious.getPrevious.isDefined)

    assertTrue(pattern.getFilterFunction.isDefined)
    assertTrue(previous.getFilterFunction.isDefined)
    assertFalse(preprevious.getFilterFunction.isDefined)

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "next")
    assertEquals(preprevious.getName, "start")
  }

  @Test
  def testPatternWithSubtyping: Unit = {
    val pattern = Pattern.begin[Event]("start")
      .next("subevent")
      .subtype(classOf[SubEvent])
      .followedBy("end")

    val jPattern = JPattern.begin[Event]("start")
      .next("subevent")
      .subtype(classOf[SubEvent])
      .followedBy("end")

    assertTrue(checkCongruentRepresentations(pattern, jPattern))
    assertTrue(checkCongruentRepresentations(wrapPattern(jPattern).get, jPattern))

    val previous = pattern.getPrevious.orNull
    val preprevious = previous.getPrevious.orNull

    assertTrue(pattern.getPrevious.isDefined)
    assertTrue(previous.getPrevious.isDefined)
    assertFalse(preprevious.getPrevious.isDefined)

    assertTrue(previous.getFilterFunction.isDefined)
    assertTrue(previous.getFilterFunction.get.isInstanceOf[SubtypeFilterFunction[_]])

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "subevent")
    assertEquals(preprevious.getName, "start")
  }

  @Test
  def testPatternWithSubtypingAndFilter: Unit = {
    val pattern = Pattern.begin[Event]("start")
      .next("subevent")
      .subtype(classOf[SubEvent])
      .where((value: SubEvent) => false)
      .followedBy("end")

    val jpattern = JPattern.begin[Event]("start")
      .next("subevent")
      .subtype(classOf[SubEvent])
      .where(new FilterFunction[SubEvent]() {
        @throws[Exception]
        def filter(value: SubEvent): Boolean = {
          return false
        }
      }).followedBy("end")

    assertTrue(checkCongruentRepresentations(pattern, jpattern))
    assertTrue(checkCongruentRepresentations(wrapPattern(jpattern).get, jpattern))


    val previous = pattern.getPrevious.orNull
    val preprevious = previous.getPrevious.orNull

    assertTrue(pattern.getPrevious.isDefined)
    assertTrue(previous.getPrevious.isDefined)
    assertFalse(preprevious.getPrevious.isDefined)

    assertTrue(pattern.isInstanceOf[FollowedByPattern[_, _]])
    assertTrue(previous.getFilterFunction.isDefined)

    assertEquals(pattern.getName, "end")
    assertEquals(previous.getName, "subevent")
    assertEquals(preprevious.getName, "start")
  }

  def checkCongruentRepresentations[T, _ <: T](pattern: Pattern[T, _ <: T],
                                               jPattern: JPattern[T, _ <: T]): Boolean = {
    ((pattern == null && jPattern == null)
      || (pattern != null && jPattern != null)
      //check equal pattern names
      && threeWayEquals(
      pattern.getName,
      pattern.wrappedPattern.getName,
      jPattern.getName())
      //check equal time windows
      && threeWayEquals(
      pattern.getWindowTime.orNull,
      pattern.wrappedPattern.getWindowTime,
      jPattern.getWindowTime())
      //check congruent class names / types
      && threeWayEquals(
      pattern.getClass.getSimpleName,
      pattern.wrappedPattern.getClass.getSimpleName,
      jPattern.getClass().getSimpleName())
      //best effort to confirm congruent filter functions
      && compareFilterFunctions(
      pattern.getFilterFunction.orNull,
      jPattern.getFilterFunction())
      //recursively check previous patterns
      && checkCongruentRepresentations(
      pattern.getPrevious.orNull,
      jPattern.getPrevious()))
  }

  def threeWayEquals(a: AnyRef, b: AnyRef, c: AnyRef): Boolean = {
    a == b && b == c
  }

  def compareFilterFunctions(sFilter: FilterFunction[_], jFilter: FilterFunction[_]): Boolean = {
    /**
      * We would like to simply compare the filter functions like this:
      *
      * {{{(pattern.getFilterFunction.orNull == jPattern.getFilterFunction)}}}
      *
      * However, the closure cleaning makes comparing filter functions by reference impossible.
      * Testing for functional equivalence is an undecidable problem. Thus, for do a best effort by
      * simply matching the presence/absence and known classes of filter functions in the patterns.
      */
    (sFilter, jFilter) match {
      //matching types: and-filter; branch and recurse for inner filters
      case (saf: AndFilterFunction[_], jaf: AndFilterFunction[_])
      => (compareFilterFunctions(saf.getLeft(), jaf.getLeft())
        && compareFilterFunctions(saf.getRight(), jaf.getRight()))
      //matching types: subtype-filter
      case (saf: SubtypeFilterFunction[_], jaf: SubtypeFilterFunction[_]) => true
      //mismatch: one-sided and/subtype-filter
      case (_: AndFilterFunction[_] | _: SubtypeFilterFunction[_], _) => false
      case (_, _: AndFilterFunction[_] | _: SubtypeFilterFunction[_]) => false
      //from here we can only check mutual presence or absence of a function
      case (s: FilterFunction[_], j: FilterFunction[_]) => true
      case (null, null) => true
      case _ => false
    }
  }

}
