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

package org.apache.flink.table.planner.`match`

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy._
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.TableException

import org.junit.Test

class PatternTranslatorTest extends PatternTranslatorTestBase {

  @Test
  def simplePattern(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).next("B"))
  }

  @Test
  def testAfterMatch(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   AFTER MATCH SKIP TO NEXT ROW
        |   PATTERN (A B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   AFTER MATCH SKIP TO LAST A
        |   PATTERN (A B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToLast("A").throwExceptionOnMiss()).next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   AFTER MATCH SKIP TO FIRST A
        |   PATTERN (A B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToFirst("A").throwExceptionOnMiss()).next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN (A B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipPastLastEvent()).next("B"))
  }

  @Test
  def testQuantifiers(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A{2,} B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).timesOrMore(2).consecutive().greedy().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A+ B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).oneOrMore().consecutive().greedy().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A{2,6} B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).times(2, 6).consecutive().greedy().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A{2})
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).times(2).consecutive())
  }

  @Test
  def testOptional(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A* B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).oneOrMore().consecutive().greedy().optional().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A? B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).optional().next("B"))
  }

  @Test
  def testReluctant(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A{2,}? B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).timesOrMore(2).consecutive().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A+? B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).oneOrMore().consecutive().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A{2,6}? B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).times(2, 6).consecutive().next("B"))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A*? B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      Pattern.begin("A", skipToNext()).oneOrMore().consecutive().optional().next("B"))
  }

  @Test
  def testControlCharsInPatternName(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    `A"`.f0 AS aid
        |  PATTERN (`A"`? \u006C C)
        |  DEFINE
        |    `A"` as `A"`.f0 = 1
        |) AS T
        |""".stripMargin,
      Pattern.begin("A\"", skipToNext()).optional().next("\u006C").next("C"))
  }

  @Test
  def testWithinClause(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    A.f0 AS aid
        |  PATTERN (A B) WITHIN INTERVAL '10 00:00:00.004' DAY TO SECOND
        |  DEFINE
        |    A as A.f0 = 1
        |) AS T
        |""".stripMargin,
      Pattern.begin("A", skipToNext()).next("B")
        .within(Time.milliseconds(10 * 24 * 60 * 60 * 1000 + 4)))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    A.f0 AS aid
        |  PATTERN (A B) WITHIN INTERVAL '10 00' DAY TO HOUR
        |  DEFINE
        |    A as A.f0 = 1
        |) AS T
        |""".stripMargin,
      Pattern.begin("A", skipToNext()).next("B")
        .within(Time.milliseconds(10 * 24 * 60 * 60 * 1000)))

    verifyPattern(
      """MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    A.f0 AS aid
        |  PATTERN (A B) WITHIN INTERVAL '10' MINUTE
        |  DEFINE
        |    A as A.f0 = 1
        |) AS T
        |""".stripMargin,
      Pattern.begin("A", skipToNext()).next("B")
        .within(Time.milliseconds(10 * 60 * 1000)))
  }

  @Test(expected = classOf[TableException])
  def testWithinClauseWithYearMonthResolution(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |  ORDER BY proctime
        |  MEASURES
        |    A.f0 AS aid
        |  PATTERN (A B) WITHIN INTERVAL '2-10' YEAR TO MONTH
        |  DEFINE
        |    A as A.f0 = 1
        |) AS T
        |""".stripMargin,
      null /* don't care */)
  }

  @Test(expected = classOf[TableException])
  def testReluctantOptionalNotSupported(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN (A?? B)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      null /* don't care */)
  }

  @Test(expected = classOf[TableException])
  def testGroupPatternsAreNotSupported(): Unit = {
    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 as aF0
        |   PATTERN ((A B)+ C)
        |   DEFINE
        |     A as A.f0 = 1
        |)""".stripMargin,
      null /* don't care */)
  }

  @Test
  def testPermutationsAreNotSupported(): Unit = {
    thrown.expectMessage("Currently, CEP doesn't support PERMUTE patterns.")
    thrown.expect(classOf[TableException])

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 AS aF0
        |   PATTERN (PERMUTE(A  C))
        |   DEFINE
        |     A AS A.f0 = 1
        |)""".stripMargin,
      null /* don't care */)
  }

  @Test
  def testExclusionsAreNotSupported(): Unit = {
    thrown.expectMessage("Currently, CEP doesn't support '{-' '-}' patterns.")
    thrown.expect(classOf[TableException])

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 AS aF0
        |   PATTERN (A { - B - }  C)
        |   DEFINE
        |     A AS A.f0 = 1
        |)""".stripMargin,
      null /* don't care */)
  }

  @Test
  def testAlternationsAreNotSupported(): Unit = {
    thrown.expectMessage("Currently, CEP doesn't support branching patterns.")
    thrown.expect(classOf[TableException])

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 AS aF0
        |   PATTERN (( A | B )  C)
        |   DEFINE
        |     A AS A.f0 = 1
        |)""".stripMargin,
      null /* don't care */)
  }

  @Test
  def testPhysicalOffsetsAreNotSupported(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Flink does not support physical offsets within partition.")

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 AS aF0
        |   PATTERN (A)
        |   DEFINE
        |     A AS PREV(A.f0) = 1
        |)""".stripMargin,
      null /* don't care */)
  }

  @Test
  def testPatternVariablesMustBeUnique(): Unit = {
    thrown.expectMessage("Pattern variables must be unique. That might change in the future.")
    thrown.expect(classOf[TableException])

    verifyPattern(
      """MATCH_RECOGNIZE (
        |   ORDER BY proctime
        |   MEASURES
        |     A.f0 AS aF0
        |   PATTERN (A B A)
        |   DEFINE
        |     A AS A.f0 = 1
        |)""".stripMargin,
      null /* don't care */)
  }
}
