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
package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.table.planner.runtime.batch.sql.join.JoinType.{BroadcastHashJoin, HashJoin, JoinType, NestedLoopJoin, SortMergeJoin}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class OuterJoinITCase extends BatchTestBase {

  @Parameter var expectedJoinType: JoinType = _

  private lazy val leftT = Seq(
    row(1, 2.0),
    row(2, 100.0),
    row(2, 1.0), // This row is duplicated to ensure that we will have multiple buffered matches
    row(2, 1.0),
    row(3, 3.0),
    row(5, 1.0),
    row(6, 6.0),
    row(null, null)
  )

  private lazy val rightT = Seq(
    row(0, 0.0),
    row(2, 3.0), // This row is duplicated to ensure that we will have multiple buffered matches
    row(2, -1.0),
    row(2, -1.0),
    row(2, 3.0),
    row(3, 2.0),
    row(4, 1.0),
    row(5, 3.0),
    row(7, 7.0),
    row(null, null)
  )

  @BeforeEach
  override def before(): Unit = {
    super.before()
    registerCollection("uppercasedata", upperCaseData, INT_STRING, "N, L", nullablesOfUpperCaseData)
    registerCollection("lowercasedata", lowerCaseData, INT_STRING, "n, l", nullablesOfLowerCaseData)
    registerCollection("allnulls", allNulls, INT_ONLY, "a", nullablesOfAllNulls)
    registerCollection("leftT", leftT, INT_DOUBLE, "a, b")
    registerCollection("rightT", rightT, INT_DOUBLE, "c, d")
    JoinITCaseHelper.disableOtherJoinOpForJoin(tEnv, expectedJoinType)
  }

  @TestTemplate
  def testLeftOuter(): Unit = {
    checkResult(
      "SELECT * FROM leftT LEFT JOIN rightT ON a = c and b < d",
      Seq(
        row(null, null, null, null),
        row(1, 2.0, null, null),
        row(2, 100.0, null, null),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(3, 3.0, null, null),
        row(5, 1.0, 5, 3.0),
        row(6, 6.0, null, null)
      )
    )
  }

  @TestTemplate
  def testRightOuter(): Unit = {
    checkResult(
      "SELECT * FROM leftT RIGHT JOIN rightT ON a = c and b < d",
      Seq(
        row(null, null, null, null),
        row(null, null, 0, 0.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(null, null, 2, -1.0),
        row(null, null, 2, -1.0),
        row(2, 1.0, 2, 3.0),
        row(2, 1.0, 2, 3.0),
        row(null, null, 3, 2.0),
        row(null, null, 4, 1.0),
        row(5, 1.0, 5, 3.0),
        row(null, null, 7, 7.0)
      )
    )
  }

  @TestTemplate
  def testFullOuter(): Unit = {
    if (expectedJoinType != NestedLoopJoin && expectedJoinType != BroadcastHashJoin) {
      checkResult(
        "SELECT * FROM leftT FULL JOIN rightT ON a = c and b < d",
        Seq(
          row(1, 2.0, null, null),
          row(null, null, 2, -1.0),
          row(null, null, 2, -1.0),
          row(2, 100.0, null, null),
          row(2, 1.0, 2, 3.0),
          row(2, 1.0, 2, 3.0),
          row(2, 1.0, 2, 3.0),
          row(2, 1.0, 2, 3.0),
          row(3, 3.0, null, null),
          row(5, 1.0, 5, 3.0),
          row(6, 6.0, null, null),
          row(null, null, 0, 0.0),
          row(null, null, 3, 2.0),
          row(null, null, 4, 1.0),
          row(null, null, 7, 7.0),
          row(null, null, null, null),
          row(null, null, null, null)
        )
      )
    }
  }

  @TestTemplate
  def testLeftEmptyOuter(): Unit = {
    checkResult(
      "SELECT * FROM (SELECT * FROM leftT WHERE FALSE) " +
        "LEFT JOIN (SELECT * FROM rightT WHERE FALSE) ON a = c and b < d",
      Seq())
  }

  @TestTemplate
  def testRightEmptyOuter(): Unit = {
    checkResult(
      "SELECT * FROM (SELECT * FROM leftT WHERE FALSE) " +
        "RIGHT JOIN (SELECT * FROM rightT WHERE FALSE) ON a = c and b < d",
      Seq())
  }

  @TestTemplate
  def testFullEmptyOuter(): Unit = {
    if (expectedJoinType != NestedLoopJoin && expectedJoinType != BroadcastHashJoin) {
      checkResult(
        "SELECT * FROM (SELECT * FROM leftT WHERE FALSE) " +
          "FULL JOIN (SELECT * FROM rightT WHERE FALSE) ON a = c and b < d",
        Seq())
    }
  }

  @TestTemplate
  def testLeftUpperAndLower(): Unit = {
    checkResult(
      "SELECT * FROM uppercasedata u LEFT JOIN lowercasedata l ON l.n = u.N",
      row(1, "A", 1, "a") ::
        row(2, "B", 2, "b") ::
        row(3, "C", 3, "c") ::
        row(4, "D", 4, "d") ::
        row(5, "E", null, null) ::
        row(6, "F", null, null) :: Nil
    )

    checkResult(
      "SELECT * FROM uppercasedata u LEFT JOIN lowercasedata l ON l.n = u.N AND l.n > 1",
      row(1, "A", null, null) ::
        row(2, "B", 2, "b") ::
        row(3, "C", 3, "c") ::
        row(4, "D", 4, "d") ::
        row(5, "E", null, null) ::
        row(6, "F", null, null) :: Nil
    )

    checkResult(
      "SELECT * FROM uppercasedata u LEFT JOIN lowercasedata l ON l.n = u.N AND u.N > 1",
      row(1, "A", null, null) ::
        row(2, "B", 2, "b") ::
        row(3, "C", 3, "c") ::
        row(4, "D", 4, "d") ::
        row(5, "E", null, null) ::
        row(6, "F", null, null) :: Nil
    )

    checkResult(
      "SELECT * FROM uppercasedata u LEFT JOIN lowercasedata l ON l.n = u.N AND l.l > u.L",
      row(1, "A", 1, "a") ::
        row(2, "B", 2, "b") ::
        row(3, "C", 3, "c") ::
        row(4, "D", 4, "d") ::
        row(5, "E", null, null) ::
        row(6, "F", null, null) :: Nil
    )
  }

  @TestTemplate
  def testLeftUpperAndLowerWithAgg(): Unit = {
    checkResult(
      """
        |SELECT l.N, count(*)
        |FROM uppercasedata l LEFT JOIN allnulls r ON (l.N = r.a)
        |GROUP BY l.N
      """.stripMargin,
      row(1, 1) ::
        row(2, 1) ::
        row(3, 1) ::
        row(4, 1) ::
        row(5, 1) ::
        row(6, 1) :: Nil
    )

    checkResult(
      """
        |SELECT r.a, count(*)
        |FROM uppercasedata l LEFT OUTER JOIN allnulls r ON (l.N = r.a)
        |GROUP BY r.a
      """.stripMargin,
      row(null, 6) :: Nil
    )
  }

  @TestTemplate
  def testRightUpperAndLower(): Unit = {
    checkResult(
      "SELECT * FROM lowercasedata l RIGHT JOIN uppercasedata u ON l.n = u.N",
      row(1, "a", 1, "A") ::
        row(2, "b", 2, "B") ::
        row(3, "c", 3, "C") ::
        row(4, "d", 4, "D") ::
        row(null, null, 5, "E") ::
        row(null, null, 6, "F") :: Nil
    )
    checkResult(
      "SELECT * FROM lowercasedata l RIGHT JOIN uppercasedata u ON l.n = u.N AND l.n > 1",
      row(null, null, 1, "A") ::
        row(2, "b", 2, "B") ::
        row(3, "c", 3, "C") ::
        row(4, "d", 4, "D") ::
        row(null, null, 5, "E") ::
        row(null, null, 6, "F") :: Nil
    )
    checkResult(
      "SELECT * FROM lowercasedata l RIGHT JOIN uppercasedata u ON l.n = u.N AND u.N > 1",
      row(null, null, 1, "A") ::
        row(2, "b", 2, "B") ::
        row(3, "c", 3, "C") ::
        row(4, "d", 4, "D") ::
        row(null, null, 5, "E") ::
        row(null, null, 6, "F") :: Nil
    )
    checkResult(
      "SELECT * FROM lowercasedata l RIGHT JOIN uppercasedata u ON l.n = u.N AND l.l > u.L",
      row(1, "a", 1, "A") ::
        row(2, "b", 2, "B") ::
        row(3, "c", 3, "C") ::
        row(4, "d", 4, "D") ::
        row(null, null, 5, "E") ::
        row(null, null, 6, "F") :: Nil
    )

  }

  @TestTemplate
  def testRightUpperAndLowerWithAgg(): Unit = {
    checkResult(
      """
        |SELECT l.a, count(*)
        |FROM allnulls l RIGHT OUTER JOIN uppercasedata r ON (l.a = r.N)
        |GROUP BY l.a
      """.stripMargin,
      row(null, 6) :: Nil
    )

    checkResult(
      """
        |SELECT r.N, count(*)
        |FROM allnulls l RIGHT OUTER JOIN uppercasedata r ON (l.a = r.N)
        |GROUP BY r.N
      """.stripMargin,
      row(1, 1) ::
        row(2, 1) ::
        row(3, 1) ::
        row(4, 1) ::
        row(5, 1) ::
        row(6, 1) :: Nil
    )
  }

  @TestTemplate
  def testFullUpperAndLower(): Unit = {
    if (expectedJoinType != NestedLoopJoin && expectedJoinType != BroadcastHashJoin) {
      val leftData = upperCaseData.filter(_.getField(0).asInstanceOf[Int] <= 4)
      val rightData = upperCaseData.filter(_.getField(0).asInstanceOf[Int] >= 3)

      registerCollection("leftUpper", leftData, INT_STRING, "N, L")
      registerCollection("rightUpper", rightData, INT_STRING, "N, L")

      checkResult(
        "SELECT * FROM leftUpper FULL JOIN rightUpper ON leftUpper.N = rightUpper.N",
        row(1, "A", null, null) ::
          row(2, "B", null, null) ::
          row(3, "C", 3, "C") ::
          row(4, "D", 4, "D") ::
          row(null, null, 5, "E") ::
          row(null, null, 6, "F") :: Nil
      )

      checkResult(
        "SELECT * FROM leftUpper FULL JOIN rightUpper ON " +
          "leftUpper.N = rightUpper.N AND leftUpper.N <> 3",
        row(1, "A", null, null) ::
          row(2, "B", null, null) ::
          row(3, "C", null, null) ::
          row(null, null, 3, "C") ::
          row(4, "D", 4, "D") ::
          row(null, null, 5, "E") ::
          row(null, null, 6, "F") :: Nil
      )

      checkResult(
        "SELECT * FROM leftUpper FULL JOIN rightUpper ON " +
          "leftUpper.N = rightUpper.N AND rightUpper.N <> 3",
        row(1, "A", null, null) ::
          row(2, "B", null, null) ::
          row(3, "C", null, null) ::
          row(null, null, 3, "C") ::
          row(4, "D", 4, "D") ::
          row(null, null, 5, "E") ::
          row(null, null, 6, "F") :: Nil
      )
    }
  }

  @TestTemplate
  def testFullUpperAndLowerWithAgg(): Unit = {
    if (expectedJoinType != NestedLoopJoin && expectedJoinType != BroadcastHashJoin) {
      checkResult(
        """
          |SELECT l.a, count(*)
          |FROM allnulls l FULL OUTER JOIN uppercasedata r ON (l.a = r.N)
          |GROUP BY l.a
      """.stripMargin,
        row(null, 10) :: Nil
      )

      checkResult(
        """
          |SELECT r.N, count(*)
          |FROM allnulls l FULL OUTER JOIN uppercasedata r ON (l.a = r.N)
          |GROUP BY r.N
        """.stripMargin,
        row(1, 1) ::
          row(2, 1) ::
          row(3, 1) ::
          row(4, 1) ::
          row(5, 1) ::
          row(6, 1) ::
          row(null, 4) :: Nil
      )

      checkResult(
        """
          |SELECT l.N, count(*)
          |FROM uppercasedata l FULL OUTER JOIN allnulls r ON (l.N = r.a)
          |GROUP BY l.N
        """.stripMargin,
        row(1, 1) ::
          row(2, 1) ::
          row(3, 1) ::
          row(4, 1) ::
          row(5, 1) ::
          row(6, 1) ::
          row(null, 4) :: Nil
      )

      checkResult(
        """
          |SELECT r.a, count(*)
          |FROM uppercasedata l FULL OUTER JOIN allnulls r ON (l.N = r.a)
          |GROUP BY r.a
        """.stripMargin,
        row(null, 10) :: Nil
      )
    }
  }
}

object OuterJoinITCase {
  @Parameters(name = "expectedJoinType={0}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array(BroadcastHashJoin),
      Array(HashJoin),
      Array(SortMergeJoin),
      Array(NestedLoopJoin))
  }
}
