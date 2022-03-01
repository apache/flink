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

import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._

import org.junit.{Before, Test}

import scala.collection.Seq

class JoinWithoutKeyITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfSmallData3)
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
    registerCollection("Table5", data5, type5, "d, e, f, g, h", nullablesOfData5)
    registerCollection("NullTable3", nullData3, type3, "a, b, c", nullablesOfNullData3)
    registerCollection("NullTable5", nullData5, type5, "d, e, f, g, h", nullablesOfNullData5)
    registerCollection("l", data2_3, INT_DOUBLE, "a, b", nullablesOfData2_3)
    registerCollection("r", data2_2, INT_DOUBLE, "c, d")

    registerCollection("testData", intStringData, INT_STRING, "a, b", nullablesOfIntStringData)
    registerCollection("testData2", intIntData2, INT_INT, "c, d", nullablesOfIntIntData2)
    registerCollection("testData3", intIntData3, INT_INT, "e, f", nullablesOfIntIntData3)
    registerCollection("leftT", SemiJoinITCase.leftT, INT_DOUBLE, "a, b")
    registerCollection("rightT", SemiJoinITCase.rightT, INT_DOUBLE, "c, d")
  }

  // single row join

  @Test
  def testCrossJoinWithLeftSingleRowInput(): Unit = {
    checkResult(
      "SELECT * FROM (SELECT count(*) FROM SmallTable3) CROSS JOIN SmallTable3",
      Seq(
        row(3, 1, 1, "Hi"),
        row(3, 2, 2, "Hello"),
        row(3, 3, 2, "Hello world")
      ))
  }

  @Test
  def testCrossJoinWithRightSingleRowInput(): Unit = {
    checkResult(
      "SELECT * FROM SmallTable3 CROSS JOIN (SELECT count(*) FROM SmallTable3)",
      Seq(
        row(1, 1, "Hi", 3),
        row(2, 2, "Hello", 3),
        row(3, 2, "Hello world", 3)
      ))
  }

  @Test
  def testCrossJoinWithEmptySingleRowInput(): Unit = {
    checkResult(
      "SELECT * FROM SmallTable3 CROSS JOIN (SELECT count(*) FROM SmallTable3 HAVING count(*) < 0)",
      Seq())
  }

  @Test
  def testLeftNullRightJoin(): Unit = {
    checkResult(
      "SELECT d, cnt FROM (SELECT cnt FROM (SELECT COUNT(*) AS cnt FROM SmallTable3) " +
          "WHERE cnt < 0) RIGHT JOIN Table5 ON d < cnt",
      Seq(
        row(1, null),
        row(2, null), row(2, null),
        row(3, null), row(3, null), row(3, null),
        row(4, null), row(4, null), row(4, null), row(4, null),
        row(5, null), row(5, null), row(5, null), row(5, null), row(5, null)
      ))
  }

  @Test
  def testLeftSingleRightJoinEqualPredicate(): Unit = {
    checkResult(
      "SELECT d, cnt FROM (SELECT COUNT(*) AS cnt FROM SmallTable3) RIGHT JOIN Table5 ON cnt = d",
      Seq(
        row(1, null),
        row(2, null), row(2, null),
        row(3, 3), row(3, 3), row(3, 3),
        row(4, null), row(4, null), row(4, null), row(4, null),
        row(5, null), row(5, null), row(5, null), row(5, null), row(5, null)
      ))
  }

  @Test
  def testSingleJoinWithReusePerRecordCode(): Unit = {
    checkResult(
      "SELECT d, cnt FROM (SELECT COUNT(*) AS cnt FROM SmallTable3) " +
          "RIGHT JOIN Table5 ON d = UNIX_TIMESTAMP(cast(CURRENT_TIMESTAMP as VARCHAR))",
      Seq(
        row(1, null),
        row(2, null), row(2, null),
        row(3, null), row(3, null), row(3, null),
        row(4, null), row(4, null), row(4, null), row(4, null),
        row(5, null), row(5, null), row(5, null), row(5, null), row(5, null)
      ))
  }

  @Test
  def testLeftSingleRightJoinNotEqualPredicate(): Unit = {
    checkResult(
      "SELECT d, cnt FROM (SELECT COUNT(*) AS cnt FROM SmallTable3) RIGHT JOIN Table5 ON cnt > d",
      Seq(
        row(1, 3),
        row(2, 3), row(2, 3),
        row(3, null), row(3, null), row(3, null),
        row(4, null), row(4, null), row(4, null), row(4, null),
        row(5, null), row(5, null), row(5, null), row(5, null), row(5, null)
      ))
  }

  @Test
  def testRightNullLeftJoin(): Unit = {
    checkResult(
      "SELECT a, cnt FROM SmallTable3 LEFT JOIN (SELECT cnt FROM " +
          "(SELECT COUNT(*) AS cnt FROM Table5) WHERE cnt < 0) ON cnt > a",
      Seq(
        row(1, null), row(2, null), row(3, null)
      ))
  }

  @Test
  def testRightSingleLeftJoinEqualPredicate(): Unit = {
    checkResult(
      "SELECT d, cnt FROM Table5 LEFT JOIN (SELECT COUNT(*) AS cnt FROM SmallTable3) ON cnt = d",
      Seq(
        row(1, null),
        row(2, null), row(2, null),
        row(3, 3), row(3, 3), row(3, 3),
        row(4, null), row(4, null), row(4, null), row(4, null),
        row(5, null), row(5, null), row(5, null), row(5, null), row(5, null)
      ))
  }

  @Test
  def testRightSingleLeftJoinNotEqualPredicate(): Unit = {
    checkResult(
      "SELECT d, cnt FROM Table5 LEFT JOIN (SELECT COUNT(*) AS cnt FROM SmallTable3) ON cnt < d",
      Seq(
        row(1, null),
        row(2, null), row(2, null),
        row(3, null), row(3, null), row(3, null),
        row(4, 3), row(4, 3), row(4, 3), row(4, 3),
        row(5, 3), row(5, 3), row(5, 3), row(5, 3), row(5, 3)
      ))
  }

  @Test
  def testRightSingleLeftJoinTwoFields(): Unit = {
    checkResult(
      "SELECT d, cnt, cnt2 FROM Table5 LEFT JOIN " +
          "(SELECT COUNT(*) AS cnt,COUNT(*) AS cnt2 FROM SmallTable3 ) AS x ON d = cnt",
      Seq(
        row(1, null, null),
        row(2, null, null), row(2, null, null),
        row(3, 3, 3), row(3, 3, 3), row(3, 3, 3),
        row(4, null, null), row(4, null, null), row(4, null, null), row(4, null, null),
        row(5, null, null), row(5, null, null), row(5, null, null), row(5, null, null),
        row(5, null, null)
      ))
  }

  // inner/cross/outer join

  @Test
  def testCrossJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 CROSS JOIN NullTable5 where a = 3 and e > 13",
      Seq(
        row("Hello world", "JKL"),
        row("Hello world", "KLM"),
        row("Hello world", "NullTuple"),
        row("Hello world", "NullTuple")))
  }

  @Test
  def testInnerJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3, NullTable5 WHERE b > e AND b < 3",
      Seq(row("Hello", "Hallo"), row("Hello world", "Hallo")))
  }

  @Test
  def testLeftJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable3 LEFT JOIN NullTable5 ON a > d + 10 where a = 3 OR a = 12",
      Seq(row("Comment#6", "Hallo"), row("Hello world", "null")))
  }

  @Test
  def testRightJoin(): Unit = {
    checkResult(
      "SELECT c, g FROM NullTable5 RIGHT JOIN NullTable3 ON a > d + 10 where a = 3 OR a = 12",
      Seq(row("Comment#6", "Hallo"), row("Hello world", "null")))
  }

  @Test
  def testFullJoin(): Unit = {
    checkResult(
      "SELECT * FROM (SELECT c, g FROM NullTable5 FULL JOIN NullTable3 ON a > d) " +
          "WHERE c is null or g is null",
      Seq(
        row("Hi", null), row("NullTuple", null), row("NullTuple", null),
        row(null, "NullTuple"), row(null, "NullTuple")
      ))
  }

  @Test
  def testUncorrelatedExist(): Unit = {
    checkResult(
      "select * from l where exists (select * from r where c > 0)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0))
    )

    checkResult(
      "select * from l where exists (select * from r where c < 0)",
      Seq()
    )
  }

  @Test
  def testUncorrelatedNotExist(): Unit = {
    checkResult(
      "select * from l where not exists (select * from r where c < 0)",
      Seq(row(2, 3.0), row(2, 3.0), row(3, 2.0), row(4, 1.0))
    )

    checkResult(
      "select * from l where not exists (select * from r where c > 0)",
      Seq()
    )
  }

  @Test
  def testCartesianJoin(): Unit = {
    val data = Seq(row(1, null), row(2, 2))
    registerCollection("T1", data, INT_INT, "a, b")
    registerCollection("T2", data, INT_INT, "c, d")

    checkResult(
      "select * from T1 JOIN T2 ON true",
      Seq(row(1, null, 1, null), row(1, null, 2, 2), row(2, 2, 1, null), row(2, 2, 2, 2))
    )

    checkResult(
      "select * from T1 JOIN T2 ON a > c",
      Seq(row(2, 2, 1, null))
    )

  }

  @Test
  def testInner(): Unit = {
    checkResult(
      """
          SELECT b, c, d FROM testData, testData2 WHERE a = 2
        """.stripMargin,
      row("2", 1, 1) ::
          row("2", 1, 2) ::
          row("2", 2, 1) ::
          row("2", 2, 2) ::
          row("2", 3, 1) ::
          row("2", 3, 2) :: Nil)

    checkResult(
      """
          SELECT b, c, d FROM testData, testData2 WHERE a < c
        """.stripMargin,
      row("1", 2, 1) ::
          row("1", 2, 2) ::
          row("1", 3, 1) ::
          row("1", 3, 2) ::
          row("2", 3, 1) ::
          row("2", 3, 2) :: Nil)

    checkResult(
      """
          SELECT b, c, d FROM testData JOIN testData2 ON a < c
        """.stripMargin,
      row("1", 2, 1) ::
          row("1", 2, 2) ::
          row("1", 3, 1) ::
          row("1", 3, 2) ::
          row("2", 3, 1) ::
          row("2", 3, 2) :: Nil)
  }

  @Test
  def testInnerExpr(): Unit = {
    checkResult(
      "SELECT * FROM testData2, testData3 WHERE c - e = 0",
      Seq(
        row(1, 1, 1, null),
        row(1, 2, 1, null),
        row(2, 1, 2, 2),
        row(2, 2, 2, 2)
      ))

    checkResult(
      "SELECT * FROM testData2, testData3 WHERE c - e = 1",
      Seq(
        row(2, 1, 1, null),
        row(2, 2, 1, null),
        row(3, 1, 2, 2),
        row(3, 2, 2, 2)
      ))
  }

  @Test
  def testNonKeySemi(): Unit = {
    checkResult(
      "SELECT * FROM testData3 WHERE EXISTS (SELECT * FROM testData2)",
      row(1, null) :: row(2, 2) :: Nil)
    checkResult(
      "SELECT * FROM testData3 WHERE NOT EXISTS (SELECT * FROM testData2)",
      Nil)
    checkResult(
      """
        |SELECT e FROM testData3
        |WHERE
        |  EXISTS (SELECT * FROM testData)
        |OR
        |  EXISTS (SELECT * FROM testData2)""".stripMargin,
      row(1) :: row(2) :: Nil)
    checkResult(
      """
        |SELECT a FROM testData
        |WHERE
        |  a IN (SELECT c FROM testData2)
        |OR
        |  a IN (SELECT e FROM testData3)""".stripMargin,
      row(1) :: row(2) :: row(3) :: Nil)
  }

  @Test
  def testComposedNonEqualConditionLeftAnti(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a < c AND b < d)",
      Seq(row(3, 3.0), row(6, null), row(null, 5.0), row(null, null)))
  }

  @Test
  def testComposedNonEqualConditionLeftSemi(): Unit = {
    checkResult(
      "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a < c AND b < d)",
      Seq(row(1, 2.0), row(1, 2.0), row(2, 1.0), row(2, 1.0)))
  }
}
