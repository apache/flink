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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.planner.expressions.utils.TestNonDeterministicUdf
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode

import org.assertj.core.api.Assertions
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class TableSinkITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  override def before(): Unit = {
    super.before()

    val srcDataId = TestValuesTableFactory.registerData(
      Seq(
        row("jason", 1L),
        row("jason", 1L),
        row("jason", 1L),
        row("jason", 1L)
      ))
    tEnv.executeSql(s"""
                       |CREATE TABLE src (person String, votes BIGINT) WITH(
                       |  'connector' = 'values',
                       |  'data-id' = '$srcDataId'
                       |)
                       |""".stripMargin)

    val awardDataId = TestValuesTableFactory.registerData(
      Seq(
        row(1L, 5.2d),
        row(2L, 12.1d),
        row(3L, 18.3d),
        row(4L, 22.5d)
      ))
    tEnv.executeSql(
      s"""
         |CREATE TABLE award (votes BIGINT, prize DOUBLE, PRIMARY KEY(votes) NOT ENFORCED) WITH(
         |  'connector' = 'values',
         |  'data-id' = '$awardDataId'
         |)
         |""".stripMargin)

    val peopleDataId = TestValuesTableFactory.registerData(Seq(row("jason", 22)))
    tEnv.executeSql(
      s"""
         |CREATE TABLE people (person STRING, age INT, PRIMARY KEY(person) NOT ENFORCED) WITH(
         |  'connector' = 'values',
         |  'data-id' = '$peopleDataId'
         |)
         |""".stripMargin)

    val userDataId = TestValuesTableFactory.registerData(TestData.userChangelog)
    tEnv.executeSql(s"""
                       |CREATE TABLE users (
                       |  user_id STRING,
                       |  user_name STRING,
                       |  email STRING,
                       |  balance DECIMAL(18,2),
                       |  primary key (user_id) not enforced
                       |) WITH (
                       | 'connector' = 'values',
                       | 'data-id' = '$userDataId',
                       | 'changelog-mode' = 'I,UA,UB,D'
                       |)
                       |""".stripMargin)
  }

  @Test
  def testJoinDisorderChangeLog(): Unit = {
    tEnv.executeSql("""
                      |CREATE TABLE JoinDisorderChangeLog (
                      |  person STRING, votes BIGINT, prize DOUBLE, age INT,
                      |  PRIMARY KEY(person) NOT ENFORCED) WITH(
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |INSERT INTO JoinDisorderChangeLog
                    |SELECT T1.person, T1.sum_votes, T1.prize, T2.age FROM
                    | (SELECT T.person, T.sum_votes, award.prize FROM
                    |   (SELECT person, SUM(votes) AS sum_votes FROM src GROUP BY person) T,
                    |   award
                    |   WHERE T.sum_votes = award.votes) T1, people T2
                    | WHERE T1.person = T2.person
                    |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("JoinDisorderChangeLog")
    val expected = List("+I[jason, 4, 22.5, 22]")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testSinkDisorderChangeLog(): Unit = {
    tEnv.executeSql("""
                      |CREATE TABLE SinkDisorderChangeLog (
                      |  person STRING, votes BIGINT, prize DOUBLE,
                      |  PRIMARY KEY(person) NOT ENFORCED) WITH(
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |INSERT INTO SinkDisorderChangeLog
                    |SELECT T.person, T.sum_votes, award.prize FROM
                    |   (SELECT person, SUM(votes) AS sum_votes FROM src GROUP BY person) T, award
                    |   WHERE T.sum_votes = award.votes
                    |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("SinkDisorderChangeLog")
    val expected = List("+I[jason, 4, 22.5]")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testSinkDisorderChangeLogWithRank(): Unit = {
    tEnv.executeSql("""
                      |CREATE TABLE SinkRankChangeLog (
                      |  person STRING, votes BIGINT,
                      |  PRIMARY KEY(person) NOT ENFORCED) WITH(
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)

    tEnv
      .executeSql(
        """
          |INSERT INTO SinkRankChangeLog
          |SELECT person, sum_votes FROM
          | (SELECT person, sum_votes,
          |   ROW_NUMBER() OVER (PARTITION BY vote_section ORDER BY sum_votes DESC) AS rank_number
          |   FROM (SELECT person, SUM(votes) AS sum_votes, SUM(votes) / 2 AS vote_section FROM src
          |      GROUP BY person))
          |   WHERE rank_number < 10
          |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("SinkRankChangeLog")
    val expected = List("+I[jason, 4]")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testChangelogSourceWithNonDeterministicFuncSinkWithDifferentPk(): Unit = {
    tEnv.createTemporaryFunction("ndFunc", new TestNonDeterministicUdf)
    tEnv.executeSql("""
                      |CREATE TABLE sink_with_pk (
                      |  user_id STRING,
                      |  user_name STRING,
                      |  email STRING,
                      |  balance DECIMAL(18,2),
                      |  PRIMARY KEY(email) NOT ENFORCED
                      |) WITH(
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)

    tEnv
      .executeSql(s"""
                     |insert into sink_with_pk
                     |select user_id, SPLIT_INDEX(ndFunc(user_name), '-', 0), email, balance
                     |from users
                     |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("sink_with_pk")
    val expected = List(
      "+I[user1, Tom, tom123@gmail.com, 8.10]",
      "+I[user3, Bailey, bailey@qq.com, 9.99]",
      "+I[user4, Tina, tina@gmail.com, 11.30]")
    assertEquals(expected.sorted, result.sorted)

    val rawResult = TestValuesTableFactory.getRawResultsAsStrings("sink_with_pk")
    val expectedRaw = List(
      "+I[user1, Tom, tom@gmail.com, 10.02]",
      "+I[user2, Jack, jack@hotmail.com, 71.20]",
      "-D[user1, Tom, tom@gmail.com, 10.02]",
      "+I[user1, Tom, tom123@gmail.com, 8.10]",
      "+I[user3, Bailey, bailey@gmail.com, 9.99]",
      "-D[user2, Jack, jack@hotmail.com, 71.20]",
      "+I[user4, Tina, tina@gmail.com, 11.30]",
      "-D[user3, Bailey, bailey@gmail.com, 9.99]",
      "+I[user3, Bailey, bailey@qq.com, 9.99]"
    )
    assertEquals(expectedRaw, rawResult.toList)
  }

  @Test
  def testInsertPartColumn(): Unit = {
    tEnv.executeSql("""
                      |CREATE TABLE zm_test (
                      |  `person` String,
                      |  `votes` BIGINT,
                      |  `m1` MAP<STRING, BIGINT>,
                      |  `m2` MAP<STRING NOT NULL, BIGINT>,
                      |  `m3` MAP<STRING, BIGINT NOT NULL>,
                      |  `m4` MAP<STRING NOT NULL, BIGINT NOT NULL>
                      |) WITH (
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'true'
                      |)
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into zm_test(`person`, `votes`)
                    |  select
                    |    `person`,
                    |    `votes`
                    |  from
                    |    src
                    |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("zm_test")
    val expected = List(
      "+I[jason, 1, null, null, null, null]",
      "+I[jason, 1, null, null, null, null]",
      "+I[jason, 1, null, null, null, null]",
      "+I[jason, 1, null, null, null, null]"
    )
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testCreateTableAsSelect(): Unit = {
    tEnv
      .executeSql("""
                    |CREATE TABLE MyCtasTable
                    | WITH (
                    |   'connector' = 'values',
                    |   'sink-insert-only' = 'true'
                    |) AS
                    |  SELECT
                    |    `person`,
                    |    `votes`
                    |  FROM
                    |    src
                    |""".stripMargin)
      .await()
    val actual = TestValuesTableFactory.getResultsAsStrings("MyCtasTable")
    val expected = List(
      "+I[jason, 1]",
      "+I[jason, 1]",
      "+I[jason, 1]",
      "+I[jason, 1]"
    )
    Assertions.assertThat(actual.sorted).isEqualTo(expected.sorted)
    // test statement set
    val statementSet = tEnv.createStatementSet()
    statementSet.addInsertSql("""
                                |CREATE TABLE MyCtasTableUseStatement
                                | WITH (
                                |   'connector' = 'values',
                                |   'sink-insert-only' = 'true'
                                |) AS
                                |  SELECT
                                |    `person`,
                                |    `votes`
                                |  FROM
                                |    src
                                |""".stripMargin)
    statementSet.execute().await()
    val actualUseStatement = TestValuesTableFactory.getResultsAsStrings("MyCtasTableUseStatement")
    Assertions.assertThat(actualUseStatement.sorted).isEqualTo(expected.sorted)
  }

  @Test
  def testCreateTableAsSelectWithoutOptions(): Unit = {
    // TODO: CTAS supports ManagedTable
    // If the connector option is not specified, Flink will creates a Managed table.
    // Managed table requires two layers of log storage and file storage
    // and depends on the flink table store, CTAS will support Managed Table in the future.
    Assertions
      .assertThatThrownBy(
        () => tEnv.executeSql("CREATE TABLE MyCtasTable AS SELECT `person`, `votes` FROM src"))
      .hasMessage("You should enable the checkpointing for sinking to managed table " +
        "'default_catalog.default_database.MyCtasTable'," +
        " managed table relies on checkpoint to commit and the data is visible only after commit.")
  }

  @Test
  def testPartialInsert(): Unit = {
    val srcDataId = TestValuesTableFactory.registerData(
      Seq(
        row(1L, "jason", 3L, "X", 43),
        row(2L, "andy", 2L, "Y", 32),
        row(3L, "clark", 1L, "Z", 29)
      ))
    tEnv.executeSql(s"""
                       |CREATE TABLE test_source (
                       |  id bigint,
                       |  person String,
                       |  votes bigint,
                       |  city String,
                       |  age int)
                       |WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$srcDataId'
                       |)
                       |""".stripMargin)
    tEnv.executeSql("""
                      |CREATE TABLE test_sink (
                      |  id bigint,
                      |  person String,
                      |  votes bigint,
                      |  city String,
                      |  age int,
                      |  primary key(id) not enforced
                      |) WITH (
                      |  'connector' = 'values',
                      |  'sink-insert-only' = 'false'
                      |)
                      |""".stripMargin)

    tEnv
      .executeSql("""
                    |insert into test_sink (id, person, votes)
                    |  select
                    |    id,
                    |    person,
                    |    votes
                    |  from
                    |    test_source
                    |""".stripMargin)
      .await()

    val result = TestValuesTableFactory.getResultsAsStrings("test_sink")
    val expected = List(
      "+I[1, jason, 3, null, null]",
      "+I[2, andy, 2, null, null]",
      "+I[3, clark, 1, null, null]")
    assertEquals(expected.sorted, result.sorted)

    tEnv
      .executeSql("""
                    |insert into test_sink (id, city, age)
                    |  select
                    |    id,
                    |    city,
                    |    age 
                    |  from
                    |    test_source
                    |""".stripMargin)
      .await()

    val result2 = TestValuesTableFactory.getResultsAsStrings("test_sink")
    val expected2 =
      List("+I[1, jason, 3, X, 43]", "+I[2, andy, 2, Y, 32]", "+I[3, clark, 1, Z, 29]")
    assertEquals(expected2.sorted, result2.sorted)
  }
}
