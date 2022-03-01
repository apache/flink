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

import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class TableSinkITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  override def before(): Unit = {
    super.before()

    val srcDataId = TestValuesTableFactory.registerData(Seq(
      row("jason", 1L),
      row("jason", 1L),
      row("jason", 1L),
      row("jason", 1L)
    ))
    tEnv.executeSql(
      s"""
        |CREATE TABLE src (person String, votes BIGINT) WITH(
        |  'connector' = 'values',
        |  'data-id' = '$srcDataId'
        |)
        |""".stripMargin)

    val awardDataId = TestValuesTableFactory.registerData(Seq(
      row(1L, 5.2D),
      row(2L, 12.1D),
      row(3L, 18.3D),
      row(4L, 22.5D)
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
  }

  @Test
  def testJoinDisorderChangeLog(): Unit = {
    tEnv.executeSql(
      """
        |CREATE TABLE JoinDisorderChangeLog (
        |  person STRING, votes BIGINT, prize DOUBLE, age INT,
        |  PRIMARY KEY(person) NOT ENFORCED) WITH(
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)

    tEnv.executeSql(
      """
        |INSERT INTO JoinDisorderChangeLog
        |SELECT T1.person, T1.sum_votes, T1.prize, T2.age FROM
        | (SELECT T.person, T.sum_votes, award.prize FROM
        |   (SELECT person, SUM(votes) AS sum_votes FROM src GROUP BY person) T,
        |   award
        |   WHERE T.sum_votes = award.votes) T1, people T2
        | WHERE T1.person = T2.person
        |""".stripMargin).await()

    val result = TestValuesTableFactory.getResults("JoinDisorderChangeLog")
    val expected = List("+I[jason, 4, 22.5, 22]")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testSinkDisorderChangeLog(): Unit = {
    tEnv.executeSql(
      """
        |CREATE TABLE SinkDisorderChangeLog (
        |  person STRING, votes BIGINT, prize DOUBLE,
        |  PRIMARY KEY(person) NOT ENFORCED) WITH(
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)

    tEnv.executeSql(
      """
        |INSERT INTO SinkDisorderChangeLog
        |SELECT T.person, T.sum_votes, award.prize FROM
        |   (SELECT person, SUM(votes) AS sum_votes FROM src GROUP BY person) T, award
        |   WHERE T.sum_votes = award.votes
        |""".stripMargin).await()

    val result = TestValuesTableFactory.getResults("SinkDisorderChangeLog")
    val expected = List("+I[jason, 4, 22.5]")
    assertEquals(expected.sorted, result.sorted)
  }

  @Test
  def testSinkDisorderChangeLogWithRank(): Unit = {
    tEnv.executeSql(
      """
        |CREATE TABLE SinkRankChangeLog (
        |  person STRING, votes BIGINT,
        |  PRIMARY KEY(person) NOT ENFORCED) WITH(
        |  'connector' = 'values',
        |  'sink-insert-only' = 'false'
        |)
        |""".stripMargin)

    tEnv.executeSql(
      """
        |INSERT INTO SinkRankChangeLog
        |SELECT person, sum_votes FROM
        | (SELECT person, sum_votes,
        |   ROW_NUMBER() OVER (PARTITION BY vote_section ORDER BY sum_votes DESC) AS rank_number
        |   FROM (SELECT person, SUM(votes) AS sum_votes, SUM(votes) / 2 AS vote_section FROM src
        |      GROUP BY person))
        |   WHERE rank_number < 10
        |""".stripMargin).await()

    val result = TestValuesTableFactory.getResults("SinkRankChangeLog")
    val expected = List("+I[jason, 4]")
    assertEquals(expected.sorted, result.sorted)
  }
}
