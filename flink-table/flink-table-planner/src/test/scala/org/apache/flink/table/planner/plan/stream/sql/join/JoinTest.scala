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
package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableFunc1, TableTestBase}

import org.junit.Test

class JoinTest extends TableTestBase {

  private val util: StreamTableTestUtil = streamTestUtil()
  util.addTableSource[(Int, Long, Long)]("A", 'a1, 'a2, 'a3)
  util.addTableSource[(Int, Long, Long)]("B", 'b1, 'b2, 'b3)
  util.addTableSource[(Int, Long, String)]("t", 'a, 'b, 'c)
  util.addTableSource[(Long, String, Int)]("s", 'x, 'y, 'z)

  @Test
  def testDependentConditionDerivationInnerJoin(): Unit = {
    util.verifyExecPlan("SELECT a1, b1 FROM A JOIN B ON (a1 = 1 AND b1 = 1) OR (a2 = 2 AND b2 = 2)")
  }

  @Test
  def testDependentConditionDerivationInnerJoinWithTrue(): Unit = {
    util.verifyExecPlan("SELECT a1, b1 FROM A JOIN B ON (a1 = 1 AND b1 = 1) OR (a2 = 2 AND true)")
  }

  @Test
  def testDependentConditionDerivationInnerJoinWithNull(): Unit = {
    util.verifyExecPlan("SELECT * FROM t JOIN s ON (a = 1 AND x = 1) OR (a = 2 AND y is null)")
  }

  @Test
  def testInnerJoin(): Unit = {
    util.verifyExecPlan("SELECT a1, b1 FROM A JOIN B ON a1 = b1")
  }

  @Test
  def testInnerJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) JOIN ($query2) ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testInnerJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a2 = b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinNonEqui(): Unit = {
    util.verifyRelPlan(
      "SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1 AND a2 > b2",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1 AND a2 > b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1 AND a2 > b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2 AND a1 > b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoin(): Unit = {
    util.verifyRelPlan("SELECT a1, b1 FROM A LEFT JOIN B ON a1 = b1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN ($query2) ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) LEFT JOIN B ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testLeftJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) LEFT JOIN ($query2) ON a2 = b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinNonEqui(): Unit = {
    util.verifyRelPlan(
      "SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1 AND a2 > b2",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1 AND a2 > b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithRightNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1 AND a2 > b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2 AND a1 > b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoin(): Unit = {
    util.verifyRelPlan("SELECT a1, b1 FROM A RIGHT JOIN B ON a1 = b1", ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN ($query2) ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithRightNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query = s"SELECT a1, b1 FROM ($query1) RIGHT JOIN B ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testRightJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B group by b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) RIGHT JOIN ($query2) ON a2 = b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinNonEqui(): Unit = {
    util.verifyRelPlan(
      "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1 AND a2 > b2",
      ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithEqualPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1 AND a2 > b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithFullNotPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1 AND a2 > b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithPkNonEqui(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2 AND a1 > b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM A FULL JOIN B ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithEqualPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN ($query2) ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithFullNotPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query = s"SELECT a1, b1 FROM ($query1) FULL JOIN B ON a1 = b1"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFullJoinWithPk(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM A GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM B GROUP BY b1"
    val query = s"SELECT a1, a2, b1, b2 FROM ($query1) FULL JOIN ($query2) ON a2 = b2"
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSelfJoinPlan(): Unit = {
    util.addTableSource[(Long, String)]("src", 'key, 'v)
    val sql =
      s"""
         |SELECT * FROM (
         |  SELECT * FROM src WHERE key = 0) src1
         |LEFT OUTER JOIN (
         |  SELECT * FROM src WHERE key = 0) src2
         |ON (src1.key = src2.key AND src2.key > 10)
       """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testJoinWithSort(): Unit = {
    util.addTableSource[(Int, Int, String)]("MyTable3", 'i, 'j, 't)
    util.addTableSource[(Int, Int)]("MyTable4", 'i, 'k)

    val sqlQuery =
      """
        |SELECT * FROM
        |  MyTable3 FULL JOIN
        |  (SELECT * FROM MyTable4 ORDER BY MyTable4.i DESC, MyTable4.k ASC) MyTable4
        |  ON MyTable3.i = MyTable4.i and MyTable3.i = MyTable4.k
      """.stripMargin

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinEquiPred(): Unit = {
    util.verifyExecPlan("SELECT b, y FROM t LEFT OUTER JOIN s ON a = z")
  }

  @Test
  def testLeftOuterJoinEquiAndLocalPred(): Unit = {
    util.verifyExecPlan("SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < 2")
  }

  @Test
  def testLeftOuterJoinEquiAndNonEquiPred(): Unit = {
    util.verifyExecPlan("SELECT b, y FROM t LEFT OUTER JOIN s ON a = z AND b < x")
  }

  @Test
  def testRightOuterJoinEquiPred(): Unit = {
    util.verifyExecPlan("SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z")
  }

  @Test
  def testRightOuterJoinEquiAndLocalPred(): Unit = {
    util.verifyExecPlan("SELECT b, x FROM t RIGHT OUTER JOIN s ON a = z AND x < 2")
  }

  @Test
  def testRightOuterJoinEquiAndNonEquiPred(): Unit = {
    util.verifyExecPlan("SELECT b, y FROM t RIGHT OUTER JOIN s ON a = z AND b < x")
  }

  @Test
  def testJoinAndSelectOnPartialCompositePrimaryKey(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TABLE tableWithCompositePk (
                               |  pk1 INT,
                               |  pk2 BIGINT,
                               |  PRIMARY KEY (pk1, pk2) NOT ENFORCED
                               |) WITH (
                               |  'connector'='values'
                               |)
                               |""".stripMargin)
    util.verifyExecPlan("SELECT A.a1 FROM A LEFT JOIN tableWithCompositePk T ON A.a1 = T.pk1")
  }

  @Test
  def testJoinDisorderChangeLog(): Unit = {
    util.tableEnv.executeSql("""
                               |CREATE TABLE src (person String, votes BIGINT) WITH(
                               |  'connector' = 'values'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE TABLE award (votes BIGINT, prize DOUBLE, PRIMARY KEY(votes) NOT ENFORCED) WITH(
        |  'connector' = 'values'
        |)
        |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE TABLE people (person STRING, age INT, PRIMARY KEY(person) NOT ENFORCED) WITH(
        |  'connector' = 'values'
        |)
        |""".stripMargin)

    util.verifyExecPlan("""
                          |SELECT T1.person, T1.sum_votes, T1.prize, T2.age FROM
                          | (SELECT T.person, T.sum_votes, award.prize FROM
                          |   (SELECT person, SUM(votes) AS sum_votes FROM src GROUP BY person) T,
                          |   award
                          |   WHERE T.sum_votes = award.votes) T1, people T2
                          | WHERE T1.person = T2.person
                          |""".stripMargin)
  }

  @Test
  def testJoinOutputUpsertKeyNotMatchSinkPk(): Unit = {
    // test for FLINK-20370
    util.tableEnv.executeSql("""
                               |create table source_city (
                               | id varchar,
                               | city_name varchar,
                               | primary key (id) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create table source_customer (
                               | customer_id varchar,
                               | city_id varchar,
                               | age int,
                               | gender varchar,
                               | update_time timestamp(3),
                               | primary key (customer_id) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create table sink (
                               | city_id varchar,
                               | city_name varchar,
                               | customer_cnt bigint,
                               | primary key (city_name) not enforced
                               |) with (
                               | 'connector' = 'values'
                               | ,'sink-insert-only' = 'false'
                               |)
                               |""".stripMargin)

    // verify UB should reserve and add upsertMaterialize if join outputs' upsert keys differs from
    // sink's pks
    util.verifyExplainInsert(
      """
        |insert into sink
        |select t1.city_id, t2.city_name, t1.customer_cnt
        | from (select city_id, count(*) customer_cnt from source_customer group by city_id) t1
        | join source_city t2 on t1.city_id = t2.id
        |""".stripMargin,
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testJoinOutputUpsertKeyInSinkPk(): Unit = {
    // test for FLINK-20370
    util.tableEnv.executeSql("""
                               |create table source_city (
                               | id varchar,
                               | city_name varchar,
                               | primary key (id) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create table source_customer (
                               | customer_id varchar,
                               | city_id varchar,
                               | age int,
                               | gender varchar,
                               | update_time timestamp(3),
                               | primary key (customer_id) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create table sink (
                               | city_id varchar,
                               | city_name varchar,
                               | customer_cnt bigint,
                               | primary key (city_id, city_name) not enforced
                               |) with (
                               | 'connector' = 'values'
                               | ,'sink-insert-only' = 'false'
                               |)
                               |""".stripMargin)

    // verify UB should reserve and no upsertMaterialize if join outputs' upsert keys are subset of
    // sink's pks
    util.verifyExplainInsert(
      """
        |insert into sink
        |select t1.city_id, t2.city_name, t1.customer_cnt
        | from (select city_id, count(*) customer_cnt from source_customer group by city_id) t1
        | join source_city t2 on t1.city_id = t2.id
        |""".stripMargin,
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testJoinOutputLostUpsertKeyWithSinkPk(): Unit = {
    // test for FLINK-20370
    util.tableEnv.executeSql("""
                               |create table source_city (
                               | id varchar,
                               | city_name varchar,
                               | primary key (id) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create table source_customer (
                               | customer_id varchar,
                               | city_id varchar,
                               | age int,
                               | gender varchar,
                               | update_time timestamp(3),
                               | primary key (customer_id) not enforced
                               |) with (
                               | 'connector' = 'values',
                               | 'changelog-mode' = 'I,UA,D'
                               |)
                               |""".stripMargin)

    util.tableEnv.executeSql("""
                               |create table sink (
                               | city_name varchar,
                               | customer_cnt bigint,
                               | primary key (city_name) not enforced
                               |) with (
                               | 'connector' = 'values'
                               | ,'sink-insert-only' = 'false'
                               |)
                               |""".stripMargin)

    // verify UB should reserve and add upsertMaterialize if join outputs' lost upsert keys
    util.verifyExplainInsert(
      """
        |insert into sink
        |select t2.city_name, t1.customer_cnt
        | from (select city_id, count(*) customer_cnt from source_customer group by city_id) t1
        | join source_city t2 on t1.city_id = t2.id
        |""".stripMargin,
      ExplainDetail.CHANGELOG_MODE
    )
  }

  @Test
  def testInnerJoinWithFilterPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |   (select a1, count(a2) as a2 from A group by a1)
                          |   join
                          |   (select b1, count(b2) as b2 from B group by b1)
                          |   on true where a1 = b1 and a2 = b2 and b1 = 2
                          |""".stripMargin)
  }

  @Test
  def testInnerJoinWithJoinConditionPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |  (select a1, count(a2) as a2 from A group by a1)
                          |   join
                          |  (select b1, count(b2) as b2 from B group by b1)
                          |   on true where a1 = b1 and a2 = b2 and b1 = 2 and a2 = 1
                          |""".stripMargin)
  }

  @Test
  def testLeftJoinWithFilterPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |  (select a1, count(a2) as a2 from A group by a1)
                          |   left join
                          |  (select b1, count(b2) as b2 from B group by b1)
                          |   on true where a1 = b1 and b2 = a2 and a1 = 2
                          |""".stripMargin)
  }

  @Test
  def testLeftJoinWithJoinConditionPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |  (select a1, count(a2) as a2 from A group by a1)
                          |   left join
                          |  (select b1, count(b2) as b2 from B group by b1)
                          |   on a1 = b1 and a2 = b2 and a1 = 2 and b2 = 1
                          |""".stripMargin)
  }

  @Test
  def testRightJoinWithFilterPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          |  (select a1, count(a2) as a2 from A group by a1)
                          |   right join
                          |  (select b1, count(b2) as b2 from B group by b1)
                          |   on true where a1 = b1 and a2 = b2 and b1 = 2
                          |""".stripMargin)
  }

  @Test
  def testRightJoinWithJoinConditionPushDown(): Unit = {
    util.verifyExecPlan("""
                          |SELECT * FROM
                          | (select a1, count(a2) as a2 from A group by a1)
                          |   right join
                          | (select b1, count(b2) as b2 from B group by b1)
                          |   on a1 = b1 and a2 = b2 and b1 = 2 and a2 = 1
                          |""".stripMargin)
  }

  @Test
  def testJoinUDTFWithInvalidJoinHint(): Unit = {
    // TODO the error message should be improved after we support extracting alias from table func
    util.addTemporarySystemFunction("TableFunc1", new TableFunc1)
    util.verifyExpectdException(
      "SELECT /*+ LOOKUP('table'='D') */ T.a FROM t AS T CROSS JOIN LATERAL TABLE(TableFunc1(c)) AS D(c1)",
      "The options of following hints cannot match the name of input tables or views: \n" +
        "`D` in `LOOKUP`"
    )
  }

  @Test
  def testJoinPartitionTableWithNonExistentPartition(): Unit = {
    util.tableEnv.executeSql("""
                               |create table leftPartitionTable (
                               | a1 varchar,
                               | b1 int)
                               | partitioned by (b1) 
                               | with (
                               | 'connector' = 'values',
                               | 'bounded' = 'false',
                               | 'partition-list' = 'b1:1'
                               |)
                               |""".stripMargin)
    util.tableEnv.executeSql("""
                               |create table rightPartitionTable (
                               | a2 varchar,
                               | b2 int)
                               | partitioned by (b2) 
                               | with (
                               | 'connector' = 'values',
                               | 'bounded' = 'false',
                               | 'partition-list' = 'b2:2'
                               |)
                               |""".stripMargin)
    // partition 'b2 = 3' not exists.
    util.verifyExecPlan(
      """
        |SELECT * FROM leftPartitionTable, rightPartitionTable WHERE b1 = 1 AND b2 = 3 AND a1 = a2
        |""".stripMargin)
  }
}
