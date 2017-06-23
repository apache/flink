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

package org.apache.flink.table.api.scala.batch.table

import java.sql.Timestamp
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.batch.utils.TableProgramsCollectionTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.TestBaseUtils.compareResultAsText
import org.apache.flink.types.Row
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class InITCase(configMode: TableConfigMode)
  extends TableProgramsCollectionTestBase(configMode) {

  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env)

  @Test
  def testSqlInOperator(): Unit = {
    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T1", ds1)
    val expected = "1,Hi\n16,Comment#10\n17,Comment#11\n" +
      "18,Comment#12\n19,Comment#13\n20,Comment#14\n21,Comment#15"
    val sqlQuery = "SELECT a, c FROM T1 WHERE b IN (SELECT b FROM T1 WHERE b = 6 OR b = 1)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlNotInOperator(): Unit = {
    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T2", ds1)
    val expected = "10,Comment#4\n11,Comment#5\n12,Comment#6\n" +
      "13,Comment#7\n14,Comment#8\n15,Comment#9\n" +
      "2,Hello\n3,Hello world\n4,Hello world, how are you?\n5,I am fine.\n" +
      "6,Luke Skywalker\n7,Comment#1\n8,Comment#2\n9,Comment#3\n"
    val sqlQuery = "SELECT a, c FROM T2 WHERE b NOT IN (SELECT b FROM T2 WHERE b = 6 OR b = 1)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInOperatorWithDate(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T3", ds1)
    val expected = "14,Comment#8\n15,Comment#9\n16,Comment#10\n17,Comment#11\n" +
      "18,Comment#12\n19,Comment#13\n20,Comment#14\n21,Comment#15"
    val sqlQuery = "SELECT a, e FROM T3 WHERE b IN (SELECT b FROM T3 WHERE a = 16)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInOperatorWithTime(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T4", ds1)
    val expected = "16,Comment#10\n2,Hello\n9,Comment#3"
    val sqlQuery = "SELECT a, e FROM T4 WHERE c IN (SELECT c FROM T4 WHERE a = 16)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInOperatorWithTimestamp(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T5", ds1)
    val expected = "16,Comment#10"
    val sqlQuery = "SELECT a, e FROM T5 WHERE d IN (SELECT d FROM T5 WHERE a = 16)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInOperatorWithFields(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T6", ds1)
    val expected = "1,1,0,Hallo,1\n2,2,1,Hallo Welt,2\n2,3,2,Hallo Welt wie,1\n" +
      "3,4,3,Hallo Welt wie gehts?,2\n5,11,10,GHI,1\n5,12,11,HIJ,3\n5,13,12,IJK,3\n" +
      "5,14,13,JKL,2\n5,15,14,KLM,2"
    val sqlQuery = "SELECT a, b, c, d, e FROM T6 WHERE a IN (c, b, 5)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInNestedTuples(): Unit = {
    val ds1 = CollectionDataSets.getSmall2NestedTupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    tEnv.registerTable("T7", ds1)
    val expected = "(3,3),three,(3,3)"
    val sqlQuery = "SELECT a, b, c FROM T7 WHERE c IN (a)";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testInSubqueryCorrelatedNestedTuples(): Unit = {
    val ds1 = CollectionDataSets.getSmall2NestedTupleDataSet(env)
      .toTable(tEnv, 'a, 'b, 'c)
    val subquery: Table = ds1.where('b === "two").select('a).as("a1")
    val subqueryIn = ds1.select("*").where('c.in(subquery))
    val result = subqueryIn.toDataSet[Row].collect()
    val expected = "(1,1),one,(2,2)\n"
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testInSubqueryCorrelatedWithTimeStamp(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    val subquery: Table = ds1.where('a === 6).select('d)
    val subqueryIn = ds1.select("*").where('d.in(subquery))
    val result = subqueryIn.toDataSet[Row].collect()
    val expected = "6,1984-07-05,23:01:01,1984-07-05 23:01:01.105,Luke Skywalker\n"
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInInSelect(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T8", ds1)
    val expected = "true,Comment#4"
    val sqlQuery = "SELECT * FROM (SELECT d IN ('1972-02-22 07:12:00.333') as d2, e FROM T8)" +
      " as X WHERE d2=true";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testSqlInInSelectFields(): Unit = {
    val ds1 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    tEnv.registerTable("T9", ds1)
    val expected = "true,BCD\ntrue,Hallo Welt wie\ntrue,Hallo Welt wie gehts?\n"
    val sqlQuery = "SELECT * FROM (SELECT c IN (a, b, 5) as c2, d FROM T9) as X WHERE c2=true";
    val result = tEnv.sql(sqlQuery).toDataSet[Row].collect();
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testInInSelect(): Unit = {
    val ds1 = CollectionDataSets.getSmall3TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c)
    val subquery: Table = ds1.select('b.in(Timestamp.valueOf("1972-02-22 07:12:00.333"))).as("b2")
    val resultsSimple = subquery.toDataSet[Row].collect()
    val expected = "false\nfalse\ntrue\n"
    compareResultAsText(resultsSimple.asJava, expected)
  }

  @Test
  def testInSubqueryWithTimeStamp(): Unit = {
    val ds1 = CollectionDataSets.getSmall3TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c)
    val ds2 = CollectionDataSets.get5TupleDataSetOfDateTimeTimestamp(env)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
    val subquery: Table = ds1.where('a === 2).select('b).as("b2")
    val subqueryIn = ds2.where('d.in(subquery))
    val resultsSimple = subqueryIn.toDataSet[Row].collect()
    val expected = "10,1972-02-22,07:12:00,1972-02-22 07:12:00.333,Comment#4\n"
    compareResultAsText(resultsSimple.asJava, expected)
  }

  @Test
  def testInSubqueryCorrelated(): Unit = {
    val ds1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val subquery: Table = ds1.where('a === 6).select('c)
    val subqueryIn = ds1.select("*").where('c.in(subquery))
    val result = subqueryIn.toDataSet[Row].collect()
    val expected = "6,3,Luke Skywalker\n"
    compareResultAsText(result.asJava, expected)
  }

  @Test
  def testInSubquery(): Unit = {
    val ds1 = CollectionDataSets.getSmall3TupleDataSet(env).toTable(tEnv, 'd, 'e, 'f)
    val ds2 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv, 'a, 'b, 'c)
    val subquery: Table = ds1.where('d === 1).select('e)
    val subqueryIn = ds2.where('b.in(subquery))
    val resultsSimple = subqueryIn.toDataSet[Row].collect()
    val expected = "1,1,Hi\n"
    compareResultAsText(resultsSimple.asJava, expected)
  }

}

