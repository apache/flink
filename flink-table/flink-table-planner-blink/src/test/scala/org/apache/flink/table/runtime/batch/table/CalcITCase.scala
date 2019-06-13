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

package org.apache.flink.table.runtime.batch.table

import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.TestData._
import org.apache.flink.table.runtime.utils.{BatchTestBase, CollectionBatchExecTable}
import org.apache.flink.test.util.TestBaseUtils

import org.junit.{Before, Test}

import scala.collection.JavaConverters._

class CalcITCase extends BatchTestBase {

  @Before
  def before(): Unit = {
    registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)
  }

  @Test
  def testSimpleSelectAll(): Unit = {
    val t = tEnv.scan("Table3").select('a, 'b, 'c)
    checkTableResult(t, data3)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c").select('a, 'b, 'c)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
        "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
        "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
        "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
        "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
        "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
        .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
        .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
        "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
        "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSimpleSelectRenameAll(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv)
        .select('_1 as 'a, '_2 as 'b, '_3 as 'c)
        .select('a, 'b)

    val expected = "1,1\n" + "2,2\n" + "3,2\n" + "4,3\n" + "5,3\n" + "6,3\n" + "7,4\n" +
        "8,4\n" + "9,4\n" + "10,4\n" + "11,5\n" + "12,5\n" + "13,5\n" + "14,5\n" + "15,5\n" +
        "16,6\n" + "17,6\n" + "18,6\n" + "19,6\n" + "20,6\n" + "21,6\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testSelectStar(): Unit = {
    val t = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c").select('*)

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
        "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
        "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" +
        "11,5,Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" +
        "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" +
        "19,6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(t)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter(false)

    val expected = "\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter(true)
    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
        "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
        "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
        "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
        "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
        "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")
    val filterDs = ds.filter( 'c.like("%world%") )

    val expected = "3,2,Hello world\n" + "4,3,Hello world, how are you?\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter( 'a % 2 === 0 )

    val expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" +
        "6,3,Luke Skywalker\n" + "8,4," + "Comment#2\n" + "10,4,Comment#4\n" +
        "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
        "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testNotEquals(): Unit = {
    val ds = CollectionBatchExecTable.get3TupleDataSet(tEnv, "a, b, c")

    val filterDs = ds.filter( 'a % 2 !== 0)
    val expected = "1,1,Hi\n" + "3,2,Hello world\n" +
        "5,3,I am fine.\n" + "7,4,Comment#1\n" + "9,4,Comment#3\n" +
        "11,5,Comment#5\n" + "13,5,Comment#7\n" + "15,5,Comment#9\n" +
        "17,6,Comment#11\n" + "19,6,Comment#13\n" + "21,6,Comment#15\n"
    val results = executeQuery(filterDs)
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
