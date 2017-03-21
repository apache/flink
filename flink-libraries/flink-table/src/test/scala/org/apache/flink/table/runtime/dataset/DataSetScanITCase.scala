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

package org.apache.flink.table.runtime.dataset

import java.util

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.{ExecutionEnvironment => JExecutionEnvironment}
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.java.batch.TableEnvironmentTest._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.batch.utils.TableProgramsClusterTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class DataSetScanITCase(
                         mode: TestExecutionMode,
                         configMode: TableConfigMode)
  extends TableProgramsClusterTestBase(mode, configMode) {

  @Test
  def testAsFromPojo() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new util.ArrayList[SmallPojo]
    data.add(new SmallPojo("Peter", 28, 4000.00, "Sales"))
    data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering"))
    data.add(new SmallPojo("Lucy", 42, 6000.00, "HR"))

    val table = tableEnv.fromDataSet(env.fromCollection(data),
      "department AS a, " +
        "age AS b, " +
        "salary AS c, " +
        "name AS d").select("a, b, c, d")

    val ds = tableEnv.toDataSet(table, classOf[Row])

    val results = ds.collect
    val expected = "Sales,28,4000.0,Peter\n" + "Engineering,56,10000.0,Anna\n" +
      "HR,42,6000.0,Lucy\n"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsFromAndToPojo() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new util.ArrayList[SmallPojo]
    data.add(new SmallPojo("Peter", 28, 4000.00, "Sales"))
    data.add(new SmallPojo("Anna", 56, 10000.00, "Engineering"))
    data.add(new SmallPojo("Lucy", 42, 6000.00, "HR"))

    val table = tableEnv.fromDataSet(env.fromCollection(data),
      "department AS a, " + "age AS b, " + "salary AS c, " + "name AS d")
      .select("a, b, c, d")

    val ds = tableEnv.toDataSet(table, classOf[SmallPojo2])
    val results = ds.collect
    val expected = "Sales,28,4000.0,Peter\n" + "Engineering,56,10000.0,Anna\n" +
      "HR,42,6000.0,Lucy\n"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsFromAndToPrivateFieldPojo() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new util.ArrayList[PrivateSmallPojo]
    data.add(new PrivateSmallPojo("Peter", 28, 4000.00, "Sales"))
    data.add(new PrivateSmallPojo("Anna", 56, 10000.00, "Engineering"))
    data.add(new PrivateSmallPojo("Lucy", 42, 6000.00, "HR"))

    val table = tableEnv.fromDataSet(env.fromCollection(data),
      "department AS a, " + "age AS b, " + "salary AS c, " + "name AS d")
      .select("a, b, c, d")
    val ds = tableEnv.toDataSet(table, classOf[PrivateSmallPojo2])
    val results = ds.collect
    val expected = "Sales,28,4000.0,Peter\n" + "Engineering,56,10000.0,Anna\n" +
      "HR,42,6000.0,Lucy\n"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsFromAndToTuple() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val table = tableEnv.fromDataSet(
      CollectionDataSets.get3TupleDataSet(env), "a, b, c")
      .select("a, b, c")

    val ti = new TupleTypeInfo[Tuple3[
      Integer,
      java.lang.Long,
      java.lang.String]](
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    val ds = tableEnv.toDataSet(table, ti)
    val results = ds.collect

    val expected = "(1,1,Hi)\n" + "(2,2,Hello)\n" + "(3,2,Hello world)\n" +
      "(4,3,Hello world, how are you?)\n" + "(5,3,I am fine.)\n" + "(6,3,Luke Skywalker)\n" +
      "(7,4,Comment#1)\n" + "(8,4,Comment#2)\n" + "(9,4,Comment#3)\n" + "(10,4,Comment#4)\n" +
      "(11,5,Comment#5)\n" + "(12,5,Comment#6)\n" + "(13,5,Comment#7)\n" +
      "(14,5,Comment#8)\n" + "(15,5,Comment#9)\n" + "(16,6,Comment#10)\n" +
      "(17,6,Comment#11)\n" + "(18,6,Comment#12)\n" + "(19,6,Comment#13)\n" +
      "(20,6,Comment#14)\n" + "(21,6,Comment#15)\n";
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsFromPrivateFieldsPojo() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new util.ArrayList[PrivateSmallPojo]
    data.add(new PrivateSmallPojo("Peter", 28, 4000.00, "Sales"))
    data.add(new PrivateSmallPojo("Anna", 56, 10000.00, "Engineering"))
    data.add(new PrivateSmallPojo("Lucy", 42, 6000.00, "HR"))
    val table = tableEnv.fromDataSet(env.fromCollection(data),
      "department AS a, " +
        "age AS b, " +
        "salary AS c, " +
        "name AS d").select("a, b, c, d")

    val ds = tableEnv.toDataSet(table, classOf[Row])
    val results = ds.collect
    val expected = "Sales,28,4000.0,Peter\n" + "Engineering,56,10000.0,Anna\n" +
      "HR,42,6000.0,Lucy\n"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsFromTuple() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)
    val table = tableEnv.fromDataSet(CollectionDataSets.get3TupleDataSet(env), "a, b, c")
      .select("a, b, c")

    val ds = tableEnv.toDataSet(table, classOf[Row])
    val results = ds.collect

    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" +
      "4,3,Hello world, how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" +
      "7,4,Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" +
      "10,4,Comment#4\n" + "11,5,Comment#5\n" + "12,5,Comment#6\n" +
      "13,5,Comment#7\n" + "14,5,Comment#8\n" +
      "15,5,Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" +
      "18,6,Comment#12\n" + "19,6,Comment#13\n" +
      "20,6,Comment#14\n" + "21,6,Comment#15\n"

    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsFromTupleToPojo(): Unit = {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new util.ArrayList[Tuple4[String, Integer, Double, String]]
    data.add(new Tuple4[String, Integer, Double, String]("Rofl", 1, 1.0, "Hi"))
    data.add(new Tuple4[String, Integer, Double, String]("lol", 2, 1.0, "Hi"))
    data.add(new Tuple4[String, Integer, Double, String]("Test me", 4, 3.33, "Hello world"))

    val table = tEnv.fromDataSet(env.fromCollection(data), "q, w, e, r")
      .select("q as a, w as b, e as c, r as d")

    val ds = tEnv.toDataSet(table, classOf[SmallPojo2])
    val results = ds.collect
    val expected = "Rofl,1,1.0,Hi\n" + "lol,2,1.0,Hi\n" + "Test me,4,3.33,Hello world\n"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testAsWithPojoAndGenericTypes() {
    val env = JExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = new util.ArrayList[PojoWithGeneric]
    data.add(new PojoWithGeneric(
      "Peter", 28, new util.HashMap[String, String], new util.ArrayList[String]))

    val hm1 = new util.HashMap[String, String]
    hm1.put("test1", "test1")
    data.add(new PojoWithGeneric(
      "Anna", 56, hm1, new util.ArrayList[String]))

    val hm2 = new util.HashMap[String, String]
    hm2.put("abc", "cde")
    data.add(new PojoWithGeneric("Lucy", 42, hm2, new util.ArrayList[String]))

    val table = tableEnv.fromDataSet(env.fromCollection(data),
      "name AS a, " +
        "age AS b, " +
        "generic AS c, " +
        "generic2 AS d")
      .select("a, b, c, c as c2, d")
      .select("a, b, c, c === c2, d")

    val ds = tableEnv.toDataSet(table, classOf[Row])
    val results = ds.collect
    val expected = "Peter,28,{},true,[]\n" + "Anna,56,{test1=test1},true,[]\n" +
      "Lucy,42,{abc=cde},true,[]\n"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testToTableFromCaseClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t = env.fromCollection(data)
      .toTable(tEnv, 'a, 'b, 'c, 'd)
      .select('a, 'b, 'c, 'd)

    val expected: String =
      "Peter,28,4000.0,Sales\n" +
        "Anna,56,10000.0,Engineering\n" +
        "Lucy,42,6000.0,HR\n"
    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testToTableFromAndToCaseClass(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env, config)

    val data = List(
      SomeCaseClass("Peter", 28, 4000.00, "Sales"),
      SomeCaseClass("Anna", 56, 10000.00, "Engineering"),
      SomeCaseClass("Lucy", 42, 6000.00, "HR"))

    val t = env.fromCollection(data)
      .toTable(tEnv, 'a, 'b, 'c, 'd)
      .select('a, 'b, 'c, 'd)

    val expected: String =
      "SomeCaseClass(Peter,28,4000.0,Sales)\n" +
        "SomeCaseClass(Anna,56,10000.0,Engineering)\n" +
        "SomeCaseClass(Lucy,42,6000.0,HR)\n"
    val results = t.toDataSet[SomeCaseClass].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

case class SomeCaseClass(name: String, age: Int, salary: Double, department: String) {
  def this() {
    this("", 0, 0.0, "")
  }
}
