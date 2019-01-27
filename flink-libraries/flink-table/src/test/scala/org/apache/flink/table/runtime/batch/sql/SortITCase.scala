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

package org.apache.flink.table.runtime.batch.sql

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableEnvironment, TableException}
import org.apache.flink.table.runtime.utils.SortTestUtils._
import org.apache.flink.table.runtime.utils.{CommonTestData, RowsCollectTableSink}
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID
import org.junit._
import org.junit.rules.ExpectedException

import scala.collection.JavaConverters._

@Ignore
class SortITCase extends AbstractTestBase {

  private val expectedException = ExpectedException.none()
  private val conf = BatchTestBase.initConfigForTest(new TableConfig)

  @Before
  def setUp(): Unit = {
    conf.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_RANGE_ENABLED, true)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
  }

  @Rule
  def thrown: ExpectedException = expectedException

  private def getExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env
  }

  @Test(expected = classOf[TableException])
  def testOrderByWithOffset(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)

    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 OFFSET 2"

    val csvTable = CommonTestData.get3Source()
    tEnv.registerTableSource("MyTable", csvTable)

    tEnv.sqlQuery(sqlQuery).collect()
  }

  @Test
  def testOrderByMultipleFields(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC, _2 DESC"

    val csvTable = CommonTestData.get3Source()
    tEnv.registerTableSource("MyTable", csvTable)

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) => (
        - x.productElement(0).asInstanceOf[Int],
        - x.productElement(1).asInstanceOf[Long]))

    val expected = sortExpectedly(tupleDataSetStrings)
    val tableSink = new RowsCollectTableSink
    val typeInformation = TypeExtractor.getForClass(classOf[Seq[Row]])
    val typeSerializer = typeInformation.createSerializer(env.getConfig)
    val id = new AbstractID().toString
    tableSink.init(typeSerializer, id)
    tEnv.sqlQuery(sqlQuery).writeToSink(tableSink)

    val result = getOrderedRows(tEnv, typeSerializer, id)
    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOppositeOrderByMultipleFields(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 ASC, _2 DESC"

    val csvTable = CommonTestData.get3Source()
    tEnv.registerTableSource("MyTable", csvTable)

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) => (
        x.productElement(0).asInstanceOf[Int],
        -x.productElement(1).asInstanceOf[Long]))

    val expected = sortExpectedly(tupleDataSetStrings)
    val tableSink = new RowsCollectTableSink
    val typeInformation = TypeExtractor.getForClass(classOf[Seq[Row]])
    val typeSerializer = typeInformation.createSerializer(env.getConfig)
    val id = new AbstractID().toString
    tableSink.init(typeSerializer, id)
    tEnv.sqlQuery(sqlQuery).writeToSink(tableSink)

    val result = getOrderedRows(tEnv, typeSerializer, id)
    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByLastSingleField(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    val sqlQuery = "SELECT * FROM MyTable ORDER BY _3 DESC"

    val csvTable = CommonTestData.get3Source()
    tEnv.registerTableSource("MyTable", csvTable)

    implicit def tupleOrdering[T <: Product] = Ordering.by(
      (x : T) => x.productElement(2).asInstanceOf[String]).reverse

    val expected = sortExpectedly(tupleDataSetStrings)
    val tableSink = new RowsCollectTableSink
    val typeInformation = TypeExtractor.getForClass(classOf[Seq[Row]])
    val typeSerializer = typeInformation.createSerializer(env.getConfig)
    val id = new AbstractID().toString
    tableSink.init(typeSerializer, id)
    tEnv.sqlQuery(sqlQuery).writeToSink(tableSink)

    val result = getOrderedRows(tEnv, typeSerializer, id)
    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOppositeOrderByLastTwoFields(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    val sqlQuery = "SELECT * FROM MyTable ORDER BY _5 DESC, _4 ASC"

    val csvTable = CommonTestData.get5Source()
    tEnv.registerTableSource("MyTable", csvTable)

    def tupleOrdering[T <: Product] = Ordering.by((x : T) => (
        -x.productElement(4).asInstanceOf[Long],
        x.productElement(3).asInstanceOf[String]))

    val dataSet: List[Product] =  CommonTestData.get5Data().toList
    val expected = sortExpectedly(dataSet)(tupleOrdering)
    val tableSink = new RowsCollectTableSink
    val typeInformation = TypeExtractor.getForClass(classOf[Seq[Row]])
    val typeSerializer = typeInformation.createSerializer(env.getConfig)
    val id = new AbstractID().toString
    tableSink.init(typeSerializer, id)
    tEnv.sqlQuery(sqlQuery).writeToSink(tableSink)

    val result = getOrderedRows(tEnv, typeSerializer, id)
    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }

  @Test
  def testOrderByRepeatedFields(): Unit = {
    val env = getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
    tEnv.getConfig.setSubsectionOptimization(true)
    val sqlQuery = "SELECT * FROM MyTable ORDER BY _1 DESC, _1 DESC"

    val csvTable = CommonTestData.get3Source()
    tEnv.registerTableSource("MyTable", csvTable)

    implicit def tupleOrdering[T <: Product] = Ordering.by((x : T) =>
        - x.productElement(0).asInstanceOf[Int])

    val expected = sortExpectedly(tupleDataSetStrings)
    val tableSink = new RowsCollectTableSink
    val typeInformation = TypeExtractor.getForClass(classOf[Seq[Row]])
    val typeSerializer = typeInformation.createSerializer(env.getConfig)
    val id = new AbstractID().toString
    tableSink.init(typeSerializer, id)
    tEnv.sqlQuery(sqlQuery).writeToSink(tableSink)
    val result = getOrderedRows(tEnv, typeSerializer, id)
    TestBaseUtils.compareOrderedResultAsText(result.asJava, expected)
  }
}
