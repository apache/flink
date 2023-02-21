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
package org.apache.flink.table.api

import org.apache.flink.api.common.JobStatus
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.table.planner.utils.TestTableSourceSinks
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.test.util.AbstractTestBase.MINI_CLUSTER_RESOURCE
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.CollectionUtil

import _root_.java.lang.{Long => JLong}
import _root_.java.util
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.containsInAnyOrder
import org.junit.{Before, Rule, Test}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.time.Instant

@RunWith(classOf[Parameterized])
class TableITCase(tableEnvName: String, isStreaming: Boolean) extends AbstractTestBase {

  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  var tEnv: TableEnvironment = _

  private val settings = if (isStreaming) {
    EnvironmentSettings.newInstance().inStreamingMode().build()
  } else {
    EnvironmentSettings.newInstance().inBatchMode().build()
  }

  @Before
  def setup(): Unit = {
    tableEnvName match {
      case "TableEnvironment" =>
        tEnv = TableEnvironmentImpl.create(settings)
      case "StreamTableEnvironment" =>
        tEnv = StreamTableEnvironment.create(
          StreamExecutionEnvironment.getExecutionEnvironment,
          settings)
      case _ => throw new UnsupportedOperationException("unsupported tableEnvName: " + tableEnvName)
    }
    TestTableSourceSinks.createPersonCsvTemporaryTable(tEnv, "MyTable")
  }

  @Test
  def testExecute(): Unit = {
    val query =
      """
        |select id, concat(concat(`first`, ' '), `last`) as `full name`
        |from MyTable where mod(id, 2) = 0
      """.stripMargin
    val table = tEnv.sqlQuery(query)
    val tableResult = table.execute()
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      ResolvedSchema.of(
        Column.physical("id", DataTypes.INT()),
        Column.physical("full name", DataTypes.STRING())),
      tableResult.getResolvedSchema)
    val expected = util.Arrays.asList(
      Row.of(Integer.valueOf(2), "Bob Taylor"),
      Row.of(Integer.valueOf(4), "Peter Smith"),
      Row.of(Integer.valueOf(6), "Sally Miller"),
      Row.of(Integer.valueOf(8), "Kelly Williams")
    )
    // wait for data ready
    // this is just for testing, because iterator will also wait for data ready
    tableResult.await()
    val it = tableResult.collect()
    val actual = CollectionUtil.iteratorToList(it)
    // actively close the job even it is finished
    it.close()
    actual.sort(new util.Comparator[Row]() {
      override def compare(o1: Row, o2: Row): Int = {
        o1.getField(0).asInstanceOf[Int].compareTo(o2.getField(0).asInstanceOf[Int])
      }
    })
    assertEquals(expected, actual)
  }

  @Test
  def testCollectWithClose(): Unit = {
    val sourceDdl =
      """
        |create table unbounded_source (
        |  id int
        |) with (
        |  'connector' = 'datagen',
        |  'number-of-rows' = '10000',
        |  'rows-per-second' = '1' -- slow producing speed to make sure that
        |                          -- source is not finished when job is cancelled
        |)
        |""".stripMargin
    tEnv.executeSql(sourceDdl)
    val query = "select id from unbounded_source where mod(id, 2) = 0"
    val table = tEnv.sqlQuery(query)
    val tableResult = table.execute()
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    val it = tableResult.collect()
    it.close()

    // wait for mini cluster to shut down
    val jobClient = tableResult.getJobClient.get()
    val jobId = jobClient.getJobID
    MINI_CLUSTER_RESOURCE.getClusterClient.requestJobResult(jobId).get()

    assertEquals(JobStatus.CANCELED, jobClient.getJobStatus.get())
  }

  @Test
  def testExecuteWithUpdateChanges(): Unit = {
    val tableResult = tEnv.sqlQuery("select count(*) as c from MyTable").execute()
    assertTrue(tableResult.getJobClient.isPresent)
    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, tableResult.getResultKind)
    assertEquals(
      ResolvedSchema.of(Column.physical("c", DataTypes.BIGINT().notNull())),
      tableResult.getResolvedSchema)
    val expected = if (isStreaming) {
      util.Arrays.asList(
        Row.ofKind(RowKind.INSERT, JLong.valueOf(1)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(1)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(2)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(2)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(3)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(3)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(4)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(4)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(5)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(5)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(6)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(6)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(7)),
        Row.ofKind(RowKind.UPDATE_BEFORE, JLong.valueOf(7)),
        Row.ofKind(RowKind.UPDATE_AFTER, JLong.valueOf(8))
      )
    } else {
      util.Arrays.asList(Row.of(JLong.valueOf(8)))
    }
    val actual = CollectionUtil.iteratorToList(tableResult.collect())
    assertEquals(expected, actual)
  }

  @Test
  def testCollectWithMultiRowtime(): Unit = {
    tEnv.executeSql("""
                      |CREATE TABLE MyTableWithRowtime1 (
                      |  ts AS TO_TIMESTAMP_LTZ(id, 3),
                      |  WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE)
                      |LIKE MyTable""".stripMargin)
    tEnv.executeSql("""
                      |CREATE TABLE MyTableWithRowtime2 (
                      |  ts AS TO_TIMESTAMP_LTZ(id, 3),
                      |  WATERMARK FOR ts AS ts - INTERVAL '1' MINUTE)
                      |LIKE MyTable""".stripMargin)

    val tableResult =
      tEnv.executeSql("""
                        |SELECT MyTableWithRowtime1.ts, MyTableWithRowtime2.ts
                        |FROM MyTableWithRowtime1, MyTableWithRowtime2
                        |WHERE
                        |  MyTableWithRowtime1.first = MyTableWithRowtime2.first AND
                        |  MyTableWithRowtime1.ts = MyTableWithRowtime2.ts""".stripMargin)

    val expected =
      for (i <- 1 to 8)
        yield Row.ofKind(RowKind.INSERT, Instant.ofEpochMilli(i), Instant.ofEpochMilli(i))

    val actual = CollectionUtil.iteratorToList(tableResult.collect())
    assertThat(actual, containsInAnyOrder(expected: _*))
  }
}

object TableITCase {
  @Parameterized.Parameters(name = "{0}:isStream={1}")
  def parameters(): util.Collection[Array[_]] = {
    util.Arrays.asList(
      Array("TableEnvironment", true),
      Array("TableEnvironment", false),
      Array("StreamTableEnvironment", true)
    )
  }
}
