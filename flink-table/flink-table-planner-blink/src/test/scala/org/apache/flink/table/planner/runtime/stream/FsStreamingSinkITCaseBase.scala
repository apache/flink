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

package org.apache.flink.table.planner.runtime.stream

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.runtime.state.CheckpointListener
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.filesystem.DefaultPartTimeExtractor.{toLocalDateTime, toMills}
import org.apache.flink.table.filesystem.FileSystemOptions._
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestSinkUtil}
import org.apache.flink.types.Row

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists

import org.junit.Assert.assertEquals
import org.junit.rules.Timeout
import org.junit.{Assert, Before, Rule, Test}

import java.io.File
import java.net.URI

import scala.collection.JavaConversions._
import scala.collection.Seq

/**
  * Streaming sink ITCase base, test checkpoint.
  */
abstract class FsStreamingSinkITCaseBase extends StreamingTestBase {

  @Rule
  def timeoutPerTest: Timeout = Timeout.seconds(20)

  protected var resultPath: String = _

  private val data = Seq(
    Row.of(Integer.valueOf(1), "a", "b", "2020-05-03", "7"),
    Row.of(Integer.valueOf(2), "p", "q", "2020-05-03", "8"),
    Row.of(Integer.valueOf(3), "x", "y", "2020-05-03", "9"),
    Row.of(Integer.valueOf(4), "x", "y", "2020-05-03", "10"),
    Row.of(Integer.valueOf(5), "x", "y", "2020-05-03", "11"))

  @Before
  override def before(): Unit = {
    super.before()
    resultPath = tempFolder.newFolder().toURI.toString

    env.setParallelism(1)
    env.enableCheckpointing(100)

    val stream = new DataStream(env.getJavaEnv.addSource(
      new FiniteTestSource(data),
      new RowTypeInfo(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)))

    tEnv.createTemporaryView("my_table", stream, $("a"), $("b"), $("c"), $("d"), $("e"))
  }

  def additionalProperties(): Array[String] = Array()

  @Test
  def testNonPart(): Unit = {
    test(false)
  }

  @Test
  def testPart(): Unit = {
    test(true)
    val basePath = new File(new URI(resultPath).getPath, "d=2020-05-03")
    Assert.assertEquals(5, basePath.list().length)
    Assert.assertTrue(new File(new File(basePath, "e=7"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=8"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=9"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=10"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=11"), "_MY_SUCCESS").exists())
  }

  private def test(partition: Boolean): Unit = {
    val dollar = '$'
    val ddl = s"""
                 |create table sink_table (
                 |  a int,
                 |  b string,
                 |  c string,
                 |  d string,
                 |  e string
                 |)
                 |${if (partition) "partitioned by (d, e)" else ""}
                 |with (
                 |  'connector' = 'filesystem',
                 |  'path' = '$resultPath',
                 |  '${PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()}' =
                 |      '${dollar}d ${dollar}e:00:00',
                 |  '${SINK_PARTITION_COMMIT_DELAY.key()}' = '1h',
                 |  '${SINK_PARTITION_COMMIT_POLICY_KIND.key()}' = 'success-file',
                 |  '${SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key()}' = '_MY_SUCCESS',
                 |  ${additionalProperties().mkString(",\n")}
                 |)
       """.stripMargin
    tEnv.executeSql(ddl)

    val tableResult = tEnv.sqlQuery("select * from my_table").executeInsert("sink_table")
    tableResult.getJobClient.get()
      .getJobExecutionResult(Thread.currentThread().getContextClassLoader)
      .get()

    check(
      ddl,
      "select * from sink_table",
      data)
  }

  def check(ddl: String, sqlQuery: String, expectedResult: Seq[Row]): Unit = {
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(setting)
    tEnv.executeSql(ddl)

    val result = Lists.newArrayList(tEnv.sqlQuery(sqlQuery).execute().collect())

    assertEquals(
      expectedResult.map(TestSinkUtil.rowToString(_)).sorted,
      result.map(TestSinkUtil.rowToString(_)).sorted)
  }
}

class FiniteTestSource(elements: Iterable[Row]) extends SourceFunction[Row] with CheckpointListener{

  private var running: Boolean = true

  private var numCheckpointsComplete: Int = 0

  @throws[Exception]
  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    val lock = ctx.getCheckpointLock
    lock.synchronized {
      for (t <- elements) {
        ctx.collect(t)
        ctx.emitWatermark(new Watermark(
          toMills(toLocalDateTime(s"${t.getField(3)} ${t.getField(4)}:00:00"))))
      }
    }

    ctx.emitWatermark(new Watermark(Long.MaxValue))

    lock.synchronized {
      while (running && numCheckpointsComplete < 2) {
        lock.wait(1);
      }
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  @throws[Exception]
  override def notifyCheckpointComplete(checkpointId: Long): Unit = {
    numCheckpointsComplete += 1
  }

  @throws[Exception]
  override def notifyCheckpointAborted(checkpointId: Long): Unit = {}
}
