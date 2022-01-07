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

import org.apache.flink.api.common.state.CheckpointListener
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.connector.file.table.FileSystemConnectorOptions._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.data.TimestampData
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestSinkUtil}
import org.apache.flink.types.Row
import org.apache.flink.util.CollectionUtil
import org.junit.Assert.assertEquals
import org.junit.rules.Timeout
import org.junit.{Assert, Before, Rule, Test}

import java.io.File
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.collection.JavaConversions._
import scala.collection.Seq

/**
  * Streaming sink ITCase base, test checkpoint.
  */
abstract class FsStreamingSinkITCaseBase extends StreamingTestBase {

  @Rule
  def timeoutPerTest: Timeout = Timeout.seconds(240)

  protected var resultPath: String = _

  // iso date
  private val data: Seq[Row] = Seq(
    Row.of(Integer.valueOf(1), "a", "b", "05-03-2020", "07"),
    Row.of(Integer.valueOf(2), "p", "q", "05-03-2020", "08"),
    Row.of(Integer.valueOf(3), "x", "y", "05-03-2020", "09"),
    Row.of(Integer.valueOf(4), "x", "y", "05-03-2020", "10"),
    Row.of(Integer.valueOf(5), "x", "y", "05-03-2020", "11"))

  // basic iso date
  private val data2 = Seq(
    Row.of(Integer.valueOf(1), "a", "b", "20200503", "07"),
    Row.of(Integer.valueOf(2), "p", "q", "20200503", "08"),
    Row.of(Integer.valueOf(3), "x", "y", "20200503", "09"),
    Row.of(Integer.valueOf(4), "x", "y", "20200504", "10"),
    Row.of(Integer.valueOf(5), "x", "y", "20200504", "11"))

  @Before
  override def before(): Unit = {
    super.before()

    env.setParallelism(1)
    env.enableCheckpointing(100)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
  }

  def additionalProperties(): Array[String] = Array()

  @Test
  def testNonPart(): Unit = {
    testPartitionCustomFormatDate(partition = false)
  }

  @Test
  def testPart(): Unit = {
    testPartitionCustomFormatDate(partition = true)
    val basePath = new File(new URI(resultPath).getPath, "d=05-03-2020")
    Assert.assertEquals(5, basePath.list().length)
    Assert.assertTrue(new File(new File(basePath, "e=07"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=08"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=09"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=10"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "e=11"), "_MY_SUCCESS").exists())
  }

  @Test
  def testMetastorePolicy(): Unit = {
    thrown.expectMessage(
      "Can not configure a 'metastore' partition commit policy for a file system table." +
        " You can only configure 'metastore' partition commit policy for a hive table.")
    testPartitionCustomFormatDate(partition = true, "metastore")
  }

  @Test
  def testPartitionWithBasicDate(): Unit = {

    // create source test data stream
    val fun = (t: Row) => {
      val localDateTime = LocalDateTime.of(
        LocalDate.parse(t.getFieldAs[String](3), DateTimeFormatter.BASIC_ISO_DATE),
        LocalTime.MIDNIGHT)
      TimestampData.fromLocalDateTime(localDateTime).getMillisecond
    }

    val stream: DataStream[Row] = new DataStream(env.getJavaEnv.addSource(
      new FiniteTestSource(data2, fun),
      new RowTypeInfo(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)))

    // write out the data
    test(stream, "default", "yyyyMMdd", "$d", "d", "partition-time", "1d", data2)

    // verify that the written data is correct
    val basePath = new File(new URI(resultPath).getPath)
    Assert.assertEquals(2, basePath.list().length)
    Assert.assertTrue(new File(new File(basePath, "d=20200503"), "_MY_SUCCESS").exists())
    Assert.assertTrue(new File(new File(basePath, "d=20200504"), "_MY_SUCCESS").exists())
  }

  def testPartitionCustomFormatDate(partition: Boolean, policy: String = "success-file"): Unit = {

    val fun = (t: Row) => {
      val localDateTime = LocalDateTime.parse(s"${t.getField(3)} ${t.getField(4)}:00:00",
        DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss"))
      TimestampData.fromLocalDateTime(localDateTime).getMillisecond
    }

    val stream = new DataStream(env.getJavaEnv.addSource(
      new FiniteTestSource(data, fun),
      new RowTypeInfo(Types.INT, Types.STRING, Types.STRING, Types.STRING, Types.STRING)))

    test(stream, "default", "MM-dd-yyyy HH:mm:ss", "$d $e:00:00", if (partition) "d,e" else "",
      "process-time", "1h", data, policy)
  }

  private def test(dataStream: DataStream[Row],
                   timeExtractorKind: String,
                   timeExtractorFormatterPattern: String,
                   timeExtractorPattern: String,
                   partition: String,
                   commitTrigger: String,
                   commitDelay: String,
                   dataTest: Seq[Row],
                   policy: String = "success-file",
                   successFileName: String = "_MY_SUCCESS"
                  ): Unit = {

    resultPath = tempFolder.newFolder().toURI.toString

    tEnv.createTemporaryView("my_table", dataStream, $("a"), $("b"), $("c"), $("d"), $("e"))

    val ddl =
      s"""
         |create table sink_table (
         |  a int,
         |  b string,
         |  c string,
         |  d string,
         |  e string
         |)
         |${if (partition.nonEmpty) s"partitioned by ($partition) " else " "}
         |with (
         |  'connector' = 'filesystem',
         |  'path' = '$resultPath',
         |  '${PARTITION_TIME_EXTRACTOR_KIND.key()}' = '$timeExtractorKind',
         |${
        if (timeExtractorFormatterPattern.nonEmpty) s" '" +
          s"${PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER.key()}' = " +
          s"'$timeExtractorFormatterPattern',"
        else ""
      }
         |
         |  '${PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()}' =
         |      '$timeExtractorPattern',
         |  '${SINK_PARTITION_COMMIT_TRIGGER.key()}' = '$commitTrigger',
         |  '${SINK_PARTITION_COMMIT_DELAY.key()}' = '$commitDelay',
         |  '${SINK_PARTITION_COMMIT_POLICY_KIND.key()}' = '$policy',
         |  '${SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME.key()}' = '$successFileName',
         |  ${additionalProperties().mkString(",\n")}
         |)
       """.stripMargin
    tEnv.executeSql(ddl)

    tEnv.sqlQuery("select * from my_table").executeInsert("sink_table").await()

    check("select * from sink_table", dataTest)
  }

  def check(sqlQuery: String, expectedResult: Seq[Row]): Unit = {
    val iter = tEnv.sqlQuery(sqlQuery).execute().collect()
    val result = CollectionUtil.iteratorToList(iter)
    iter.close()

    assertEquals(
      expectedResult.map(TestSinkUtil.rowToString(_)).sorted,
      result.map(TestSinkUtil.rowToString(_)).sorted)
  }
}

class FiniteTestSource(elements: Iterable[Row], watermarkGenerator: Row => Long) extends
  SourceFunction[Row] with CheckpointListener {

  private var running: Boolean = true

  private var numCheckpointsComplete: Int = 0

  @throws[Exception]
  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    val lock = ctx.getCheckpointLock
    lock.synchronized {
      for (t <- elements) {
        ctx.collect(t)
        ctx.emitWatermark(new Watermark(watermarkGenerator(t)))
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
