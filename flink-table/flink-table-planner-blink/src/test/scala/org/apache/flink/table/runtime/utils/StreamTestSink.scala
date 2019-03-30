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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.{TableConfig, Types}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.sinks.{AppendStreamTableSink, BatchTableSink}
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.apache.flink.table.util.BaseRowTestUtil
import org.apache.flink.types.Row

import _root_.java.util.TimeZone
import _root_.java.util.concurrent.atomic.AtomicInteger

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer

object StreamTestSink {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private[utils] val idCounter: AtomicInteger = new AtomicInteger(0)

  private[utils] val globalResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, ArrayBuffer[String]]]
  private[utils] val globalRetractResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, ArrayBuffer[String]]]
  private[utils] val globalUpsertResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, mutable.Map[String, String]]]

  private[utils] def getNewSinkId: Int = {
    val idx = idCounter.getAndIncrement()
    this.synchronized{
      globalResults.put(idx, mutable.HashMap.empty[Int, ArrayBuffer[String]])
      globalRetractResults.put(idx, mutable.HashMap.empty[Int, ArrayBuffer[String]])
      globalUpsertResults.put(idx, mutable.HashMap.empty[Int, mutable.Map[String, String]])
    }
    idx
  }

  def clear(): Unit = {
    globalResults.clear()
    globalRetractResults.clear()
    globalUpsertResults.clear()
  }
}

abstract class AbstractExactlyOnceSink[T] extends RichSinkFunction[T] with CheckpointedFunction {
  protected var resultsState: ListState[String] = _
  protected var localResults: ArrayBuffer[String] = _
  protected val idx: Int = StreamTestSink.getNewSinkId

  protected var globalResults: mutable.Map[Int, ArrayBuffer[String]]= _
  protected var globalRetractResults: mutable.Map[Int, ArrayBuffer[String]] = _
  protected var globalUpsertResults: mutable.Map[Int, mutable.Map[String, String]] = _

  override def initializeState(context: FunctionInitializationContext): Unit = {
    resultsState = context.getOperatorStateStore
      .getListState(new ListStateDescriptor[String]("sink-results", Types.STRING))

    localResults = mutable.ArrayBuffer.empty[String]

    if (context.isRestored) {
      for (value <- resultsState.get().asScala) {
        localResults += value
      }
    }

    val taskId = getRuntimeContext.getIndexOfThisSubtask
    StreamTestSink.synchronized(
      StreamTestSink.globalResults(idx) += (taskId -> localResults)
    )
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    resultsState.clear()
    for (value <- localResults) {
      resultsState.add(value)
    }
  }

  protected def clearAndStashGlobalResults(): Unit = {
    if (globalResults == null) {
      StreamTestSink.synchronized{
        globalResults = StreamTestSink.globalResults.remove(idx).get
        globalRetractResults = StreamTestSink.globalRetractResults.remove(idx).get
        globalUpsertResults = StreamTestSink.globalUpsertResults.remove(idx).get
      }
    }
  }

  protected def getResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalResults.foreach {
      case (_, list) => result ++= list
    }
    result.toList
  }
}

final class TestingAppendBaseRowSink(
    rowTypeInfo: BaseRowTypeInfo, tz: TimeZone)
  extends AbstractExactlyOnceSink[BaseRow] {

  def this(rowTypeInfo: BaseRowTypeInfo) {
    this(rowTypeInfo, TimeZone.getTimeZone("UTC"))
  }

  def invoke(value: BaseRow): Unit = localResults +=
    BaseRowTestUtil.baseRowToString(value, rowTypeInfo, tz)

  def getAppendResults: List[String] = getResults

}

final class TestingAppendSink(tz: TimeZone) extends AbstractExactlyOnceSink[Row] {
  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }
  def invoke(value: Row): Unit = localResults += TestSinkUtil.rowToString(value, tz)
  def getAppendResults: List[String] = getResults
}

final class TestingAppendTableSink(tz: TimeZone) extends AppendStreamTableSink[Row]
  with BatchTableSink[Row]{
  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _
  var sink = new TestingAppendSink(tz)
  var outputFormat = new TestingOutputFormat[Row](tz)

  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  override def emitDataStream(dataStream: DataStream[Row]): DataStreamSink[Row] = {
    dataStream.addSink(sink).name("TestingAppendTableSink")
        .setParallelism(dataStream.getParallelism)
  }

  override def emitBoundedStream(
      boundedStream: DataStream[Row],
      tableConfig: TableConfig,
      executionConfig: ExecutionConfig): DataStreamSink[Row] = {
    boundedStream.writeUsingOutputFormat(outputFormat).name("appendTableSink")
  }

  override def getOutputType: TypeInformation[Row] = new RowTypeInfo(fTypes, fNames)

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TestingAppendTableSink = {
    val copy = new TestingAppendTableSink(tz)
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy.outputFormat = outputFormat
    copy.sink = sink
    copy
  }

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  def getAppendResults: List[String] = sink.getAppendResults

  def getResults: List[String] = outputFormat.getResults
}

class TestingOutputFormat[T](tz: TimeZone)
  extends OutputFormat[T] {

  val index: Int = StreamTestSink.getNewSinkId
  var localRetractResults: ArrayBuffer[String] = _

  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  protected var globalResults: mutable.Map[Int, ArrayBuffer[String]] = _

  def configure(var1: Configuration): Unit = {}

  def open(taskNumber: Int, numTasks: Int): Unit = {
    localRetractResults = mutable.ArrayBuffer.empty[String]
    StreamTestSink.synchronized{
      StreamTestSink.globalResults(index) += (taskNumber -> localRetractResults)
    }
  }

  def writeRecord(value: T): Unit = localRetractResults += { value match {
    case r: Row => TestSinkUtil.rowToString(r, tz)
    case tp: JTuple2[java.lang.Boolean, Row]  =>
      "(" + tp.f0.toString + "," + TestSinkUtil.rowToString(tp.f1, tz) + ")"
    case _ => ""
  }}

  def close(): Unit = {}

  protected def clearAndStashGlobalResults(): Unit = {
    if (globalResults == null) {
      StreamTestSink.synchronized{
        globalResults = StreamTestSink.globalResults.remove(index).get
      }
    }
  }

  def getResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalResults.foreach {
      case (_, list) => result ++= list
    }
    result.toList
  }
}
