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

package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.Types
import org.apache.flink.table.data.util.DataFormatConverters
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.planner.utils.RowDataTestUtil
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.sinks._
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row

import _root_.java.lang.{Boolean => JBoolean}
import _root_.java.util.TimeZone
import _root_.java.util.concurrent.atomic.AtomicInteger
import java.util

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable
import _root_.scala.collection.mutable.ArrayBuffer

object StreamTestSink {

  private[utils] val idCounter: AtomicInteger = new AtomicInteger(0)

  private[utils] val globalResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, ArrayBuffer[String]]]
  private[utils] val globalRetractResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, ArrayBuffer[String]]]
  private[utils] val globalUpsertResults =
    mutable.HashMap.empty[Int, mutable.Map[Int, mutable.Map[String, String]]]

  private[utils] def getNewSinkId: Int = {
    val idx = idCounter.getAndIncrement()
    this.synchronized {
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

  protected var globalResults: mutable.Map[Int, ArrayBuffer[String]] = _
  protected var globalRetractResults: mutable.Map[Int, ArrayBuffer[String]] = _
  protected var globalUpsertResults: mutable.Map[Int, mutable.Map[String, String]] = _

  def isInitialized: Boolean = globalResults != null

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
      StreamTestSink.synchronized {
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

final class StringSink[T] extends AbstractExactlyOnceSink[T]() {
  override def invoke(value: T) {
    localResults += value.toString
  }

  override def getResults: List[String] = super.getResults
}

final class TestingAppendRowDataSink(
    rowTypeInfo: RowDataTypeInfo, tz: TimeZone)
  extends AbstractExactlyOnceSink[RowData] {

  def this(rowTypeInfo: RowDataTypeInfo) {
    this(rowTypeInfo, TimeZone.getTimeZone("UTC"))
  }

  override def invoke(value: RowData): Unit = localResults +=
    RowDataTestUtil.rowToString(value, rowTypeInfo, tz)

  def getAppendResults: List[String] = getResults

  def getJavaAppendResults: java.util.List[String] = new util.ArrayList[String](getResults.asJava)

}

final class TestingAppendSink(tz: TimeZone) extends AbstractExactlyOnceSink[Row] {
  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  override def invoke(value: Row): Unit = localResults += TestSinkUtil.rowToString(value, tz)

  def getAppendResults: List[String] = getResults
}

final class TestingUpsertSink(keys: Array[Int], tz: TimeZone)
  extends AbstractExactlyOnceSink[(Boolean, RowData)] {

  private var upsertResultsState: ListState[String] = _
  private var localUpsertResults: mutable.Map[String, String] = _
  private var fieldTypes: Array[TypeInformation[_]] = _

  def this(keys: Array[Int]) {
    this(keys, TimeZone.getTimeZone("UTC"))
  }

  def configureTypes(fieldTypes: Array[TypeInformation[_]]): Unit = {
    this.fieldTypes = fieldTypes
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    super.initializeState(context)
    upsertResultsState = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[String]("sink-upsert-results", Types.STRING))

    localUpsertResults = mutable.HashMap.empty[String, String]

    if (context.isRestored) {
      var key: String = null
      var value: String = null
      for (entry <- upsertResultsState.get().asScala) {
        if (key == null) {
          key = entry
        } else {
          value = entry
          localUpsertResults += (key -> value)
          key = null
          value = null
        }
      }
      if (key != null) {
        throw new RuntimeException("The resultState is corrupt.")
      }
    }

    val taskId = getRuntimeContext.getIndexOfThisSubtask
    StreamTestSink.synchronized {
      StreamTestSink.globalUpsertResults(idx) += (taskId -> localUpsertResults)
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    upsertResultsState.clear()
    for ((key, value) <- localUpsertResults) {
      upsertResultsState.add(key)
      upsertResultsState.add(value)
    }
  }

  override def invoke(d: (Boolean, RowData)): Unit = {
    this.synchronized {
      val wrapRow = new GenericRowData(2)
      wrapRow.setField(0, d._1)
      wrapRow.setField(1, d._2)
      val converter =
        DataFormatConverters.getConverterForDataType(
          TypeConversions.fromLegacyInfoToDataType(
            new TupleTypeInfo(Types.BOOLEAN, new RowTypeInfo(fieldTypes: _*))))
          .asInstanceOf[DataFormatConverters.DataFormatConverter[RowData, JTuple2[JBoolean, Row]]]
      val v = converter.toExternal(wrapRow)
      val rowString = TestSinkUtil.rowToString(v.f1, tz)
      val tupleString = "(" + v.f0.toString + "," + rowString + ")"
      localResults += tupleString
      val keyString = TestSinkUtil.rowToString(Row.project(v.f1, keys), tz)
      if (v.f0) {
        localUpsertResults += (keyString -> rowString)
      } else {
        val oldValue = localUpsertResults.remove(keyString)
        if (oldValue.isEmpty) {
          throw new RuntimeException("Tried to delete a value that wasn't inserted first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
  }

  def getRawResults: List[String] = getResults

  def getUpsertResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalUpsertResults.foreach {
      case (_, map) => map.foreach(result += _._2)
    }
    result.toList
  }
}

final class TestingUpsertTableSink(val keys: Array[Int], val tz: TimeZone)
  extends UpsertStreamTableSink[RowData] {
  private var fNames: Array[String] = _
  private var fTypes: Array[TypeInformation[_]] = _
  private var sink = new TestingUpsertSink(keys, tz)
  var expectedKeys: Option[Array[String]] = None
  var expectedIsAppendOnly: Option[Boolean] = None

  def this(keys: Array[Int]) {
    this(keys, TimeZone.getTimeZone("UTC"))
  }

  override def setKeyFields(keys: Array[String]): Unit = {
    if (expectedKeys.isDefined && keys == null) {
      throw new AssertionError("Provided key fields should not be null.")
    } else if (expectedKeys.isEmpty) {
      return
    }
    val expectedStr = expectedKeys.get.sorted.mkString(",")
    val keysStr = keys.sorted.mkString(",")
    if (!expectedStr.equals(keysStr)) {
      throw new AssertionError(
        s"Provided key fields($keysStr) do not match expected keys($expectedStr)")
    }
  }

  override def setIsAppendOnly(isAppendOnly: JBoolean): Unit = {
    if (expectedIsAppendOnly.isEmpty) {
      return
    }
    if (expectedIsAppendOnly.get != isAppendOnly) {
      throw new AssertionError("Provided isAppendOnly does not match expected isAppendOnly")
    }
  }

  override def getRecordType: TypeInformation[RowData] =
    new RowDataTypeInfo(
      fTypes.map(TypeConversions.fromLegacyInfoToDataType(_).getLogicalType),
      fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def consumeDataStream(
      dataStream: DataStream[JTuple2[JBoolean, RowData]]): DataStreamSink[_] = {
    dataStream.map(new MapFunction[JTuple2[JBoolean, RowData], (Boolean, RowData)] {
      override def map(value: JTuple2[JBoolean, RowData]): (Boolean, RowData) = {
        (value.f0, value.f1)
      }
    })
      .setParallelism(dataStream.getParallelism)
      .addSink(sink)
      .name(s"TestingUpsertTableSink(keys=${
        if (keys != null) {
          "(" + keys.mkString(",") + ")"
        } else {
          "null"
        }
      })")
      .setParallelism(dataStream.getParallelism)
  }

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TestingUpsertTableSink = {
    val copy = new TestingUpsertTableSink(keys, tz)
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    sink.configureTypes(fieldTypes)
    copy.sink = sink
    copy
  }

  def getRawResults: List[String] = sink.getRawResults

  def getUpsertResults: List[String] = sink.getUpsertResults
}

final class TestingAppendTableSink(tz: TimeZone) extends AppendStreamTableSink[Row] {
  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _
  var sink = new TestingAppendSink(tz)
  var outputFormat = new TestingOutputFormat[Row](tz)

  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
    dataStream.addSink(sink).name("TestingAppendTableSink")
      .setParallelism(dataStream.getParallelism)
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

  def getResults: List[String] = sink.getAppendResults
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
    StreamTestSink.synchronized {
      StreamTestSink.globalResults(index) += (taskNumber -> localRetractResults)
    }
  }

  def writeRecord(value: T): Unit = localRetractResults += {
    value match {
      case r: Row => TestSinkUtil.rowToString(r, tz)
      case tp: JTuple2[java.lang.Boolean, Row] =>
        "(" + tp.f0.toString + "," + TestSinkUtil.rowToString(tp.f1, tz) + ")"
      case _ => ""
    }
  }

  def close(): Unit = {}

  protected def clearAndStashGlobalResults(): Unit = {
    if (globalResults == null) {
      StreamTestSink.synchronized {
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

class TestingRetractSink(tz: TimeZone)
  extends AbstractExactlyOnceSink[(Boolean, Row)] {
  protected var retractResultsState: ListState[String] = _
  protected var localRetractResults: ArrayBuffer[String] = _

  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    super.initializeState(context)
    retractResultsState = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[String]("sink-retract-results", Types.STRING))

    localRetractResults = mutable.ArrayBuffer.empty[String]

    if (context.isRestored) {
      for (value <- retractResultsState.get().asScala) {
        localRetractResults += value
      }
    }

    val taskId = getRuntimeContext.getIndexOfThisSubtask
    StreamTestSink.synchronized {
      StreamTestSink.globalRetractResults(idx) += (taskId -> localRetractResults)
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    super.snapshotState(context)
    retractResultsState.clear()
    for (value <- localRetractResults) {
      retractResultsState.add(value)
    }
  }

  override def invoke(v: (Boolean, Row)): Unit = {
    this.synchronized {
      val tupleString = "(" + v._1.toString + "," + TestSinkUtil.rowToString(v._2, tz) + ")"
      localResults += tupleString
      val rowString = TestSinkUtil.rowToString(v._2, tz)
      if (v._1) {
        localRetractResults += rowString
      } else {
        val index = localRetractResults.indexOf(rowString)
        if (index >= 0) {
          localRetractResults.remove(index)
        } else {
          throw new RuntimeException("Tried to retract a value that wasn't added first. " +
            "This is probably an incorrectly implemented test. " +
            "Try to set the parallelism of the sink to 1.")
        }
      }
    }
  }

  def getRawResults: List[String] = getResults

  def getRetractResults: List[String] = {
    clearAndStashGlobalResults()
    val result = ArrayBuffer.empty[String]
    this.globalRetractResults.foreach {
      case (_, list) => result ++= list
    }
    result.toList
  }
}

final class TestingRetractTableSink(tz: TimeZone) extends RetractStreamTableSink[Row] {

  var fNames: Array[String] = _
  var fTypes: Array[TypeInformation[_]] = _
  var sink = new TestingRetractSink(tz)

  def this() {
    this(TimeZone.getTimeZone("UTC"))
  }

  override def consumeDataStream(
      dataStream: DataStream[JTuple2[JBoolean, Row]]): DataStreamSink[_] = {
    dataStream.map(new MapFunction[JTuple2[JBoolean, Row], (Boolean, Row)] {
      override def map(value: JTuple2[JBoolean, Row]): (Boolean, Row) = {
        (value.f0, value.f1)
      }
    }).setParallelism(dataStream.getParallelism)
      .addSink(sink)
      .name("TestingRetractTableSink")
      .setParallelism(dataStream.getParallelism)
  }

  override def getRecordType: TypeInformation[Row] =
    new RowTypeInfo(fTypes, fNames)

  override def getFieldNames: Array[String] = fNames

  override def getFieldTypes: Array[TypeInformation[_]] = fTypes

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TestingRetractTableSink = {
    val copy = new TestingRetractTableSink(tz)
    copy.fNames = fieldNames
    copy.fTypes = fieldTypes
    copy.sink = sink
    copy
  }

  def getRawResults: List[String] = {
    sink.getRawResults
  }

  def getRetractResults: List[String] = {
    sink.getRetractResults
  }
}
