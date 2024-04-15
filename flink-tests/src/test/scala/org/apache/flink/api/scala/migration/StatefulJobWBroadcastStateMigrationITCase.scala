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
package org.apache.flink.api.scala.migration

import org.apache.flink.FlinkVersion
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{OpenContext, RichFlatMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.migration.CustomEnum.CustomEnum
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext, StateBackendLoader}
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.checkpointing.utils.SnapshotMigrationTestBase
import org.apache.flink.test.checkpointing.utils.SnapshotMigrationTestBase.{ExecutionMode, SnapshotSpec, SnapshotType}
import org.apache.flink.test.util.MigrationTest
import org.apache.flink.test.util.MigrationTest.ParameterizedSnapshotsGenerator
import org.apache.flink.util.Collector

import org.junit.{Assert, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import javax.annotation.Nullable

import java.util
import java.util.function.BiFunction
import java.util.stream.Collectors

import scala.util.{Failure, Try}

object StatefulJobWBroadcastStateMigrationITCase {

  @Parameterized.Parameters(name = "Test snapshot: {0}")
  def createSpecsForTestRuns: util.Collection[SnapshotSpec] =
    internalParameters(null)

  def createSpecsForTestDataGeneration(version: FlinkVersion): util.Collection[SnapshotSpec] =
    internalParameters(version)

  def internalParameters(
      @Nullable targetGeneratingVersion: FlinkVersion): util.Collection[SnapshotSpec] = {
    val getFlinkVersions =
      new BiFunction[FlinkVersion, FlinkVersion, util.Collection[FlinkVersion]] {
        override def apply(
            minInclVersion: FlinkVersion,
            maxInclVersion: FlinkVersion): util.Collection[FlinkVersion] = if (
          targetGeneratingVersion != null
        )
          FlinkVersion
            .rangeOf(minInclVersion, maxInclVersion)
            .stream()
            .filter(v => v.equals(targetGeneratingVersion))
            .collect(Collectors.toList())
        else
          FlinkVersion.rangeOf(minInclVersion, maxInclVersion)
      }

    // Note: It is not safe to restore savepoints created in a Scala applications with Flink
    // version 1.7 or below. The reason is that up to version 1.7 the underlying Scala serializer
    // used names of anonymous classes that depend on the relative position/order in code, e.g.,
    // if two anonymous classes, instantiated inside the same class and from the same base class,
    // change order in the code their names are switched.
    // As a consequence, changes in code may result in restore failures.
    // This was fixed in version 1.8, see: https://issues.apache.org/jira/browse/FLINK-10493
    var parameters: util.List[SnapshotSpec] = new util.LinkedList[SnapshotSpec]()
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.MEMORY_STATE_BACKEND_NAME,
        SnapshotType.SAVEPOINT_CANONICAL,
        getFlinkVersions.apply(FlinkVersion.v1_8, FlinkVersion.v1_13)
      ))
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
        SnapshotType.SAVEPOINT_CANONICAL,
        getFlinkVersions.apply(FlinkVersion.v1_14, MigrationTest.getMostRecentlyPublishedVersion)
      ))
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
        SnapshotType.SAVEPOINT_CANONICAL,
        getFlinkVersions.apply(FlinkVersion.v1_15, MigrationTest.getMostRecentlyPublishedVersion)
      ))
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
        SnapshotType.SAVEPOINT_NATIVE,
        getFlinkVersions.apply(FlinkVersion.v1_15, MigrationTest.getMostRecentlyPublishedVersion)
      ))
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
        SnapshotType.SAVEPOINT_NATIVE,
        getFlinkVersions.apply(FlinkVersion.v1_15, MigrationTest.getMostRecentlyPublishedVersion)
      ))
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.HASHMAP_STATE_BACKEND_NAME,
        SnapshotType.CHECKPOINT,
        getFlinkVersions.apply(FlinkVersion.v1_15, MigrationTest.getMostRecentlyPublishedVersion)
      ))
    parameters.addAll(
      SnapshotSpec.withVersions(
        StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME,
        SnapshotType.CHECKPOINT,
        getFlinkVersions.apply(FlinkVersion.v1_15, MigrationTest.getMostRecentlyPublishedVersion)
      ))
    parameters
  }

  def getSnapshotPath(snapshotSpec: SnapshotSpec): String = {
    val path = new StringBuilder(s"stateful-scala-with-broadcast-udf-migration-itcase")
    path ++= s"-flink${snapshotSpec.getFlinkVersion()}"
    path ++= s"-${snapshotSpec.getStateBackendType()}"
    snapshotSpec.getSnapshotType() match {
      case SnapshotType.SAVEPOINT_CANONICAL =>
        path ++= "-savepoint"
      case SnapshotType.SAVEPOINT_NATIVE =>
        path ++= "-savepoint-native"
      case SnapshotType.CHECKPOINT =>
        path ++= "-checkpoint"
      case _ => throw new UnsupportedOperationException
    }
    path.toString()
  }

  val NUM_ELEMENTS = 4
}

/** ITCase for migration Scala state types across different Flink versions. */
@RunWith(classOf[Parameterized])
class StatefulJobWBroadcastStateMigrationITCase(snapshotSpec: SnapshotSpec)
  extends SnapshotMigrationTestBase
  with Serializable
  with MigrationTest {

  /** Generates all the required states. */
  @ParameterizedSnapshotsGenerator("createSpecsForTestDataGeneration")
  def generateSnapshots(snapshotSpec: SnapshotSpec): Unit = {
    testOrCreateSavepointWithBroadcast(ExecutionMode.CREATE_SNAPSHOT, snapshotSpec)
  }

  @Test
  def testSavepointWithBroadcast(): Unit = {
    testOrCreateSavepointWithBroadcast(ExecutionMode.VERIFY_SNAPSHOT, snapshotSpec)
  }

  def testOrCreateSavepointWithBroadcast(
      executionMode: ExecutionMode,
      snapshotSpec: SnapshotSpec): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    snapshotSpec.getStateBackendType match {
      case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME =>
        env.setStateBackend(new EmbeddedRocksDBStateBackend())

        if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
          // disable changelog backend for now to ensure determinism in test data generation (see FLINK-31766)
          env.enableChangelogStateBackend(false)
        }
      case StateBackendLoader.MEMORY_STATE_BACKEND_NAME =>
        env.setStateBackend(new MemoryStateBackend())
      case StateBackendLoader.HASHMAP_STATE_BACKEND_NAME =>
        env.setStateBackend(new HashMapStateBackend())
      case _ => throw new UnsupportedOperationException
    }

    lazy val firstBroadcastStateDesc = new MapStateDescriptor[Long, Long](
      "broadcast-state-1",
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]],
      BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

    lazy val secondBroadcastStateDesc = new MapStateDescriptor[String, String](
      "broadcast-state-2",
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    env.enableCheckpointing(500)
    env.setParallelism(4)
    env.setMaxParallelism(4)

    val stream = env
      .addSource(new CheckpointedSource(4))
      .setMaxParallelism(1)
      .uid("checkpointedSource")
      .keyBy(
        new KeySelector[(Long, Long), Long] {
          override def getKey(value: (Long, Long)): Long = value._1
        }
      )
      .flatMap(new StatefulFlatMapper)
      .keyBy(
        new KeySelector[(Long, Long), Long] {
          override def getKey(value: (Long, Long)): Long = value._1
        }
      )

    val broadcastStream = env
      .addSource(new CheckpointedSource(4))
      .setMaxParallelism(1)
      .uid("checkpointedBroadcastSource")
      .broadcast(firstBroadcastStateDesc, secondBroadcastStateDesc)

    if (executionMode == ExecutionMode.CREATE_SNAPSHOT) {
      stream
        .connect(broadcastStream)
        .process(new TestBroadcastProcessFunction)
        .addSink(new AccumulatorCountingSink)

      executeAndSnapshot(
        env,
        s"src/test/resources/"
          + StatefulJobWBroadcastStateMigrationITCase.getSnapshotPath(snapshotSpec),
        snapshotSpec.getSnapshotType(),
        new Tuple2(
          AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
          StatefulJobWBroadcastStateMigrationITCase.NUM_ELEMENTS
        )
      )
    } else if (executionMode == ExecutionMode.VERIFY_SNAPSHOT) {
      val expectedFirstState: Map[Long, Long] =
        Map(0L -> 0L, 1L -> 1L, 2L -> 2L, 3L -> 3L)
      val expectedSecondState: Map[String, String] =
        Map("0" -> "0", "1" -> "1", "2" -> "2", "3" -> "3")

      stream
        .connect(broadcastStream)
        .process(new VerifyingBroadcastProcessFunction(expectedFirstState, expectedSecondState))
        .addSink(new AccumulatorCountingSink)

      restoreAndExecute(
        env,
        SnapshotMigrationTestBase.getResourceFilename(
          StatefulJobWBroadcastStateMigrationITCase.getSnapshotPath(snapshotSpec)),
        new Tuple2(
          AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
          StatefulJobWBroadcastStateMigrationITCase.NUM_ELEMENTS)
      )
    } else {
      throw new UnsupportedOperationException("Unsupported execution mode.")
    }
  }
}

class TestBroadcastProcessFunction
  extends KeyedBroadcastProcessFunction[Long, (Long, Long), (Long, Long), (Long, Long)] {

  lazy val firstBroadcastStateDesc = new MapStateDescriptor[Long, Long](
    "broadcast-state-1",
    BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]],
    BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

  val secondBroadcastStateDesc = new MapStateDescriptor[String, String](
    "broadcast-state-2",
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  @throws[Exception]
  override def processElement(
      value: (Long, Long),
      ctx: KeyedBroadcastProcessFunction[
        Long,
        (Long, Long),
        (Long, Long),
        (Long, Long)]#ReadOnlyContext,
      out: Collector[(Long, Long)]): Unit = {

    out.collect(value)
  }

  @throws[Exception]
  override def processBroadcastElement(
      value: (Long, Long),
      ctx: KeyedBroadcastProcessFunction[Long, (Long, Long), (Long, Long), (Long, Long)]#Context,
      out: Collector[(Long, Long)]): Unit = {

    ctx.getBroadcastState(firstBroadcastStateDesc).put(value._1, value._2)
    ctx.getBroadcastState(secondBroadcastStateDesc).put(value._1.toString, value._2.toString)
  }
}

@SerialVersionUID(1L)
private object CheckpointedSource {
  var CHECKPOINTED_STRING = "Here be dragons!"
}

@SerialVersionUID(1L)
private class CheckpointedSource(val numElements: Int)
  extends SourceFunction[(Long, Long)]
  with CheckpointedFunction {

  private var isRunning = true
  private var state: ListState[CustomCaseClass] = _

  @throws[Exception]
  override def run(ctx: SourceFunction.SourceContext[(Long, Long)]) {
    ctx.emitWatermark(new Watermark(0))
    ctx.getCheckpointLock synchronized {
      var i = 0
      while (i < numElements) {
        ctx.collect(i, i)
        i += 1
      }
    }
    // don't emit a final watermark so that we don't trigger the registered event-time
    // timers
    while (isRunning) Thread.sleep(20)
  }

  def cancel() {
    isRunning = false
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    state = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[CustomCaseClass](
        "sourceState",
        createTypeInformation[CustomCaseClass]))
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.update(java.util.Collections.singletonList(CustomCaseClass("Here be dragons!", 123)))
  }
}

@SerialVersionUID(1L)
private object AccumulatorCountingSink {
  var NUM_ELEMENTS_ACCUMULATOR = classOf[AccumulatorCountingSink[_]] + "_NUM_ELEMENTS"
}

@SerialVersionUID(1L)
private class AccumulatorCountingSink[T] extends RichSinkFunction[T] {

  private var count: Int = 0

  @throws[Exception]
  override def open(openContext: OpenContext) {
    super.open(openContext)
    getRuntimeContext.addAccumulator(
      AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR,
      new IntCounter)
  }

  @throws[Exception]
  override def invoke(value: T) {
    count += 1
    getRuntimeContext.getAccumulator(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR).add(1)
  }
}

class StatefulFlatMapper extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var caseClassState: ValueState[CustomCaseClass] = _
  private var caseClassWithNestingState: ValueState[CustomCaseClassWithNesting] = _
  private var collectionState: ValueState[List[CustomCaseClass]] = _
  private var tryState: ValueState[Try[CustomCaseClass]] = _
  private var tryFailureState: ValueState[Try[CustomCaseClass]] = _
  private var optionState: ValueState[Option[CustomCaseClass]] = _
  private var optionNoneState: ValueState[Option[CustomCaseClass]] = _
  private var eitherLeftState: ValueState[Either[CustomCaseClass, String]] = _
  private var eitherRightState: ValueState[Either[CustomCaseClass, String]] = _
  private var enumOneState: ValueState[CustomEnum] = _
  private var enumThreeState: ValueState[CustomEnum] = _

  override def open(openContext: OpenContext): Unit = {
    caseClassState = getRuntimeContext.getState(
      new ValueStateDescriptor[CustomCaseClass](
        "caseClassState",
        createTypeInformation[CustomCaseClass]))
    caseClassWithNestingState = getRuntimeContext.getState(
      new ValueStateDescriptor[CustomCaseClassWithNesting](
        "caseClassWithNestingState",
        createTypeInformation[CustomCaseClassWithNesting]))
    collectionState = getRuntimeContext.getState(
      new ValueStateDescriptor[List[CustomCaseClass]](
        "collectionState",
        createTypeInformation[List[CustomCaseClass]]))
    tryState = getRuntimeContext.getState(
      new ValueStateDescriptor[Try[CustomCaseClass]](
        "tryState",
        createTypeInformation[Try[CustomCaseClass]]))
    tryFailureState = getRuntimeContext.getState(
      new ValueStateDescriptor[Try[CustomCaseClass]](
        "tryFailureState",
        createTypeInformation[Try[CustomCaseClass]]))
    optionState = getRuntimeContext.getState(
      new ValueStateDescriptor[Option[CustomCaseClass]](
        "optionState",
        createTypeInformation[Option[CustomCaseClass]]))
    optionNoneState = getRuntimeContext.getState(
      new ValueStateDescriptor[Option[CustomCaseClass]](
        "optionNoneState",
        createTypeInformation[Option[CustomCaseClass]]))
    eitherLeftState = getRuntimeContext.getState(
      new ValueStateDescriptor[Either[CustomCaseClass, String]](
        "eitherLeftState",
        createTypeInformation[Either[CustomCaseClass, String]]))
    eitherRightState = getRuntimeContext.getState(
      new ValueStateDescriptor[Either[CustomCaseClass, String]](
        "eitherRightState",
        createTypeInformation[Either[CustomCaseClass, String]]))
    enumOneState = getRuntimeContext.getState(
      new ValueStateDescriptor[CustomEnum]("enumOneState", createTypeInformation[CustomEnum]))
    enumThreeState = getRuntimeContext.getState(
      new ValueStateDescriptor[CustomEnum]("enumThreeState", createTypeInformation[CustomEnum]))
  }

  override def flatMap(in: (Long, Long), collector: Collector[(Long, Long)]): Unit = {
    caseClassState.update(CustomCaseClass(in._1.toString, in._2 * 2))
    caseClassWithNestingState.update(
      CustomCaseClassWithNesting(in._1, CustomCaseClass(in._1.toString, in._2 * 2)))
    collectionState.update(List(CustomCaseClass(in._1.toString, in._2 * 2)))
    tryState.update(Try(CustomCaseClass(in._1.toString, in._2 * 5)))
    tryFailureState.update(Failure(new RuntimeException))
    optionState.update(Some(CustomCaseClass(in._1.toString, in._2 * 2)))
    optionNoneState.update(None)
    eitherLeftState.update(Left(CustomCaseClass(in._1.toString, in._2 * 2)))
    eitherRightState.update(Right((in._1 * 3).toString))
    enumOneState.update(CustomEnum.ONE)
    enumOneState.update(CustomEnum.THREE)

    collector.collect(in)
  }
}

class VerifyingBroadcastProcessFunction(
    firstExpectedBroadcastState: Map[Long, Long],
    secondExpectedBroadcastState: Map[String, String])
  extends KeyedBroadcastProcessFunction[Long, (Long, Long), (Long, Long), (Long, Long)] {

  lazy val firstBroadcastStateDesc = new MapStateDescriptor[Long, Long](
    "broadcast-state-1",
    BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]],
    BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]])

  val secondBroadcastStateDesc = new MapStateDescriptor[String, String](
    "broadcast-state-2",
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.STRING_TYPE_INFO)

  @throws[Exception]
  override def processElement(
      value: (Long, Long),
      ctx: KeyedBroadcastProcessFunction[
        Long,
        (Long, Long),
        (Long, Long),
        (Long, Long)]#ReadOnlyContext,
      out: Collector[(Long, Long)]): Unit = {

    var actualFirstState = Map[Long, Long]()

    import scala.collection.JavaConversions._
    for (entry <- ctx.getBroadcastState(firstBroadcastStateDesc).immutableEntries()) {
      val v = firstExpectedBroadcastState.get(entry.getKey).get
      Assert.assertEquals(v, entry.getValue)
      actualFirstState += (entry.getKey -> entry.getValue)
    }

    Assert.assertEquals(firstExpectedBroadcastState, actualFirstState)

    var actualSecondState = Map[String, String]()
    for (entry <- ctx.getBroadcastState(secondBroadcastStateDesc).immutableEntries()) {
      val v = secondExpectedBroadcastState.get(entry.getKey).get
      Assert.assertEquals(v, entry.getValue)
      actualSecondState += (entry.getKey -> entry.getValue)
    }

    Assert.assertEquals(secondExpectedBroadcastState, actualSecondState)
    out.collect(value)
  }

  @throws[Exception]
  override def processBroadcastElement(
      value: (Long, Long),
      ctx: KeyedBroadcastProcessFunction[Long, (Long, Long), (Long, Long), (Long, Long)]#Context,
      out: Collector[(Long, Long)]): Unit = {

    // do nothing
  }
}
