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

package org.apache.flink.streaming.api.scala

import java.lang

import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.io.ParallelIteratorInputFormat
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.graph.{StreamEdge, StreamGraph}
import org.apache.flink.streaming.api.operators._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.partitioner._
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}

class DataStreamTest extends AbstractTestBase {

  private val expectedException = ExpectedException.none()

  @Rule
  def thrownException = expectedException

  @Test
  def testNaming(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source1Operator = env.generateSequence(0, 0).name("testSource1")
    val source1 = source1Operator
    assert("testSource1" == source1Operator.getName)

    val dataStream1 = source1
      .map(x => 0L)
      .name("testMap")
    assert("testMap" == dataStream1.getName)

    val dataStream2 = env.generateSequence(0, 0).name("testSource2")
      .keyBy(x=>x)
      .reduce((x, y) => 0L)
      .name("testReduce")
    assert("testReduce" == dataStream2.getName)

    val connected = dataStream1.connect(dataStream2)
      .flatMap({ (in, out: Collector[(Long, Long)]) => }, { (in, out: Collector[(Long, Long)]) => })
      .name("testCoFlatMap")

    assert("testCoFlatMap" == connected.getName)

    val func: (((Long, Long), (Long, Long)) => (Long, Long)) =
      (x: (Long, Long), y: (Long, Long)) => (0L, 0L)

    val windowed = connected
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](10)))
      .fold((0L, 0L))(func)

    windowed.name("testWindowFold")

    assert("testWindowFold" == windowed.getName)

    windowed.print()

    val plan = env.getExecutionPlan

    assert(plan contains "testSource1")
    assert(plan contains "testSource2")
    assert(plan contains "testMap")
    assert(plan contains "testReduce")
    assert(plan contains "testCoFlatMap")
    assert(plan contains "testWindowFold")
  }

  /**
   * Tests that [[DataStream.keyBy]] and [[DataStream.partitionCustom]] result in
   * different and correct topologies. Does the some for the [[ConnectedStreams]].
   */
  @Test
  def testPartitioning(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src1: DataStream[(Long, Long)] = env.fromElements((0L, 0L))
    val src2: DataStream[(Long, Long)] = env.fromElements((0L, 0L))

    val connected = src1.connect(src2)

    val group1 = src1.keyBy(0)
    val group2 = src1.keyBy(1, 0)
    val group3 = src1.keyBy("_1")
    val group4 = src1.keyBy(x => x._1)

    val gid1 = createDownStreamId(group1)
    val gid2 = createDownStreamId(group2)
    val gid3 = createDownStreamId(group3)
    val gid4 = createDownStreamId(group4)
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, gid1)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, gid2)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, gid3)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, gid4)))

    //Testing DataStream partitioning
    val partition1: DataStream[_] = src1.keyBy(0)
    val partition2: DataStream[_] = src1.keyBy(1, 0)
    val partition3: DataStream[_] = src1.keyBy("_1")
    val partition4: DataStream[_] = src1.keyBy((x : (Long, Long)) => x._1)

    val pid1 = createDownStreamId(partition1)
    val pid2 = createDownStreamId(partition2)
    val pid3 = createDownStreamId(partition3)
    val pid4 = createDownStreamId(partition4)

    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, pid1)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, pid2)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, pid3)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, pid4)))

    // Testing DataStream custom partitioning
    val longPartitioner: Partitioner[Long] = new Partitioner[Long] {
      override def partition(key: Long, numPartitions: Int): Int = 0
    }

    val customPartition1: DataStream[_] =
      src1.partitionCustom(longPartitioner, 0)
    val customPartition3: DataStream[_] =
      src1.partitionCustom(longPartitioner, "_1")
    val customPartition4: DataStream[_] =
      src1.partitionCustom(longPartitioner, (x : (Long, Long)) => x._1)

    val cpid1 = createDownStreamId(customPartition1)
    val cpid2 = createDownStreamId(customPartition3)
    val cpid3 = createDownStreamId(customPartition4)
    assert(isCustomPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, cpid1)))
    assert(isCustomPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, cpid2)))
    assert(isCustomPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, cpid3)))

    //Testing ConnectedStreams grouping
    val connectedGroup1: ConnectedStreams[_, _] = connected.keyBy(0, 0)
    val downStreamId1: Integer = createDownStreamId(connectedGroup1)

    val connectedGroup2: ConnectedStreams[_, _] = connected.keyBy(Array[Int](0), Array[Int](0))
    val downStreamId2: Integer = createDownStreamId(connectedGroup2)

    val connectedGroup3: ConnectedStreams[_, _] = connected.keyBy("_1", "_1")
    val downStreamId3: Integer = createDownStreamId(connectedGroup3)

    val connectedGroup4: ConnectedStreams[_, _] =
      connected.keyBy(Array[String]("_1"), Array[String]("_1"))
    val downStreamId4: Integer = createDownStreamId(connectedGroup4)

    val connectedGroup5: ConnectedStreams[_, _] = connected.keyBy(x => x._1, x => x._1)
    val downStreamId5: Integer = createDownStreamId(connectedGroup5)

    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, downStreamId1)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, downStreamId1)))

    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, downStreamId2)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, downStreamId2)))

    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, downStreamId3)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, downStreamId3)))

    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, downStreamId4)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, downStreamId4)))

    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, downStreamId5)))
    assert(isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, downStreamId5)))

    //Testing ConnectedStreams partitioning
    val connectedPartition1: ConnectedStreams[_, _] = connected.keyBy(0, 0)
    val connectDownStreamId1: Integer = createDownStreamId(connectedPartition1)

    val connectedPartition2: ConnectedStreams[_, _] =
      connected.keyBy(Array[Int](0), Array[Int](0))
    val connectDownStreamId2: Integer = createDownStreamId(connectedPartition2)

    val connectedPartition3: ConnectedStreams[_, _] = connected.keyBy("_1", "_1")
    val connectDownStreamId3: Integer = createDownStreamId(connectedPartition3)

    val connectedPartition4: ConnectedStreams[_, _] =
      connected.keyBy(Array[String]("_1"), Array[String]("_1"))
    val connectDownStreamId4: Integer = createDownStreamId(connectedPartition4)

    val connectedPartition5: ConnectedStreams[_, _] =
      connected.keyBy(x => x._1, x => x._1)
    val connectDownStreamId5: Integer = createDownStreamId(connectedPartition5)

    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, connectDownStreamId1))
    )
    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, connectDownStreamId1))
    )

    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, connectDownStreamId2))
    )
    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, connectDownStreamId2))
    )

    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, connectDownStreamId3))
    )
    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, connectDownStreamId3))
    )

    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, connectDownStreamId4))
    )
    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, connectDownStreamId4))
    )

    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src1.getId, connectDownStreamId5))
    )
    assert(
      isPartitioned(env.getStreamGraph.getStreamEdges(src2.getId, connectDownStreamId5))
    )
  }

  /**
   * Tests whether parallelism gets set.
   */
  @Test
  def testParallelism() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val parallelism = env.getParallelism

    val src = env.fromElements(new Tuple2[Long, Long](0L, 0L))
    val map = src.map(x => (0L, 0L))
    val windowed: DataStream[(Long, Long)] = map
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](10)))
      .fold((0L, 0L))((x: (Long, Long), y: (Long, Long)) => (0L, 0L))

    windowed.print()
    val sink = map.addSink(x => {})

    assert(1 == env.getStreamGraph.getStreamNode(src.getId).getParallelism)
    assert(parallelism == env.getStreamGraph.getStreamNode(map.getId).getParallelism)
    assert(1 == env.getStreamGraph.getStreamNode(windowed.getId).getParallelism)
    assert(parallelism == env
      .getStreamGraph
      .getStreamNode(sink.getTransformation.getId)
      .getParallelism)

    try {
      src.setParallelism(3)
      fail()
    }
    catch {
      case success: IllegalArgumentException => {
      }
    }

    val newParallelism = parallelism - 1

    env.setParallelism(newParallelism)
    // the parallelism does not change since some windowing code takes the parallelism from
    // input operations and that cannot change dynamically
    assert(1 == env.getStreamGraph.getStreamNode(src.getId).getParallelism)
    assert(parallelism == env.getStreamGraph.getStreamNode(map.getId).getParallelism)
    assert(1 == env.getStreamGraph.getStreamNode(windowed.getId).getParallelism)
    assert(parallelism == env
      .getStreamGraph
      .getStreamNode(sink.getTransformation.getId)
      .getParallelism)

    val parallelSource = env.generateSequence(0, 0)
    parallelSource.print()

    assert(newParallelism == env.getStreamGraph.getStreamNode(parallelSource.getId).getParallelism)

    parallelSource.setParallelism(3)
    assert(3 == env.getStreamGraph.getStreamNode(parallelSource.getId).getParallelism)

    map.setParallelism(2)
    assert(2 == env.getStreamGraph.getStreamNode(map.getId).getParallelism)

    sink.setParallelism(4)
    assert(4 == env.getStreamGraph.getStreamNode(sink.getTransformation.getId).getParallelism)
  }


  /**
    * Tests setting the parallelism after a partitioning operation (e.g., broadcast, rescale)
    * should fail.
    */
  @Test
  def testParallelismFailAfterPartitioning(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.fromElements(new Tuple2[Long, Long](0L, 0L))
    val map = src.map(_ => (0L, 0L))

    // This could be replaced with other partitioning operations (e.g., rescale, shuffle, forward),
    // which trigger the setConnectionType() method.
    val broadcastStream = map.broadcast
    thrownException.expect(classOf[UnsupportedOperationException])
    thrownException.expectMessage("cannot set the parallelism")
    broadcastStream.setParallelism(1)
  }

  /**
   * Tests whether resource gets set.
   */
  /*
  @Test
  def testResource() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val minResource1: ResourceSpec = new ResourceSpec(1.0, 100)
    val preferredResource1: ResourceSpec = new ResourceSpec(2.0, 200)
    val minResource2: ResourceSpec = new ResourceSpec(1.0, 200)
    val preferredResource2: ResourceSpec = new ResourceSpec(2.0, 300)
    val minResource3: ResourceSpec = new ResourceSpec(1.0, 300)
    val preferredResource3: ResourceSpec = new ResourceSpec(2.0, 400)
    val minResource4: ResourceSpec = new ResourceSpec(1.0, 400)
    val preferredResource4: ResourceSpec = new ResourceSpec(2.0, 500)
    val minResource5: ResourceSpec = new ResourceSpec(1.0, 500)
    val preferredResource5: ResourceSpec = new ResourceSpec(2.0, 600)
    val minResource6: ResourceSpec = new ResourceSpec(1.0, 600)
    val preferredResource6: ResourceSpec = new ResourceSpec(2.0, 700)
    val minResource7: ResourceSpec = new ResourceSpec(1.0, 700)
    val preferredResource7: ResourceSpec = new ResourceSpec(2.0, 800)

    val source1: DataStream[Long] = env.generateSequence(0, 0)
      .resource(minResource1, preferredResource1)
    val map1: DataStream[String] = source1.map(x => "")
      .resource(minResource2, preferredResource2)
    val source2: DataStream[Long] = env.generateSequence(0, 0)
      .resource(minResource3, preferredResource3)
    val map2: DataStream[String] = source2.map(x => "")
      .resource(minResource4, preferredResource4)

    val connected: DataStream[String] = map1.connect(map2)
      .flatMap({ (in, out: Collector[(String)]) => }, { (in, out: Collector[(String)]) => })
      .resource(minResource5, preferredResource5)

    val windowed  = connected
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](5)))
      .fold("")((accumulator: String, value: String) => "")
      .resource(minResource6, preferredResource6)

    var sink = windowed.print().resource(minResource7, preferredResource7)

    val plan = env.getExecutionPlan

    assertEquals(minResource1, env.getStreamGraph.getStreamNode(source1.getId).
      getMinResource)
    assertEquals(preferredResource1, env.getStreamGraph.getStreamNode(source1.getId).
      getPreferredResource)
    assertEquals(minResource2, env.getStreamGraph.getStreamNode(map1.getId).
      getMinResource)
    assertEquals(preferredResource2, env.getStreamGraph.getStreamNode(map1.getId).
      getPreferredResource)
    assertEquals(minResource3, env.getStreamGraph.getStreamNode(source2.getId).
      getMinResource)
    assertEquals(preferredResource3, env.getStreamGraph.getStreamNode(source2.getId).
      getPreferredResource)
    assertEquals(minResource4, env.getStreamGraph.getStreamNode(map2.getId).
      getMinResource)
    assertEquals(preferredResource4, env.getStreamGraph.getStreamNode(map2.getId).
      getPreferredResource)
    assertEquals(minResource5, env.getStreamGraph.getStreamNode(connected.getId).
      getMinResource)
    assertEquals(preferredResource5, env.getStreamGraph.getStreamNode(connected.getId).
      getPreferredResource)
    assertEquals(minResource6, env.getStreamGraph.getStreamNode(windowed.getId).
      getMinResource)
    assertEquals(preferredResource6, env.getStreamGraph.getStreamNode(windowed.getId).
      getPreferredResource)
    assertEquals(minResource7, env.getStreamGraph.getStreamNode(
      sink.getPreferredResource.getId).getMinResource)
    assertEquals(preferredResource7, env.getStreamGraph.getStreamNode(
      sink.getPreferredResource.getId).getPreferredResource)
  }*/

  @Test
  def testTypeInfo() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src1: DataStream[Long] = env.generateSequence(0, 0)
    assert(TypeExtractor.getForClass(classOf[Long]) == src1.getType)

    val map: DataStream[(Integer, String)] = src1.map(x => null)
    assert(classOf[scala.Tuple2[Integer, String]] == map.getType().getTypeClass)

    val window: DataStream[String] = map
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](5)))
      .apply((w: GlobalWindow, x: Iterable[(Integer, String)], y: Collector[String]) => {})

    assert(TypeExtractor.getForClass(classOf[String]) == window.getType)

    val flatten: DataStream[Int] = window
      .windowAll(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[GlobalWindow](5)))
      .fold(0)((accumulator: Int, value: String) => 0)
    assert(TypeExtractor.getForClass(classOf[Int]) == flatten.getType())

    // TODO check for custom case class
  }

  /**
   * Verify that a [[KeyedStream.process(ProcessFunction)]] call is correctly
   * translated to an operator.
   */
  @Test
  def testKeyedStreamProcessTranslation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val processFunction = new ProcessFunction[Long, Int] {
      override def processElement(
          value: Long,
          ctx: ProcessFunction[Long, Int]#Context,
          out: Collector[Int]): Unit = ???
    }

    val flatMapped = src.keyBy(x => x).process(processFunction)

    assert(processFunction == getFunctionForDataStream(flatMapped))
    assert(getOperatorForDataStream(flatMapped).isInstanceOf[LegacyKeyedProcessOperator[_, _, _]])
  }

  /**
   * Verify that a [[KeyedStream.process(KeyedProcessFunction)]] call is correctly
   * translated to an operator.
   */
  @Test
  def testKeyedStreamKeyedProcessTranslation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val keyedProcessFunction = new KeyedProcessFunction[Long, Long, Int] {
      override def processElement(
                                   value: Long,
                                   ctx: KeyedProcessFunction[Long, Long, Int]#Context,
                                   out: Collector[Int]): Unit = ???
    }

    val flatMapped = src.keyBy(x => x).process(keyedProcessFunction)

    assert(keyedProcessFunction == getFunctionForDataStream(flatMapped))
    assert(getOperatorForDataStream(flatMapped).isInstanceOf[KeyedProcessOperator[_, _, _]])
  }

  /**
   * Verify that a [[DataStream.process(ProcessFunction)]] call is correctly
   * translated to an operator.
   */
  @Test
  def testProcessTranslation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val processFunction = new ProcessFunction[Long, Int] {
      override def processElement(
          value: Long,
          ctx: ProcessFunction[Long, Int]#Context,
          out: Collector[Int]): Unit = ???
    }

    val flatMapped = src.process(processFunction)

    assert(processFunction == getFunctionForDataStream(flatMapped))
    assert(getOperatorForDataStream(flatMapped).isInstanceOf[ProcessOperator[_, _]])
  }

  @Test def operatorTest() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val mapFunction = new MapFunction[Long, Int] {
      override def map(value: Long): Int = 0
    }

    val map = src.map(mapFunction)
    assert(mapFunction == getFunctionForDataStream(map))
    assert(getFunctionForDataStream(map.map(x => 0)).isInstanceOf[MapFunction[_, _]])
    
    val statefulMap2 = src.keyBy(x => x).mapWithState(
        (in, state: Option[Long]) => (in, None.asInstanceOf[Option[Long]]))
    
    val flatMapFunction = new FlatMapFunction[Long, Int] {
      override def flatMap(value: Long, out: Collector[Int]): Unit = {}
    }
    
    val flatMap = src.flatMap(flatMapFunction)
    assert(flatMapFunction == getFunctionForDataStream(flatMap))
    assert(
      getFunctionForDataStream(flatMap
        .flatMap((x: Int, out: Collector[Int]) => {}))
        .isInstanceOf[FlatMapFunction[_, _]])

    val statefulfMap2 = src.keyBy(x => x).flatMapWithState(
        (in, state: Option[Long]) => (List(in), None.asInstanceOf[Option[Long]]))
   
    val filterFunction = new FilterFunction[Int] {
      override def filter(value: Int): Boolean = false
    }

    val unionFilter = map.union(flatMap).filter(filterFunction)
    assert(filterFunction == getFunctionForDataStream(unionFilter))
    assert(
      getFunctionForDataStream(map
        .filter((x: Int) => true))
        .isInstanceOf[FilterFunction[_]])

    val statefulFilter2 = src.keyBy( x => x).filterWithState[Long](
        (in, state: Option[Long]) => (false, None))
   
    try {
      env.getStreamGraph.getStreamEdges(map.getId, unionFilter.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }

    try {
      env.getStreamGraph.getStreamEdges(flatMap.getId, unionFilter.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }

    val outputSelector = new OutputSelector[Int] {
      override def select(value: Int): lang.Iterable[String] = null
    }

    val split = unionFilter.split(outputSelector)
    split.print()
    val outputSelectors = env.getStreamGraph.getStreamNode(unionFilter.getId).getOutputSelectors
    assert(1 == outputSelectors.size)
    assert(outputSelector == outputSelectors.get(0))

    unionFilter.split(x => List("a")).print()
    val moreOutputSelectors = env.getStreamGraph.getStreamNode(unionFilter.getId).getOutputSelectors
    assert(2 == moreOutputSelectors.size)

    val select = split.select("a")
    val sink = select.print()
    val splitEdge =
      env.getStreamGraph.getStreamEdges(unionFilter.getId, sink.getTransformation.getId)
    assert("a" == splitEdge.get(0).getSelectedNames.get(0))

    val sinkWithIdentifier = select.print("identifier")
    val newSplitEdge = env.getStreamGraph.getStreamEdges(
      unionFilter.getId,
      sinkWithIdentifier.getTransformation.getId)
    assert("a" == newSplitEdge.get(0).getSelectedNames.get(0))

    val foldFunction = new FoldFunction[Int, String] {
      override def fold(accumulator: String, value: Int): String = ""
    }
    val fold = map.keyBy(x=>x).fold("", foldFunction)
    assert(foldFunction == getFunctionForDataStream(fold))
    assert(
      getFunctionForDataStream(map.keyBy(x=>x)
        .fold("")((x: String, y: Int) => ""))
        .isInstanceOf[FoldFunction[_, _]])

    val connect = fold.connect(flatMap)

    val coMapFunction =
      new CoMapFunction[String, Int, String] {
        override def map1(value: String): String = ""

        override def map2(value: Int): String = ""
      }
    val coMap = connect.map(coMapFunction)
    assert(coMapFunction == getFunctionForDataStream(coMap))

    try {
      env.getStreamGraph.getStreamEdges(fold.getId, coMap.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }
    try {
      env.getStreamGraph.getStreamEdges(flatMap.getId, coMap.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }
  }

  @Test
  def testChannelSelectors() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.generateSequence(0, 0)

    val broadcast = src.broadcast
    val broadcastSink = broadcast.print()
    val broadcastPartitioner = env.getStreamGraph
      .getStreamEdges(src.getId, broadcastSink.getTransformation.getId).get(0).getPartitioner
    assert(broadcastPartitioner.isInstanceOf[BroadcastPartitioner[_]])

    val shuffle: DataStream[Long] = src.shuffle
    val shuffleSink = shuffle.print()
    val shufflePartitioner = env.getStreamGraph
      .getStreamEdges(src.getId, shuffleSink.getTransformation.getId).get(0).getPartitioner
    assert(shufflePartitioner.isInstanceOf[ShufflePartitioner[_]])

    val forward: DataStream[Long] = src.forward
    val forwardSink = forward.print()
    val forwardPartitioner = env.getStreamGraph
      .getStreamEdges(src.getId, forwardSink.getTransformation.getId).get(0).getPartitioner
    assert(forwardPartitioner.isInstanceOf[ForwardPartitioner[_]])

    val rebalance: DataStream[Long] = src.rebalance
    val rebalanceSink = rebalance.print()
    val rebalancePartitioner = env.getStreamGraph
      .getStreamEdges(src.getId, rebalanceSink.getTransformation.getId).get(0).getPartitioner
    assert(rebalancePartitioner.isInstanceOf[RebalancePartitioner[_]])

    val global: DataStream[Long] = src.global
    val globalSink = global.print()
    val globalPartitioner = env.getStreamGraph
      .getStreamEdges(src.getId, globalSink.getTransformation.getId).get(0).getPartitioner
    assert(globalPartitioner.isInstanceOf[GlobalPartitioner[_]])
  }

  @Test
  def testIterations() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // we need to rebalance before iteration
    val source = env.fromElements(1, 2, 3).map { t: Int => t }

    val iterated = source.iterate((input: ConnectedStreams[Int, String]) => {
      val head = input.map(i => (i + 1).toString, s => s)
      (head.filter(_ == "2"), head.filter(_ != "2"))
    }, 1000).print()

    val iterated2 = source.iterate((input: DataStream[Int]) => 
      (input.map(_ + 1), input.map(_.toString)), 2000)

    val sg = env.getStreamGraph

    assert(sg.getIterationSourceSinkPairs.size() == 2)
  }

  @Test
  def testCreateInputPassesOnTypeInfo(): Unit = {
    StreamExecutionEnvironment.getExecutionEnvironment.createInput[Tuple1[Integer]](
      new ParallelIteratorInputFormat[Tuple1[Integer]](null))
  }

  /////////////////////////////////////////////////////////////
  // Utilities
  /////////////////////////////////////////////////////////////

  private def getFunctionForDataStream(dataStream: DataStream[_]): Function = {
    dataStream.print()
    val operator = getOperatorForDataStream(dataStream)
      .asInstanceOf[AbstractUdfStreamOperator[_, _]]
    operator.getUserFunction.asInstanceOf[Function]
  }

  private def getOperatorForDataStream(dataStream: DataStream[_]): StreamOperator[_] = {
    dataStream.print()
    val env = dataStream.javaStream.getExecutionEnvironment
    val streamGraph: StreamGraph = env.getStreamGraph
    streamGraph.getStreamNode(dataStream.getId).getOperator
  }

  private def isPartitioned(edges: java.util.List[StreamEdge]): Boolean = {
    import scala.collection.JavaConverters._
    edges.asScala.forall( _.getPartitioner.isInstanceOf[KeyGroupStreamPartitioner[_, _]])
  }

  private def isCustomPartitioned(edges: java.util.List[StreamEdge]): Boolean = {
    import scala.collection.JavaConverters._
    edges.asScala.forall( _.getPartitioner.isInstanceOf[CustomPartitionerWrapper[_, _]])
  }

  private def createDownStreamId(dataStream: DataStream[_]): Integer = {
    dataStream.print().getTransformation.getId
  }

  private def createDownStreamId(dataStream: ConnectedStreams[_, _]): Integer = {
    val m = dataStream.map(x => 0, x => 0)
    m.print()
    m.getId
  }
}
