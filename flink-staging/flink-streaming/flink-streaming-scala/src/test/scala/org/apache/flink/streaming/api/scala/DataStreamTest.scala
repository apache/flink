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
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction,
  Partitioner, FoldFunction, Function}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.graph.{StreamEdge, StreamGraph}
import org.apache.flink.streaming.api.operators.{AbstractUdfStreamOperator, StreamOperator}
import org.apache.flink.streaming.api.windowing.helper.Count
import org.apache.flink.streaming.runtime.partitioner._
import org.apache.flink.util.Collector
import org.junit.Assert.fail
import org.junit.Test
import org.apache.flink.streaming.api.scala.function.StatefulFunction

class DataStreamTest {

  private val parallelism = 2

  @Test
  def testNaming(): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

    val source1 = env.generateSequence(0, 0).name("testSource1")
    assert("testSource1" == source1.getName)

    val dataStream1 = source1
      .map(x => 0L)
      .name("testMap")
    assert("testMap" == dataStream1.getName)

    val dataStream2 = env.generateSequence(0, 0).name("testSource2")
      .groupBy(x=>x)
      .reduce((x, y) => 0)
      .name("testReduce")
    assert("testReduce" == dataStream2.getName)

    val connected = dataStream1.connect(dataStream2)
      .flatMap(
    { (in, out: Collector[Long]) => }, { (in, out: Collector[Long]) => }
    ).name("testCoFlatMap")
    assert("testCoFlatMap" == connected.getName)

    val func: ((Long, Long) => Long) =
      (x: Long, y: Long) => 0L

    val windowed = connected.window(Count.of(10))
      .foldWindow(0L, func)

    windowed.name("testWindowFold")
    assert("testWindowFold" == windowed.getName)

    windowed.flatten()

    val plan = env.getExecutionPlan

    assert(plan contains "testSource1")
    assert(plan contains "testSource2")
    assert(plan contains "testMap")
    assert(plan contains "testReduce")
    assert(plan contains "testCoFlatMap")
    assert(plan contains "testWindowFold")
  }

  /**
   * Tests that {@link DataStream#groupBy} and {@link DataStream#partitionBy(KeySelector)} result in
   * different and correct topologies. Does the some for the {@link ConnectedDataStream}.
   */
  @Test
  def testPartitioning(): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
    val graph: StreamGraph = env.getStreamGraph;

    val src1: DataStream[(Long, Long)] = env.fromElements((0L, 0L))
    val src2: DataStream[(Long, Long)] = env.fromElements((0L, 0L))

    val connected = src1.connect(src2)

    val group1 = src1.groupBy(0)
    val group2 = src1.groupBy(1, 0)
    val group3 = src1.groupBy("_1")
    val group4 = src1.groupBy(x => x._1)

    assert(isPartitioned(graph.getStreamEdge(group1.getId, createDownStreamId(group1))))
    assert(isPartitioned(graph.getStreamEdge(group2.getId, createDownStreamId(group2))))
    assert(isPartitioned(graph.getStreamEdge(group3.getId, createDownStreamId(group3))))
    assert(isPartitioned(graph.getStreamEdge(group4.getId, createDownStreamId(group4))))

    //Testing DataStream partitioning
    val partition1: DataStream[_] = src1.partitionByHash(0)
    val partition2: DataStream[_] = src1.partitionByHash(1, 0)
    val partition3: DataStream[_] = src1.partitionByHash("_1")
    val partition4: DataStream[_] = src1.partitionByHash((x : (Long, Long)) => x._1);

    assert(isPartitioned(graph.getStreamEdge(partition1.getId, createDownStreamId(partition1))))
    assert(isPartitioned(graph.getStreamEdge(partition2.getId, createDownStreamId(partition2))))
    assert(isPartitioned(graph.getStreamEdge(partition3.getId, createDownStreamId(partition3))))
    assert(isPartitioned(graph.getStreamEdge(partition4.getId, createDownStreamId(partition4))))

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

    assert(isCustomPartitioned(
      graph.getStreamEdge(customPartition1.getId, createDownStreamId(customPartition1))))
    assert(isCustomPartitioned(
      graph.getStreamEdge(customPartition3.getId, createDownStreamId(customPartition3))))
    assert(isCustomPartitioned(
      graph.getStreamEdge(customPartition4.getId, createDownStreamId(customPartition4))))

    //Testing ConnectedDataStream grouping
    val connectedGroup1: ConnectedDataStream[_, _] = connected.groupBy(0, 0)
    val downStreamId1: Integer = createDownStreamId(connectedGroup1)

    val connectedGroup2: ConnectedDataStream[_, _] = connected.groupBy(Array[Int](0), Array[Int](0))
    val downStreamId2: Integer = createDownStreamId(connectedGroup2)

    val connectedGroup3: ConnectedDataStream[_, _] = connected.groupBy("_1", "_1")
    val downStreamId3: Integer = createDownStreamId(connectedGroup3)

    val connectedGroup4: ConnectedDataStream[_, _] =
      connected.groupBy(Array[String]("_1"), Array[String]("_1"))
    val downStreamId4: Integer = createDownStreamId(connectedGroup4)

    val connectedGroup5: ConnectedDataStream[_, _] = connected.groupBy(x => x._1, x => x._1)
    val downStreamId5: Integer = createDownStreamId(connectedGroup5)

    assert(isPartitioned(graph.getStreamEdge(connectedGroup1.getFirst.getId, downStreamId1)))
    assert(isPartitioned(graph.getStreamEdge(connectedGroup1.getSecond.getId, downStreamId1)))

    assert(isPartitioned(graph.getStreamEdge(connectedGroup2.getFirst.getId, downStreamId2)))
    assert(isPartitioned(graph.getStreamEdge(connectedGroup2.getSecond.getId, downStreamId2)))

    assert(isPartitioned(graph.getStreamEdge(connectedGroup3.getFirst.getId, downStreamId3)))
    assert(isPartitioned(graph.getStreamEdge(connectedGroup3.getSecond.getId, downStreamId3)))

    assert(isPartitioned(graph.getStreamEdge(connectedGroup4.getFirst.getId, downStreamId4)))
    assert(isPartitioned(graph.getStreamEdge(connectedGroup4.getSecond.getId, downStreamId4)))

    assert(isPartitioned(graph.getStreamEdge(connectedGroup5.getFirst.getId, downStreamId5)))
    assert(isPartitioned(graph.getStreamEdge(connectedGroup5.getSecond.getId, downStreamId5)))

    //Testing ConnectedDataStream partitioning
    val connectedPartition1: ConnectedDataStream[_, _] = connected.partitionByHash(0, 0)
    val connectDownStreamId1: Integer = createDownStreamId(connectedPartition1)

    val connectedPartition2: ConnectedDataStream[_, _] =
      connected.partitionByHash(Array[Int](0), Array[Int](0))
    val connectDownStreamId2: Integer = createDownStreamId(connectedPartition2)

    val connectedPartition3: ConnectedDataStream[_, _] = connected.partitionByHash("_1", "_1")
    val connectDownStreamId3: Integer = createDownStreamId(connectedPartition3)

    val connectedPartition4: ConnectedDataStream[_, _] =
      connected.partitionByHash(Array[String]("_1"), Array[String]("_1"))
    val connectDownStreamId4: Integer = createDownStreamId(connectedPartition4)

    val connectedPartition5: ConnectedDataStream[_, _] =
      connected.partitionByHash(x => x._1, x => x._1)
    val connectDownStreamId5: Integer = createDownStreamId(connectedPartition5)

    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition1.getFirst.getId, connectDownStreamId1))
    )
    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition1.getSecond.getId, connectDownStreamId1))
    )

    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition2.getFirst.getId, connectDownStreamId2))
    )
    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition2.getSecond.getId, connectDownStreamId2))
    )

    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition3.getFirst.getId, connectDownStreamId3))
    )
    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition3.getSecond.getId, connectDownStreamId3))
    )

    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition4.getFirst.getId, connectDownStreamId4))
    )
    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition4.getSecond.getId, connectDownStreamId4))
    )

    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition5.getFirst.getId, connectDownStreamId5))
    )
    assert(
      isPartitioned(graph.getStreamEdge(connectedPartition5.getSecond.getId, connectDownStreamId5))
    )
  }

  /**
   * Tests whether parallelism gets set.
   */
  @Test
  def testParallelism {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(10)

    val graph: StreamGraph = env.getStreamGraph

    val src = env.fromElements(new Tuple2[Long, Long](0L, 0L))
    val map = src.map(x => 0L)
    val windowed: DataStream[Long] = map
      .window(Count.of(10))
      .foldWindow(0L, (x: Long, y: Long) => 0L)
      .flatten
    val sink = map.addSink(x => {})

    assert(1 == graph.getStreamNode(src.getId).getParallelism)
    assert(10 == graph.getStreamNode(map.getId).getParallelism)
    assert(10 == graph.getStreamNode(windowed.getId).getParallelism)
    assert(10 == graph.getStreamNode(sink.getId).getParallelism)

    try {
      src.setParallelism(3)
      fail
    }
    catch {
      case success: IllegalArgumentException => {
      }
    }

    env.setParallelism(7)
    assert(1 == graph.getStreamNode(src.getId).getParallelism)
    assert(7 == graph.getStreamNode(map.getId).getParallelism)
    assert(7 == graph.getStreamNode(windowed.getId).getParallelism)
    assert(7 == graph.getStreamNode(sink.getId).getParallelism)

    val parallelSource = env.generateSequence(0, 0)

    assert(7 == graph.getStreamNode(parallelSource.getId).getParallelism)

    parallelSource.setParallelism(3)
    assert(3 == graph.getStreamNode(parallelSource.getId).getParallelism)

    map.setParallelism(2)
    assert(2 == graph.getStreamNode(map.getId).getParallelism)

    sink.setParallelism(4)
    assert(4 == graph.getStreamNode(sink.getId).getParallelism)
  }

  @Test
  def testTypeInfo {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

    val src1: DataStream[Long] = env.generateSequence(0, 0)
    assert(TypeExtractor.getForClass(classOf[Long]) == src1.getType)

    val map: DataStream[(Integer, String)] = src1.map(x => null)
    assert(classOf[scala.Tuple2[Integer, String]] == map.getType.getTypeClass)

    val window: WindowedDataStream[String] = map
      .window(Count.of(5))
      .mapWindow((x: Iterable[(Integer, String)], y: Collector[String]) => {})
    assert(TypeExtractor.getForClass(classOf[String]) == window.getType)

    val flatten: DataStream[Int] = window
      .foldWindow(0,
        (accumulator: Int, value: String) => 0
      ).flatten
    assert(TypeExtractor.getForClass(classOf[Int]) == flatten.getType)

    // TODO check for custom case class
  }

  @Test def operatorTest {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

    val streamGraph = env.getStreamGraph

    val src = env.generateSequence(0, 0)

    val mapFunction = new MapFunction[Long, Int] {
      override def map(value: Long): Int = 0
    };
    val map = src.map(mapFunction)
    assert(mapFunction == getFunctionForDataStream(map))
    assert(getFunctionForDataStream(map.map(x => 0)).isInstanceOf[MapFunction[_, _]])

    val statefulMap1 = src.mapWithState((in, state: Option[Long]) => (in, None))
    assert(getFunctionForDataStream(statefulMap1).isInstanceOf[MapFunction[_,_]])
    assert(!getFunctionForDataStream(statefulMap1).
        asInstanceOf[StatefulFunction[_,_,_]].partitioned)
    
    val statefulMap2 = src.keyBy(x=>x).mapWithState(
        (in, state: Option[Long]) => (in, None))
    assert(getFunctionForDataStream(statefulMap2).
        asInstanceOf[StatefulFunction[_,_,_]].partitioned)
    
    val flatMapFunction = new FlatMapFunction[Long, Int] {
      override def flatMap(value: Long, out: Collector[Int]): Unit = {}
    }
    val flatMap = src.flatMap(flatMapFunction)
    assert(flatMapFunction == getFunctionForDataStream(flatMap))
    assert(
      getFunctionForDataStream(flatMap
        .flatMap((x: Int, out: Collector[Int]) => {}))
        .isInstanceOf[FlatMapFunction[_, _]])

    val statefulfMap1 = src.flatMapWithState((in, state: Option[Long]) => (List(in), None))
    assert(getFunctionForDataStream(statefulfMap1).isInstanceOf[FlatMapFunction[_, _]])
    assert(!getFunctionForDataStream(statefulfMap1).
        asInstanceOf[StatefulFunction[_, _, _]].partitioned)

    val statefulfMap2 = src.keyBy(x=>x).flatMapWithState(
        (in, state: Option[Long]) => (List(in), None))
    assert(getFunctionForDataStream(statefulfMap2).
        asInstanceOf[StatefulFunction[_, _, _]].partitioned)
   
    val filterFunction = new FilterFunction[Int] {
      override def filter(value: Int): Boolean = false
    }

    val unionFilter = map.union(flatMap).filter(filterFunction)
    assert(filterFunction == getFunctionForDataStream(unionFilter))
    assert(
      getFunctionForDataStream(map
        .filter((x: Int) => true))
        .isInstanceOf[FilterFunction[_]])

    val statefulFilter1 = src.filterWithState((in, state: Option[Long]) => (true, None))
    assert(getFunctionForDataStream(statefulFilter1).isInstanceOf[FilterFunction[_]])
    assert(!getFunctionForDataStream(statefulFilter1).
        asInstanceOf[StatefulFunction[_, _, _]].partitioned)

    val statefulFilter2 = src.keyBy(x=>x).filterWithState(
        (in, state: Option[Long]) => (false, None))
    assert(getFunctionForDataStream(statefulFilter2).
        asInstanceOf[StatefulFunction[_, _, _]].partitioned)
   
    try {
      streamGraph.getStreamEdge(map.getId, unionFilter.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }

    try {
      streamGraph.getStreamEdge(flatMap.getId, unionFilter.getId)
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
    val outputSelectors = streamGraph.getStreamNode(split.getId).getOutputSelectors
    assert(1 == outputSelectors.size)
    assert(outputSelector == outputSelectors.get(0))

    unionFilter.split(x => List("a"))
    val moreOutputSelectors = streamGraph.getStreamNode(split.getId).getOutputSelectors
    assert(2 == moreOutputSelectors.size)

    val select = split.select("a")
    val sink = select.print
    val splitEdge = streamGraph.getStreamEdge(select.getId, sink.getId)
    assert("a" == splitEdge.getSelectedNames.get(0))

    val foldFunction = new FoldFunction[Int, String] {
      override def fold(accumulator: String, value: Int): String = ""
    }
    val fold = map.groupBy(x=>x).fold("", foldFunction)
    assert(foldFunction == getFunctionForDataStream(fold))
    assert(
      getFunctionForDataStream(map.groupBy(x=>x)
        .fold("", (x: String, y: Int) => ""))
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
      streamGraph.getStreamEdge(fold.getId, coMap.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }
    try {
      streamGraph.getStreamEdge(flatMap.getId, coMap.getId)
    }
    catch {
      case e: Throwable => {
        fail(e.getMessage)
      }
    }
  }

  @Test
  def testChannelSelectors {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism)

    val streamGraph = env.getStreamGraph
    val src = env.generateSequence(0, 0)

    val broadcast = src.broadcast
    val broadcastSink = broadcast.print
    val broadcastPartitioner = streamGraph
      .getStreamEdge(broadcast.getId, broadcastSink.getId).getPartitioner
    assert(broadcastPartitioner.isInstanceOf[BroadcastPartitioner[_]])

    val shuffle: DataStream[Long] = src.shuffle
    val shuffleSink = shuffle.print
    val shufflePartitioner = streamGraph
      .getStreamEdge(shuffle.getId, shuffleSink.getId).getPartitioner
    assert(shufflePartitioner.isInstanceOf[ShufflePartitioner[_]])

    val forward: DataStream[Long] = src.forward
    val forwardSink = forward.print
    val forwardPartitioner = streamGraph
      .getStreamEdge(forward.getId, forwardSink.getId).getPartitioner
    assert(forwardPartitioner.isInstanceOf[RebalancePartitioner[_]])

    val rebalance: DataStream[Long] = src.rebalance
    val rebalanceSink = rebalance.print
    val rebalancePartitioner = streamGraph
      .getStreamEdge(rebalance.getId, rebalanceSink.getId).getPartitioner
    assert(rebalancePartitioner.isInstanceOf[RebalancePartitioner[_]])

    val global: DataStream[Long] = src.global
    val globalSink = global.print
    val globalPartitioner = streamGraph
      .getStreamEdge(global.getId, globalSink.getId).getPartitioner
    assert(globalPartitioner.isInstanceOf[GlobalPartitioner[_]])
  }

  @Test
  def testIterations {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements(1, 2, 3)

    val iterated = source.iterate((input: ConnectedDataStream[Int, String]) => {
      val head = input.map(i => (i + 1).toString, s => s)
      (head.filter(_ == "2"), head.filter(_ != "2"))
    }, 1000)

    val iterated2 = source.iterate((input: DataStream[Int]) => 
      (input.map(_ + 1), input.map(_.toString)), 2000)

    try {
      val invalid = source.iterate((input: ConnectedDataStream[Int, String]) => {
        val head = input.partitionByHash(1, 1).map(i => (i + 1).toString, s => s)
        (head.filter(_ == "2"), head.filter(_ != "2"))
      }, 1000)
      fail
    } catch {
      case uoe: UnsupportedOperationException =>
      case e: Exception => fail
    }

    val sg = env.getStreamGraph

    assert(sg.getStreamLoops().size() == 2)
  }

  /////////////////////////////////////////////////////////////
  // Utilities
  /////////////////////////////////////////////////////////////

  private def getFunctionForDataStream(dataStream: DataStream[_]): Function = {
    val operator = getOperatorForDataStream(dataStream)
      .asInstanceOf[AbstractUdfStreamOperator[_, _]]
    return operator.getUserFunction.asInstanceOf[Function]
  }

  private def getOperatorForDataStream(dataStream: DataStream[_]): StreamOperator[_] = {
    val env = dataStream.getJavaStream.getExecutionEnvironment
    val streamGraph: StreamGraph = env.getStreamGraph
    streamGraph.getStreamNode(dataStream.getId).getOperator
  }

  private def isPartitioned(edge: StreamEdge): Boolean = {
    return edge.getPartitioner.isInstanceOf[FieldsPartitioner[_]]
  }

  private def isCustomPartitioned(edge: StreamEdge): Boolean = {
    return edge.getPartitioner.isInstanceOf[CustomPartitionerWrapper[_, _]]
  }

  private def createDownStreamId(dataStream: DataStream[_]): Integer = {
    return dataStream.print.getId
  }

  private def createDownStreamId(dataStream: ConnectedDataStream[_, _]): Integer = {
    return dataStream.map(x => 0, x => 0).getId
  }

}
