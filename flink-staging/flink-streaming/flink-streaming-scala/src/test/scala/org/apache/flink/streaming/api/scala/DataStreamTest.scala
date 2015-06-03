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

import org.apache.flink.streaming.api.graph.{StreamEdge, StreamGraph}
import org.apache.flink.streaming.api.windowing.helper.Count
import org.apache.flink.streaming.runtime.partitioner.FieldsPartitioner
import org.apache.flink.util.Collector
import org.junit.Test

class DataStreamTest {

  private val parallelism = 2;

  @Test
  def testNaming(): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironment(parallelism);

    val source1 = env.generateSequence(0, 0).name("testSource1")
    assert("testSource1" == source1.getName)

    val dataStream1 = source1
      .map(x => 0L)
      .name("testMap")
    assert("testMap" == dataStream1.getName)

    val dataStream2 = env.generateSequence(0, 0).name("testSource2")
      .reduce((x, y) => 0)
      .name("testReduce")
    assert("testReduce" == dataStream2.getName)

    val connected = dataStream1.connect(dataStream2)
      .flatMap(
    { (in, out: Collector[Long]) => }, { (in, out: Collector[Long]) => }
    ).name("testCoFlatMap")
    assert("testCoFlatMap" == connected.getName)

    val fu: ((Long, Long) => Long) =
      (x: Long, y: Long) => 0L

    val windowed = connected.window(Count.of(10))
      .foldWindow(0L, fu)

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

    assert(isPartitioned(graph.getStreamEdge(group1.getId, createDownStreamId(group1))));
    assert(isPartitioned(graph.getStreamEdge(group2.getId, createDownStreamId(group2))));
    assert(isPartitioned(graph.getStreamEdge(group3.getId, createDownStreamId(group3))));
    assert(isPartitioned(graph.getStreamEdge(group4.getId, createDownStreamId(group4))));

    //Testing ConnectedDataStream grouping
    val connectedGroup1: ConnectedDataStream[_, _] = connected.groupBy(0, 0)
    val downStreamId1: Integer = createDownStreamId(connectedGroup1)

    val connectedGroup2: ConnectedDataStream[_, _] = connected.groupBy(Array[Int](0), Array[Int](0))
    val downStreamId2: Integer = createDownStreamId(connectedGroup2)

    val connectedGroup3: ConnectedDataStream[_, _] = connected.groupBy("_1", "_1")
    val downStreamId3: Integer = createDownStreamId(connectedGroup3)

    val connectedGroup4: ConnectedDataStream[_, _] = connected.groupBy(Array[String]("_1"), Array[String]("_1"))
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

    val connectedPartition2: ConnectedDataStream[_, _] = connected.partitionByHash(Array[Int](0), Array[Int](0))
    val connectDownStreamId2: Integer = createDownStreamId(connectedPartition2)

    val connectedPartition3: ConnectedDataStream[_, _] = connected.partitionByHash("_1", "_1")
    val connectDownStreamId3: Integer = createDownStreamId(connectedPartition3)

    val connectedPartition4: ConnectedDataStream[_, _] = connected.partitionByHash(Array[String]("_1"), Array[String]("_1"))
    val connectDownStreamId4: Integer = createDownStreamId(connectedPartition4)

    val connectedPartition5: ConnectedDataStream[_, _] = connected.partitionByHash(x => x._1, x => x._1)
    val connectDownStreamId5: Integer = createDownStreamId(connectedPartition5)

    assert(isPartitioned(graph.getStreamEdge(connectedPartition1.getFirst.getId, connectDownStreamId1)))
    assert(isPartitioned(graph.getStreamEdge(connectedPartition1.getSecond.getId, connectDownStreamId1)))

    assert(isPartitioned(graph.getStreamEdge(connectedPartition2.getFirst.getId, connectDownStreamId2)))
    assert(isPartitioned(graph.getStreamEdge(connectedPartition2.getSecond.getId, connectDownStreamId2)))

    assert(isPartitioned(graph.getStreamEdge(connectedPartition3.getFirst.getId, connectDownStreamId3)))
    assert(isPartitioned(graph.getStreamEdge(connectedPartition3.getSecond.getId, connectDownStreamId3)))

    assert(isPartitioned(graph.getStreamEdge(connectedPartition4.getFirst.getId, connectDownStreamId4)))
    assert(isPartitioned(graph.getStreamEdge(connectedPartition4.getSecond.getId, connectDownStreamId4)))

    assert(isPartitioned(graph.getStreamEdge(connectedPartition5.getFirst.getId, connectDownStreamId5)))
    assert(isPartitioned(graph.getStreamEdge(connectedPartition5.getSecond.getId, connectDownStreamId5)))
  }

  private def isPartitioned(edge: StreamEdge): Boolean = {
    return edge.getPartitioner.isInstanceOf[FieldsPartitioner[_]]
  }

  private def createDownStreamId(dataStream: DataStream[_]): Integer = {
    return dataStream.print.getId
  }

  private def createDownStreamId(dataStream: ConnectedDataStream[_, _]): Integer = {
    return dataStream.map(x  => 0, x => 0).getId
  }

}
