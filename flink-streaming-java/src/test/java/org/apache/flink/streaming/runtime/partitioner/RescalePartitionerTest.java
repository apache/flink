/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.RestartAllStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RescalePartitioner}.
 */
@SuppressWarnings("serial")
public class RescalePartitionerTest extends TestLogger {

	private RescalePartitioner<Tuple> distributePartitioner;
	private StreamRecord<Tuple> streamRecord = new StreamRecord<Tuple>(null);
	private SerializationDelegate<StreamRecord<Tuple>> sd = new SerializationDelegate<StreamRecord<Tuple>>(
			null);

	@Before
	public void setPartitioner() {
		distributePartitioner = new RescalePartitioner<Tuple>();
	}

	@Test
	public void testSelectChannelsLength() {
		sd.setInstance(streamRecord);
		assertEquals(1, distributePartitioner.selectChannels(sd, 1).length);
		assertEquals(1, distributePartitioner.selectChannels(sd, 2).length);
		assertEquals(1, distributePartitioner.selectChannels(sd, 1024).length);
	}

	@Test
	public void testSelectChannelsInterval() {
		sd.setInstance(streamRecord);
		assertEquals(0, distributePartitioner.selectChannels(sd, 3)[0]);
		assertEquals(1, distributePartitioner.selectChannels(sd, 3)[0]);
		assertEquals(2, distributePartitioner.selectChannels(sd, 3)[0]);
		assertEquals(0, distributePartitioner.selectChannels(sd, 3)[0]);
	}

	@Test
	public void testExecutionGraphGeneration() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(4);

		// get input data
		DataStream<String> text = env.addSource(new ParallelSourceFunction<String>() {
			private static final long serialVersionUID = 7772338606389180774L;

			@Override
			public void run(SourceContext<String> ctx) throws Exception {

			}

			@Override
			public void cancel() {

			}
		}).setParallelism(2);

		DataStream<Tuple2<String, Integer>> counts = text
			.rescale()
			.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
				private static final long serialVersionUID = -5255930322161596829L;

				@Override
				public void flatMap(String value,
					Collector<Tuple2<String, Integer>> out) throws Exception {

				}
			});

		counts.rescale().print().setParallelism(2);

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		final JobID jobId = new JobID();
		final String jobName = "Semi-Rebalance Test Job";
		final Configuration cfg = new Configuration();

		List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		JobVertex sourceVertex = jobVertices.get(0);
		JobVertex mapVertex = jobVertices.get(1);
		JobVertex sinkVertex = jobVertices.get(2);

		assertEquals(2, sourceVertex.getParallelism());
		assertEquals(4, mapVertex.getParallelism());
		assertEquals(2, sinkVertex.getParallelism());

		ExecutionGraph eg = new ExecutionGraph(
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			jobId,
			jobName,
			cfg,
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy(),
			new RestartAllStrategy.Factory(),
			new ArrayList<>(),
			new ArrayList<>(),
			new Scheduler(TestingUtils.defaultExecutionContext()),
			ExecutionGraph.class.getClassLoader(),
			null);
		try {
			eg.attachJobGraph(jobVertices);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Building ExecutionGraph failed: " + e.getMessage());
		}

		ExecutionJobVertex execSourceVertex = eg.getJobVertex(sourceVertex.getID());
		ExecutionJobVertex execMapVertex = eg.getJobVertex(mapVertex.getID());
		ExecutionJobVertex execSinkVertex = eg.getJobVertex(sinkVertex.getID());

		assertEquals(0, execSourceVertex.getInputs().size());

		assertEquals(1, execMapVertex.getInputs().size());
		assertEquals(4, execMapVertex.getParallelism());
		ExecutionVertex[] mapTaskVertices = execMapVertex.getTaskVertices();

		// verify that we have each parallel input partition exactly twice, i.e. that one source
		// sends to two unique mappers
		Map<Integer, Integer> mapInputPartitionCounts = new HashMap<>();
		for (ExecutionVertex mapTaskVertex: mapTaskVertices) {
			assertEquals(1, mapTaskVertex.getNumberOfInputs());
			assertEquals(1, mapTaskVertex.getInputEdges(0).length);
			ExecutionEdge inputEdge = mapTaskVertex.getInputEdges(0)[0];
			assertEquals(sourceVertex.getID(), inputEdge.getSource().getProducer().getJobvertexId());
			int inputPartition = inputEdge.getSource().getPartitionNumber();
			if (!mapInputPartitionCounts.containsKey(inputPartition)) {
				mapInputPartitionCounts.put(inputPartition, 1);
			} else {
				mapInputPartitionCounts.put(inputPartition, mapInputPartitionCounts.get(inputPartition) + 1);
			}
		}

		assertEquals(2, mapInputPartitionCounts.size());
		for (int count: mapInputPartitionCounts.values()) {
			assertEquals(2, count);
		}

		assertEquals(1, execSinkVertex.getInputs().size());
		assertEquals(2, execSinkVertex.getParallelism());
		ExecutionVertex[] sinkTaskVertices = execSinkVertex.getTaskVertices();

		// verify each sink instance has two inputs from the map and that each map subpartition
		// only occurs in one unique input edge
		Set<Integer> mapSubpartitions = new HashSet<>();
		for (ExecutionVertex sinkTaskVertex: sinkTaskVertices) {
			assertEquals(1, sinkTaskVertex.getNumberOfInputs());
			assertEquals(2, sinkTaskVertex.getInputEdges(0).length);
			ExecutionEdge inputEdge1 = sinkTaskVertex.getInputEdges(0)[0];
			ExecutionEdge inputEdge2 = sinkTaskVertex.getInputEdges(0)[1];
			assertEquals(mapVertex.getID(), inputEdge1.getSource().getProducer().getJobvertexId());
			assertEquals(mapVertex.getID(), inputEdge2.getSource().getProducer().getJobvertexId());

			int inputPartition1 = inputEdge1.getSource().getPartitionNumber();
			assertFalse(mapSubpartitions.contains(inputPartition1));
			mapSubpartitions.add(inputPartition1);
			int inputPartition2 = inputEdge2.getSource().getPartitionNumber();
			assertFalse(mapSubpartitions.contains(inputPartition2));
			mapSubpartitions.add(inputPartition2);
		}

		assertEquals(4, mapSubpartitions.size());
	}
}
