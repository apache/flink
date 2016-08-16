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

package org.apache.flink.streaming.api.graph;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.HashKeyGroupAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.util.NoOpIntMap;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class StreamingJobGraphGeneratorTest extends TestLogger {
	
	@Test
	public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
		final long seed = System.currentTimeMillis();
		final Random r = new Random(seed);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamGraph streamingJob = new StreamGraph(env);
		StreamingJobGraphGenerator compiler = new StreamingJobGraphGenerator(streamingJob);
		
		boolean closureCleanerEnabled = r.nextBoolean(), forceAvroEnabled = r.nextBoolean(), forceKryoEnabled = r.nextBoolean(), objectReuseEnabled = r.nextBoolean(), sysoutLoggingEnabled = r.nextBoolean();
		int dop = 1 + r.nextInt(10);
		
		ExecutionConfig config = streamingJob.getExecutionConfig();
		if(closureCleanerEnabled) {
			config.enableClosureCleaner();
		} else {
			config.disableClosureCleaner();
		}
		if(forceAvroEnabled) {
			config.enableForceAvro();
		} else {
			config.disableForceAvro();
		}
		if(forceKryoEnabled) {
			config.enableForceKryo();
		} else {
			config.disableForceKryo();
		}
		if(objectReuseEnabled) {
			config.enableObjectReuse();
		} else {
			config.disableObjectReuse();
		}
		if(sysoutLoggingEnabled) {
			config.enableSysoutLogging();
		} else {
			config.disableSysoutLogging();
		}
		config.setParallelism(dop);
		
		JobGraph jobGraph = compiler.createJobGraph();

		final String EXEC_CONFIG_KEY = "runtime.config";

		InstantiationUtil.writeObjectToConfig(jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			EXEC_CONFIG_KEY);

		SerializedValue<ExecutionConfig> serializedExecutionConfig = InstantiationUtil.readObjectFromConfig(
				jobGraph.getJobConfiguration(),
				EXEC_CONFIG_KEY,
				Thread.currentThread().getContextClassLoader());

		assertNotNull(serializedExecutionConfig);

		ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(getClass().getClassLoader());

		assertEquals(closureCleanerEnabled, executionConfig.isClosureCleanerEnabled());
		assertEquals(forceAvroEnabled, executionConfig.isForceAvroEnabled());
		assertEquals(forceKryoEnabled, executionConfig.isForceKryoEnabled());
		assertEquals(objectReuseEnabled, executionConfig.isObjectReuseEnabled());
		assertEquals(sysoutLoggingEnabled, executionConfig.isSysoutLoggingEnabled());
		assertEquals(dop, executionConfig.getParallelism());
	}
	
	@Test
	public void testParallelismOneNotChained() {

		// --------- the program ---------

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> input = env
				.fromElements("a", "b", "c", "d", "e", "f")
				.map(new MapFunction<String, Tuple2<String, String>>() {
					private static final long serialVersionUID = 471891682418382583L;

					@Override
					public Tuple2<String, String> map(String value) {
						return new Tuple2<>(value, value);
					}
				});

		DataStream<Tuple2<String, String>> result = input
				.keyBy(0)
				.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

					private static final long serialVersionUID = 3583760206245136188L;

					@Override
					public Tuple2<String, String> map(Tuple2<String, String> value) {
						return value;
					}
				});

		result.addSink(new SinkFunction<Tuple2<String, String>>() {
			private static final long serialVersionUID = -5614849094269539342L;

			@Override
			public void invoke(Tuple2<String, String> value) {}
		});

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		
		assertEquals(2, jobGraph.getNumberOfVertices());
		assertEquals(1, jobGraph.getVerticesAsArray()[0].getParallelism());
		assertEquals(1, jobGraph.getVerticesAsArray()[1].getParallelism());
	}

	/**
	 * Tests that the KeyGroupAssigner is properly set in the {@link StreamConfig} if the max
	 * parallelism is set for the whole job.
	 */
	@Test
	public void testKeyGroupAssignerProperlySet() {
		int maxParallelism = 42;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setMaxParallelism(maxParallelism);

		DataStream<Integer> input = env.fromElements(1, 2, 3);

		DataStream<Integer> keyedResult = input.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 350461576474507944L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap());

		keyedResult.addSink(new DiscardingSink<Integer>());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(maxParallelism, jobVertices.get(1).getMaxParallelism());

		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner = extractHashKeyGroupAssigner(jobVertices.get(1));

		assertEquals(maxParallelism, hashKeyGroupAssigner.getNumberKeyGroups());
	}

	/**
	 * Tests that the key group assigner for the keyed streams in the stream config is properly
	 * initialized with the max parallelism value if there is no max parallelism defined for the
	 * whole job.
	 */
	@Test
	public void testKeyGroupAssignerProperlySetAutoMaxParallelism() {
		int globalParallelism = 42;
		int mapParallelism = 17;
		int maxParallelism = 43;
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(globalParallelism);

		DataStream<Integer> source = env.fromElements(1, 2, 3);

		DataStream<Integer> keyedResult1 = source.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 9205556348021992189L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap());

		DataStream<Integer> keyedResult2 = keyedResult1.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setParallelism(mapParallelism);

		DataStream<Integer> keyedResult3 = keyedResult2.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setMaxParallelism(maxParallelism);

		DataStream<Integer> keyedResult4 = keyedResult3.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setMaxParallelism(maxParallelism).setParallelism(mapParallelism);

		keyedResult4.addSink(new DiscardingSink<Integer>());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> vertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		JobVertex keyedResultJV1 = vertices.get(1);
		JobVertex keyedResultJV2 = vertices.get(2);
		JobVertex keyedResultJV3 = vertices.get(3);
		JobVertex keyedResultJV4 = vertices.get(4);

		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner1 = extractHashKeyGroupAssigner(keyedResultJV1);
		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner2 = extractHashKeyGroupAssigner(keyedResultJV2);
		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner3 = extractHashKeyGroupAssigner(keyedResultJV3);
		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner4 = extractHashKeyGroupAssigner(keyedResultJV4);

		assertEquals(globalParallelism, hashKeyGroupAssigner1.getNumberKeyGroups());
		assertEquals(mapParallelism, hashKeyGroupAssigner2.getNumberKeyGroups());
		assertEquals(maxParallelism, hashKeyGroupAssigner3.getNumberKeyGroups());
		assertEquals(maxParallelism, hashKeyGroupAssigner4.getNumberKeyGroups());
	}

	/**
	 * Tests that the {@link KeyGroupAssigner} is properly set in the {@link StreamConfig} for
	 * connected streams.
	 */
	@Test
	public void testMaxParallelismWithConnectedKeyedStream() {
		int maxParallelism = 42;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> input1 = env.fromElements(1, 2, 3, 4).setMaxParallelism(128).name("input1");
		DataStream<Integer> input2 = env.fromElements(1, 2, 3, 4).setMaxParallelism(129).name("input2");

		env.getConfig().setMaxParallelism(maxParallelism);

		DataStream<Integer> keyedResult = input1.connect(input2).keyBy(
			new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = -6908614081449363419L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			},
			new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = 3195683453223164931L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			}).map(new StreamGraphGeneratorTest.NoOpIntCoMap());

		keyedResult.addSink(new DiscardingSink<Integer>());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();

		JobVertex input1JV = jobVertices.get(0);
		JobVertex input2JV = jobVertices.get(1);
		JobVertex connectedJV = jobVertices.get(2);

		// disambiguate the partial order of the inputs
		if (input1JV.getName().equals("Source: input1")) {
			assertEquals(128, input1JV.getMaxParallelism());
			assertEquals(129, input2JV.getMaxParallelism());
		} else {
			assertEquals(128, input2JV.getMaxParallelism());
			assertEquals(129, input1JV.getMaxParallelism());
		}

		assertEquals(maxParallelism, connectedJV.getMaxParallelism());

		HashKeyGroupAssigner<Integer> hashKeyGroupAssigner = extractHashKeyGroupAssigner(connectedJV);

		assertEquals(maxParallelism, hashKeyGroupAssigner.getNumberKeyGroups());
	}

	/**
	 * Tests that the {@link JobGraph} creation fails if the parallelism is greater than the max
	 * parallelism.
	 */
	@Test(expected=IllegalStateException.class)
	public void testFailureOfJobJobCreationIfParallelismGreaterThanMaxParallelism() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setMaxParallelism(42);

		DataStream<Integer> input = env.fromElements(1, 2, 3, 4);

		DataStream<Integer> result = input.map(new NoOpIntMap()).setParallelism(43);

		result.addSink(new DiscardingSink<Integer>());

		env.getStreamGraph().getJobGraph();

		fail("The JobGraph should not have been created because the parallelism is greater than " +
			"the max parallelism.");
	}

	private HashKeyGroupAssigner<Integer> extractHashKeyGroupAssigner(JobVertex jobVertex) {
		Configuration config = jobVertex.getConfiguration();

		StreamConfig streamConfig = new StreamConfig(config);

		KeyGroupAssigner<Integer> keyGroupAssigner = streamConfig.getKeyGroupAssigner(getClass().getClassLoader());

		assertTrue(keyGroupAssigner instanceof HashKeyGroupAssigner);

		return (HashKeyGroupAssigner<Integer>) keyGroupAssigner;
	}
}
