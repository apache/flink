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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.util.TestAnyModeReadingStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamingJobGraphGenerator}.
 */
@SuppressWarnings("serial")
public class StreamingJobGraphGeneratorTest extends TestLogger {

	@Test
	public void testParallelismOneNotChained() {

		// --------- the program ---------

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> input = env
				.fromElements("a", "b", "c", "d", "e", "f")
				.map(new MapFunction<String, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(String value) {
						return new Tuple2<>(value, value);
					}
				});

		DataStream<Tuple2<String, String>> result = input
				.keyBy(0)
				.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(Tuple2<String, String> value) {
						return value;
					}
				});

		result.addSink(new SinkFunction<Tuple2<String, String>>() {

			@Override
			public void invoke(Tuple2<String, String> value) {}
		});

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(2, jobGraph.getNumberOfVertices());
		assertEquals(1, verticesSorted.get(0).getParallelism());
		assertEquals(1, verticesSorted.get(1).getParallelism());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapSinkVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, mapSinkVertex.getInputs().get(0).getSource().getResultType());
	}

	/**
	 * Tests that disabled checkpointing sets the checkpointing interval to Long.MAX_VALUE.
	 */
	@Test
	public void testDisabledCheckpointing() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamGraph streamGraph = new StreamGraph(env.getConfig(), env.getCheckpointConfig(), SavepointRestoreSettings.none());
		assertFalse("Checkpointing enabled", streamGraph.getCheckpointConfig().isCheckpointingEnabled());

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

		JobCheckpointingSettings snapshottingSettings = jobGraph.getCheckpointingSettings();
		assertEquals(Long.MAX_VALUE, snapshottingSettings.getCheckpointCoordinatorConfiguration().getCheckpointInterval());
	}

	@Test
	public void generatorForwardsSavepointRestoreSettings() {
		StreamGraph streamGraph = new StreamGraph(
				new ExecutionConfig(),
				new CheckpointConfig(),
				SavepointRestoreSettings.forPath("hello"));

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

		SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
		assertThat(savepointRestoreSettings.getRestorePath(), is("hello"));
	}

	/**
	 * Verifies that the chain start/end is correctly set.
	 */
	@Test
	public void testChainStartEndSetting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// set parallelism to 2 to avoid chaining with source in case when available processors is 1.
		env.setParallelism(2);

		// fromElements -> CHAIN(Map -> Print)
		env.fromElements(1, 2, 3)
			.map(new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					return value;
				}
			})
			.print();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapPrintVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, mapPrintVertex.getInputs().get(0).getSource().getResultType());

		StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
		StreamConfig mapConfig = new StreamConfig(mapPrintVertex.getConfiguration());
		Map<Integer, StreamConfig> chainedConfigs = mapConfig.getTransitiveChainedTaskConfigs(getClass().getClassLoader());
		StreamConfig printConfig = chainedConfigs.values().iterator().next();

		assertTrue(sourceConfig.isChainStart());
		assertTrue(sourceConfig.isChainEnd());

		assertTrue(mapConfig.isChainStart());
		assertFalse(mapConfig.isChainEnd());

		assertFalse(printConfig.isChainStart());
		assertTrue(printConfig.isChainEnd());
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators (covers source and sink cases)
	 * when generating job graph.
	 */
	@Test
	public void testResourcesForChainedSourceSink() throws Exception {
		ResourceSpec resource1 = ResourceSpec.newBuilder(0.1, 100).build();
		ResourceSpec resource2 = ResourceSpec.newBuilder(0.2, 200).build();
		ResourceSpec resource3 = ResourceSpec.newBuilder(0.3, 300).build();
		ResourceSpec resource4 = ResourceSpec.newBuilder(0.4, 400).build();
		ResourceSpec resource5 = ResourceSpec.newBuilder(0.5, 500).build();

		Method opMethod = getSetResourcesMethodAndSetAccessible(SingleOutputStreamOperator.class);
		Method sinkMethod = getSetResourcesMethodAndSetAccessible(DataStreamSink.class);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Integer, Integer>> source = env.addSource(new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		});
		opMethod.invoke(source, resource1);

		DataStream<Tuple2<Integer, Integer>> map = source.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				return value;
			}
		});
		opMethod.invoke(map, resource2);

		// CHAIN(Source -> Map -> Filter)
		DataStream<Tuple2<Integer, Integer>> filter = map.filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
				return false;
			}
		});
		opMethod.invoke(filter, resource3);

		DataStream<Tuple2<Integer, Integer>> reduce = filter.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
				return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
			}
		});
		opMethod.invoke(reduce, resource4);

		DataStreamSink<Tuple2<Integer, Integer>> sink = reduce.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void invoke(Tuple2<Integer, Integer> value) throws Exception {
			}
		});
		sinkMethod.invoke(sink, resource5);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		JobVertex sourceMapFilterVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		JobVertex reduceSinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

		assertTrue(sourceMapFilterVertex.getMinResources().equals(resource3.merge(resource2).merge(resource1)));
		assertTrue(reduceSinkVertex.getPreferredResources().equals(resource4.merge(resource5)));
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators (covers middle chaining and iteration cases)
	 * when generating job graph.
	 */
	@Test
	public void testResourcesForIteration() throws Exception {
		ResourceSpec resource1 = ResourceSpec.newBuilder(0.1, 100).build();
		ResourceSpec resource2 = ResourceSpec.newBuilder(0.2, 200).build();
		ResourceSpec resource3 = ResourceSpec.newBuilder(0.3, 300).build();
		ResourceSpec resource4 = ResourceSpec.newBuilder(0.4, 400).build();
		ResourceSpec resource5 = ResourceSpec.newBuilder(0.5, 500).build();

		Method opMethod = getSetResourcesMethodAndSetAccessible(SingleOutputStreamOperator.class);
		Method sinkMethod = getSetResourcesMethodAndSetAccessible(DataStreamSink.class);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.addSource(new ParallelSourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		}).name("test_source");
		opMethod.invoke(source, resource1);

		IterativeStream<Integer> iteration = source.iterate(3000);
		opMethod.invoke(iteration, resource2);

		DataStream<Integer> flatMap = iteration.flatMap(new FlatMapFunction<Integer, Integer>() {
			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				out.collect(value);
			}
		}).name("test_flatMap");
		opMethod.invoke(flatMap, resource3);

		// CHAIN(flatMap -> Filter)
		DataStream<Integer> increment = flatMap.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return false;
			}
		}).name("test_filter");
		opMethod.invoke(increment, resource4);

		DataStreamSink<Integer> sink = iteration.closeWith(increment).addSink(new SinkFunction<Integer>() {
			@Override
			public void invoke(Integer value) throws Exception {
			}
		}).disableChaining().name("test_sink");
		sinkMethod.invoke(sink, resource5);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			if (jobVertex.getName().contains("test_source")) {
				assertTrue(jobVertex.getMinResources().equals(resource1));
			} else if (jobVertex.getName().contains("Iteration_Source")) {
				assertTrue(jobVertex.getPreferredResources().equals(resource2));
			} else if (jobVertex.getName().contains("test_flatMap")) {
				assertTrue(jobVertex.getMinResources().equals(resource3.merge(resource4)));
			} else if (jobVertex.getName().contains("Iteration_Tail")) {
				assertTrue(jobVertex.getPreferredResources().equals(ResourceSpec.DEFAULT));
			} else if (jobVertex.getName().contains("test_sink")) {
				assertTrue(jobVertex.getMinResources().equals(resource5));
			}
		}
	}

	@Test
	public void testInputOutputFormat() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Long> source = env.addSource(
			new InputFormatSourceFunction<>(
				new TypeSerializerInputFormat<>(TypeInformation.of(Long.class)),
				TypeInformation.of(Long.class)),
			TypeInformation.of(Long.class)).name("source");

		source.writeUsingOutputFormat(new DiscardingOutputFormat<>()).name("sink1");
		source.writeUsingOutputFormat(new DiscardingOutputFormat<>()).name("sink2");

		StreamGraph streamGraph = env.getStreamGraph();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(1, jobGraph.getNumberOfVertices());

		JobVertex jobVertex = jobGraph.getVertices().iterator().next();
		assertTrue(jobVertex instanceof InputOutputFormatVertex);

		InputOutputFormatContainer formatContainer = new InputOutputFormatContainer(
			new TaskConfig(jobVertex.getConfiguration()), Thread.currentThread().getContextClassLoader());
		Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats = formatContainer.getInputFormats();
		Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats = formatContainer.getOutputFormats();
		assertEquals(1, inputFormats.size());
		assertEquals(2, outputFormats.size());

		Map<String, OperatorID> nameToOperatorIds = new HashMap<>();
		StreamConfig headConfig = new StreamConfig(jobVertex.getConfiguration());
		nameToOperatorIds.put(headConfig.getOperatorName(), headConfig.getOperatorID());

		Map<Integer, StreamConfig> chainedConfigs = headConfig
			.getTransitiveChainedTaskConfigs(Thread.currentThread().getContextClassLoader());
		for (StreamConfig config : chainedConfigs.values()) {
			nameToOperatorIds.put(config.getOperatorName(), config.getOperatorID());
		}

		InputFormat<?, ?> sourceFormat = inputFormats.get(nameToOperatorIds.get("Source: source")).getUserCodeObject();
		assertTrue(sourceFormat instanceof TypeSerializerInputFormat);

		OutputFormat<?> sinkFormat1 = outputFormats.get(nameToOperatorIds.get("Sink: sink1")).getUserCodeObject();
		assertTrue(sinkFormat1 instanceof DiscardingOutputFormat);

		OutputFormat<?> sinkFormat2 = outputFormats.get(nameToOperatorIds.get("Sink: sink2")).getUserCodeObject();
		assertTrue(sinkFormat2 instanceof DiscardingOutputFormat);
	}

	/**
	 * Test setting shuffle mode to {@link ShuffleMode#PIPELINED}.
	 */
	@Test
	public void testShuffleModePipelined() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> Map -> Print
		DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);

		DataStream<Integer> partitionAfterSourceDataStream = new DataStream<>(env, new PartitionTransformation<>(
				sourceDataStream.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.PIPELINED));
		DataStream<Integer> mapDataStream = partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

		DataStream<Integer> partitionAfterMapDataStream = new DataStream<>(env, new PartitionTransformation<>(
				mapDataStream.getTransformation(), new RescalePartitioner<>(), ShuffleMode.PIPELINED));
		partitionAfterMapDataStream.print().setParallelism(2);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		// it can be chained with PIPELINED shuffle mode
		JobVertex sourceAndMapVertex = verticesSorted.get(0);

		// PIPELINED shuffle mode is translated into PIPELINED_BOUNDED result partition
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED,
				sourceAndMapVertex.getProducedDataSets().get(0).getResultType());
	}

	/**
	 * Test setting shuffle mode to {@link ShuffleMode#BATCH}.
	 */
	@Test
	public void testShuffleModeBatch() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> Map -> Print
		DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);

		DataStream<Integer> partitionAfterSourceDataStream = new DataStream<>(env, new PartitionTransformation<>(
				sourceDataStream.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.BATCH));
		DataStream<Integer> mapDataStream = partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

		DataStream<Integer> partitionAfterMapDataStream = new DataStream<>(env, new PartitionTransformation<>(
				mapDataStream.getTransformation(), new RescalePartitioner<>(), ShuffleMode.BATCH));
		partitionAfterMapDataStream.print().setParallelism(2);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(3, verticesSorted.size());

		// it can not be chained with BATCH shuffle mode
		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapVertex = verticesSorted.get(1);

		// BATCH shuffle mode is translated into BLOCKING result partition
		assertEquals(ResultPartitionType.BLOCKING,
			sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.BLOCKING,
			mapVertex.getProducedDataSets().get(0).getResultType());
	}

	/**
	 * Test setting shuffle mode to {@link ShuffleMode#UNDEFINED}.
	 */
	@Test
	public void testShuffleModeUndefined() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> Map -> Print
		DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);

		DataStream<Integer> partitionAfterSourceDataStream = new DataStream<>(env, new PartitionTransformation<>(
				sourceDataStream.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.UNDEFINED));
		DataStream<Integer> mapDataStream = partitionAfterSourceDataStream.map(value -> value).setParallelism(1);

		DataStream<Integer> partitionAfterMapDataStream = new DataStream<>(env, new PartitionTransformation<>(
				mapDataStream.getTransformation(), new RescalePartitioner<>(), ShuffleMode.UNDEFINED));
		partitionAfterMapDataStream.print().setParallelism(2);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		// it can be chained with UNDEFINED shuffle mode
		JobVertex sourceAndMapVertex = verticesSorted.get(0);

		// UNDEFINED shuffle mode is translated into PIPELINED_BOUNDED result partition by default
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED,
			sourceAndMapVertex.getProducedDataSets().get(0).getResultType());
	}

	/**
	 * Test iteration job, check slot sharing group and co-location group.
	 */
	@Test
	public void testIteration() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 2, 3).name("source");
		IterativeStream<Integer> iteration = source.iterate(3000);
		iteration.name("iteration").setParallelism(2);
		DataStream<Integer> map = iteration.map(x -> x + 1).name("map").setParallelism(2);
		DataStream<Integer> filter = map.filter((x) -> false).name("filter").setParallelism(2);
		iteration.closeWith(filter).print();

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		SlotSharingGroup slotSharingGroup = jobGraph.getVerticesAsArray()[0].getSlotSharingGroup();
		assertNotNull(slotSharingGroup);

		CoLocationGroup iterationSourceCoLocationGroup = null;
		CoLocationGroup iterationSinkCoLocationGroup = null;

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			// all vertices have same slot sharing group by default
			assertEquals(slotSharingGroup, jobVertex.getSlotSharingGroup());

			// all iteration vertices have same co-location group,
			// others have no co-location group by default
			if (jobVertex.getName().startsWith(StreamGraph.ITERATION_SOURCE_NAME_PREFIX)) {
				iterationSourceCoLocationGroup = jobVertex.getCoLocationGroup();
				assertTrue(iterationSourceCoLocationGroup.getVertices().contains(jobVertex));
			} else if (jobVertex.getName().startsWith(StreamGraph.ITERATION_SINK_NAME_PREFIX)) {
				iterationSinkCoLocationGroup = jobVertex.getCoLocationGroup();
				assertTrue(iterationSinkCoLocationGroup.getVertices().contains(jobVertex));
			} else {
				assertNull(jobVertex.getCoLocationGroup());
			}
		}

		assertNotNull(iterationSourceCoLocationGroup);
		assertNotNull(iterationSinkCoLocationGroup);
		assertEquals(iterationSourceCoLocationGroup, iterationSinkCoLocationGroup);
	}

	/**
	 * Test default schedule mode.
	 */
	@Test
	public void testDefaultScheduleMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// use eager schedule mode by default
		StreamGraph streamGraph = new StreamGraphGenerator(Collections.emptyList(),
			env.getConfig(), env.getCheckpointConfig())
			.generate();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(ScheduleMode.EAGER, jobGraph.getScheduleMode());
	}

	/**
	 * Test schedule mode is configurable or not.
	 */
	@Test
	public void testSetScheduleMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamGraph streamGraph = new StreamGraphGenerator(Collections.emptyList(),
			env.getConfig(), env.getCheckpointConfig())
			.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES)
			.generate();
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		assertEquals(ScheduleMode.LAZY_FROM_SOURCES, jobGraph.getScheduleMode());
	}

	/**
	 * Verify that "blockingConnectionsBetweenChains" is off by default.
	 */
	@Test
	public void testBlockingAfterChainingOffDisabled() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> Filter -> Print
		DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);

		// partition transformation with an undefined shuffle mode between source and filter
		DataStream<Integer> partitionAfterSourceDataStream = new DataStream<>(env, new PartitionTransformation<>(
			sourceDataStream.getTransformation(), new RescalePartitioner<>(), ShuffleMode.UNDEFINED));
		DataStream<Integer> filterDataStream = partitionAfterSourceDataStream.filter(value -> true).setParallelism(2);

		DataStream<Integer> partitionAfterFilterDataStream = new DataStream<>(env, new PartitionTransformation<>(
			filterDataStream.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.UNDEFINED));

		partitionAfterFilterDataStream.print().setParallelism(2);

		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex filterAndPrintVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED,
				filterAndPrintVertex.getInputs().get(0).getSource().getResultType());
	}

	/**
	 * Test enabling the property "blockingConnectionsBetweenChains".
	 */
	@Test
	public void testBlockingConnectionsBetweenChainsEnabled() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> Filter -> Map -> Print
		DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);

		// partition transformation with an undefined shuffle mode between source and filter
		DataStream<Integer> partitionAfterSourceDataStream = new DataStream<>(env, new PartitionTransformation<>(
			sourceDataStream.getTransformation(), new RescalePartitioner<>(), ShuffleMode.UNDEFINED));
		DataStream<Integer> filterDataStream = partitionAfterSourceDataStream.filter(value -> true).setParallelism(2);

		DataStream<Integer> partitionAfterFilterDataStream = new DataStream<>(env, new PartitionTransformation<>(
			filterDataStream.getTransformation(), new ForwardPartitioner<>(), ShuffleMode.UNDEFINED));
		partitionAfterFilterDataStream.map(value -> value).setParallelism(2);

		DataStream<Integer> partitionAfterMapDataStream = new DataStream<>(env, new PartitionTransformation<>(
			filterDataStream.getTransformation(), new RescalePartitioner<>(), ShuffleMode.PIPELINED));
		partitionAfterMapDataStream.print().setParallelism(1);

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setBlockingConnectionsBetweenChains(true);
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(3, verticesSorted.size());

		JobVertex sourceVertex = verticesSorted.get(0);
		// still can be chained
		JobVertex filterAndMapVertex = verticesSorted.get(1);
		JobVertex printVertex = verticesSorted.get(2);

		// the edge with undefined shuffle mode is translated into BLOCKING
		assertEquals(ResultPartitionType.BLOCKING, sourceVertex.getProducedDataSets().get(0).getResultType());
		// the edge with PIPELINED shuffle mode is translated into PIPELINED_BOUNDED
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, filterAndMapVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, printVertex.getInputs().get(0).getSource().getResultType());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testNotSupportInputSelectableOperatorIfCheckpointing() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(60_000L);

		DataStreamSource<String> source1 = env.fromElements("1");
		DataStreamSource<Integer> source2 = env.fromElements(1);
		source1.connect(source2)
			.transform("test",
				BasicTypeInfo.STRING_TYPE_INFO,
				new TestAnyModeReadingStreamOperator("test operator"))
			.print();

		StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
	}

	@Test
	public void testManagedMemoryFractionForSpecifiedResourceSpec() throws Exception {
		final ResourceSpec resource1 = ResourceSpec.newBuilder(1, 100)
			.setOnHeapManagedMemory(new MemorySize(100))
			.setOffHeapManagedMemory(new MemorySize(40))
			.build();
		final ResourceSpec resource2 = ResourceSpec.newBuilder(1, 100)
			.setOnHeapManagedMemory(new MemorySize(300))
			.setOffHeapManagedMemory(new MemorySize(50))
			.build();
		final ResourceSpec resource3 = ResourceSpec.newBuilder(1, 100)
			.setOnHeapManagedMemory(new MemorySize(600))
			.setOffHeapManagedMemory(new MemorySize(10))
			.build();
		final ResourceSpec resource4 = ResourceSpec.newBuilder(1, 100)
			.setOnHeapManagedMemory(new MemorySize(123))
			.setOffHeapManagedMemory(new MemorySize(456))
			.build();

		// v1(source -> map1), v2(map2) are in the same slot sharing group, v3(map3) is in a different group
		final JobGraph jobGraph = createJobGraphForManagedMemoryFractionTest(resource1, resource2, resource3, resource4);
		final JobVertex vertex1 = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		final JobVertex vertex2 = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		final JobVertex vertex3 = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);

		final StreamConfig sourceConfig = new StreamConfig(vertex1.getConfiguration());
		assertEquals(0.1, sourceConfig.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(0.4, sourceConfig.getManagedMemoryFractionOffHeap(), 0.000001);

		final StreamConfig map1Config = Iterables.getOnlyElement(
			sourceConfig.getTransitiveChainedTaskConfigs(StreamingJobGraphGeneratorTest.class.getClassLoader()).values());
		assertEquals(0.3, map1Config.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(0.5, map1Config.getManagedMemoryFractionOffHeap(), 0.000001);

		final StreamConfig map2Config = new StreamConfig(vertex2.getConfiguration());
		assertEquals(0.6, map2Config.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(0.1, map2Config.getManagedMemoryFractionOffHeap(), 0.000001);

		final StreamConfig map3Config = new StreamConfig(vertex3.getConfiguration());
		assertEquals(1.0, map3Config.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(1.0, map3Config.getManagedMemoryFractionOffHeap(), 0.000001);
	}

	@Test
	public void testManagedMemoryFractionForUnknownResourceSpec() throws Exception {
		final ResourceSpec resource1 = ResourceSpec.UNKNOWN;
		final ResourceSpec resource2 = ResourceSpec.UNKNOWN;
		final ResourceSpec resource3 = ResourceSpec.UNKNOWN;
		final ResourceSpec resource4 = ResourceSpec.UNKNOWN;

		// v1(source -> map1), v2(map2) are in the same slot sharing group, v3(map3) is in a different group
		final JobGraph jobGraph = createJobGraphForManagedMemoryFractionTest(resource1, resource2, resource3, resource4);
		final JobVertex vertex1 = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		final JobVertex vertex2 = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		final JobVertex vertex3 = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);

		final StreamConfig sourceConfig = new StreamConfig(vertex1.getConfiguration());
		assertEquals(1.0 / 3, sourceConfig.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(1.0 / 3, sourceConfig.getManagedMemoryFractionOffHeap(), 0.000001);

		final StreamConfig map1Config = Iterables.getOnlyElement(
			sourceConfig.getTransitiveChainedTaskConfigs(StreamingJobGraphGeneratorTest.class.getClassLoader()).values());
		assertEquals(1.0 / 3, map1Config.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(1.0 / 3, map1Config.getManagedMemoryFractionOffHeap(), 0.000001);

		final StreamConfig map2Config = new StreamConfig(vertex2.getConfiguration());
		assertEquals(1.0 / 3, map2Config.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(1.0 / 3, map2Config.getManagedMemoryFractionOffHeap(), 0.000001);

		final StreamConfig map3Config = new StreamConfig(vertex3.getConfiguration());
		assertEquals(1.0, map3Config.getManagedMemoryFractionOnHeap(), 0.000001);
		assertEquals(1.0, map3Config.getManagedMemoryFractionOffHeap(), 0.000001);

	}

	private JobGraph createJobGraphForManagedMemoryFractionTest(
		final ResourceSpec resource1,
		final ResourceSpec resource2,
		final ResourceSpec resource3,
		final ResourceSpec resource4) throws Exception {

		final Method opMethod = getSetResourcesMethodAndSetAccessible(SingleOutputStreamOperator.class);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStream<Integer> source = env.addSource(new ParallelSourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) {
			}

			@Override
			public void cancel() {
			}
		});
		opMethod.invoke(source, resource1);

		// CHAIN(source -> map1) in default slot sharing group
		final DataStream<Integer> map1 = source.map((MapFunction<Integer, Integer>) value -> value);
		opMethod.invoke(map1, resource2);

		// CHAIN(map2) in default slot sharing group
		final DataStream<Integer> map2 = map1.rebalance().map((MapFunction<Integer, Integer>) value -> value);
		opMethod.invoke(map2, resource3);

		// CHAIN(map3) in test slot sharing group
		final DataStream<Integer> map3 = map2.rebalance().map(value -> value).slotSharingGroup("test");
		opMethod.invoke(map3, resource4);

		return StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
	}

	@Test
	public void testSlotSharingOnAllVerticesInSameSlotSharingGroupByDefaultEnabled() {
		final StreamGraph streamGraph = createStreamGraphForSlotSharingTest();
		// specify slot sharing group for map1
		streamGraph.getStreamNodes().stream()
			.filter(n -> "map1".equals(n.getOperatorName()))
			.findFirst()
			.get()
			.setSlotSharingGroup("testSlotSharingGroup");
		streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(true);
		final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

		final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(4, verticesSorted.size());

		final JobVertex source1Vertex = verticesSorted.get(0);
		final JobVertex source2Vertex = verticesSorted.get(1);
		final JobVertex map1Vertex = verticesSorted.get(2);
		final JobVertex map2Vertex = verticesSorted.get(3);

		// all vertices should be in the same default slot sharing group
		// except for map1 which has a specified slot sharing group
		assertSameSlotSharingGroup(source1Vertex, source2Vertex, map2Vertex);
		assertDistinctSharingGroups(source1Vertex, map1Vertex);
	}

	@Test
	public void testSlotSharingOnAllVerticesInSameSlotSharingGroupByDefaultDisabled() {
		final StreamGraph streamGraph = createStreamGraphForSlotSharingTest();
		streamGraph.setAllVerticesInSameSlotSharingGroupByDefault(false);
		final JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);

		final List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(4, verticesSorted.size());

		final JobVertex source1Vertex = verticesSorted.get(0);
		final JobVertex source2Vertex = verticesSorted.get(1);
		final JobVertex map1Vertex = verticesSorted.get(2);
		final JobVertex map2Vertex = verticesSorted.get(3);

		// vertices in the same region should be in the same slot sharing group
		assertSameSlotSharingGroup(source1Vertex, map1Vertex);

		// vertices in different regions should be in different slot sharing groups
		assertDistinctSharingGroups(source1Vertex, source2Vertex, map2Vertex);
	}

	/**
	 * Create a StreamGraph as below.
	 *
	 * <p>source1 --(rebalance & pipelined)--> Map1
	 *
	 * <p>source2 --(rebalance & blocking)--> Map2
	 */
	private StreamGraph createStreamGraphForSlotSharingTest() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStream<Integer> source1 = env.fromElements(1, 2, 3).name("source1");
		source1.rebalance().map(v -> v).name("map1");

		final DataStream<Integer> source2 = env.fromElements(4, 5, 6).name("source2");
		final DataStream<Integer> partitioned = new DataStream<>(env, new PartitionTransformation<>(
			source2.getTransformation(), new RebalancePartitioner<>(), ShuffleMode.BATCH));
		partitioned.map(v -> v).name("map2");

		return env.getStreamGraph();
	}

	private void assertSameSlotSharingGroup(JobVertex... vertices) {
		for (int i = 0; i < vertices.length - 1; i++) {
			assertEquals(vertices[i].getSlotSharingGroup(), vertices[i + 1].getSlotSharingGroup());
		}
	}

	private void assertDistinctSharingGroups(JobVertex... vertices) {
		for (int i = 0; i < vertices.length - 1; i++) {
			for (int j = i + 1; j < vertices.length; j++) {
				assertNotEquals(vertices[i].getSlotSharingGroup(), vertices[j].getSlotSharingGroup());
			}
		}
	}

	private static Method getSetResourcesMethodAndSetAccessible(final Class<?> clazz) throws NoSuchMethodException {
		final Method setResourcesMethod = clazz.getDeclaredMethod("setResources", ResourceSpec.class);
		setResourcesMethod.setAccessible(true);
		return setResourcesMethod;
	}
}
