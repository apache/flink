/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.runtime.operators.sink.BatchCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.BatchGlobalCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StatelessSinkWriterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.StreamingGlobalCommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link org.apache.flink.streaming.api.transformations.SinkTransformation}.
 */
@RunWith(Parameterized.class)
public class SinkTransformationTranslatorTest extends TestLogger {

	@Parameterized.Parameters(name = "Execution Mode: {0}, Expected Committer Operator: {1}, Expected Global Committer Operator: {2}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{RuntimeExecutionMode.STREAMING, StreamingCommitterOperatorFactory.class, StreamingGlobalCommitterOperatorFactory.class},
				{RuntimeExecutionMode.BATCH, BatchCommitterOperatorFactory.class, BatchGlobalCommitterOperatorFactory.class}});
	}

	@Parameterized.Parameter()
	public RuntimeExecutionMode runtimeExecutionMode;

	@Parameterized.Parameter(1)
	public Class<?> committerClass;

	@Parameterized.Parameter(2)
	public Class<?> globalCommitterClass;

	static final String NAME = "FileSink";
	static final String SLOT_SHARE_GROUP = "FileGroup";
	static final String UID = "FileUid";
	static final int PARALLELISM = 2;

	@Test
	public void generateWriterTopology() {
		final StreamGraph streamGraph = buildGraph(
				TestSink.newBuilder().build(), runtimeExecutionMode);

		final StreamNode sourceNode = findNodeNameContains(streamGraph, "Source");
		final StreamNode writerNode = findNodeNameContains(streamGraph, "Writer");

		assertThat(streamGraph.getStreamNodes().size(), equalTo(2));

		validateTopology(
				sourceNode,
				IntSerializer.class,
				writerNode,
				String.format("Sink Writer: %s", NAME),
				UID,
				StatelessSinkWriterOperatorFactory.class,
				PARALLELISM,
				-1);
	}

	@Test
	public void generateWriterCommitterTopology() {

		final StreamGraph streamGraph = buildGraph(TestSink
				.newBuilder()
				.setDefaultCommitter()
				.build(), runtimeExecutionMode);

		final StreamNode writerNode = findNodeNameContains(streamGraph, "Writer");
		final StreamNode committerNode = findNodeNameContains(streamGraph, "Committer");

		assertThat(streamGraph.getStreamNodes().size(), equalTo(3));

		validateTopology(
				writerNode,
				SimpleVersionedSerializerTypeSerializerProxy.class,
				committerNode,
				String.format("Sink Committer: %s", NAME),
				String.format("Sink Committer: %s", UID),
				committerClass,
				runtimeExecutionMode == RuntimeExecutionMode.STREAMING ? PARALLELISM : 1,
				runtimeExecutionMode == RuntimeExecutionMode.STREAMING ? -1 : 1);
	}

	@Test
	public void generateWriterCommitterGlobalCommitterTopology() {

		final StreamGraph streamGraph = buildGraph(TestSink
				.newBuilder()
				.setDefaultCommitter()
				.setDefaultGlobalCommitter()
				.build(), runtimeExecutionMode);

		final StreamNode committerNode = findNodeNameContains(streamGraph, "Committer");
		final StreamNode globalCommitterNode = findNodeNameContains(
				streamGraph,
				"Global Committer");

		validateTopology(
				committerNode,
				SimpleVersionedSerializerTypeSerializerProxy.class,
				globalCommitterNode,
				String.format("Sink Global Committer: %s", NAME),
				String.format("Sink Global Committer: %s", UID),
				globalCommitterClass,
				1,
				1);
	}

	@Test
	public void generateWriterGlobalCommitterTopology() {
		final StreamGraph streamGraph = buildGraph(TestSink
				.newBuilder()
				.setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE)
				.setDefaultGlobalCommitter()
				.build(), runtimeExecutionMode);

		final StreamNode writerNode = findNodeNameContains(streamGraph, "Writer");
		final StreamNode globalCommitterNode = findNodeNameContains(
				streamGraph,
				"Global Committer");

		validateTopology(
				writerNode,
				SimpleVersionedSerializerTypeSerializerProxy.class,
				globalCommitterNode,
				String.format("Sink Global Committer: %s", NAME),
				String.format("Sink Global Committer: %s", UID),
				globalCommitterClass,
				1,
				1);
	}

	@Test(expected = IllegalStateException.class)
	public void throwExceptionWithoutSettingUid() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
		env.configure(config, getClass().getClassLoader());
		//disable auto generating uid
		env.getConfig().disableAutoGeneratedUIDs();
		env.fromElements(1, 2).sinkTo(TestSink.newBuilder().build());
		env.getStreamGraph();
	}

	@Test
	public void disableOperatorChain() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final DataStreamSource<Integer> src = env.fromElements(1, 2);
		final DataStreamSink<Integer> dataStreamSink = src.sinkTo(TestSink
				.newBuilder()
				.setDefaultCommitter()
				.setDefaultGlobalCommitter()
				.build());
		dataStreamSink.disableChaining();

		final StreamGraph streamGraph = env.getStreamGraph();
		final StreamNode writer = findNodeNameContains(streamGraph, "Writer");
		final StreamNode committer = findNodeNameContains(streamGraph, "Committer");
		final StreamNode globalCommitter = findNodeNameContains(streamGraph, "Global Committer");

		assertThat(writer.getOperatorFactory().getChainingStrategy(), is(ChainingStrategy.NEVER));
		assertThat(
				committer.getOperatorFactory().getChainingStrategy(),
				is(ChainingStrategy.ALWAYS));
		assertThat(
				globalCommitter.getOperatorFactory().getChainingStrategy(),
				is(ChainingStrategy.ALWAYS));

	}

	private void validateTopology(
			StreamNode src,
			Class<?> srcOutTypeInfo,
			StreamNode dest,
			String name,
			String uid,
			Class<?> expectedOperatorFactory,
			int expectedParallelism,
			int expectedMaxParallelism) {

		//verify src node
		final StreamEdge srcOutEdge = src.getOutEdges().get(0);
		assertThat(srcOutEdge.getTargetId(), equalTo(dest.getId()));
		assertThat(src.getTypeSerializerOut(), instanceOf(srcOutTypeInfo));

		//verify dest node input
		final StreamEdge destInputEdge = dest.getInEdges().get(0);
		assertThat(destInputEdge.getSourceId(), equalTo(src.getId()));
		assertThat(dest.getTypeSerializersIn()[0], instanceOf(srcOutTypeInfo));

		//verify dest node
		assertThat(dest.getOperatorName(), equalTo(name));
		assertThat(dest.getTransformationUID(), equalTo(uid));

		assertThat(
				dest.getOperatorFactory(),
				instanceOf(expectedOperatorFactory));
		assertThat(dest.getParallelism(), equalTo(expectedParallelism));
		assertThat(dest.getMaxParallelism(), equalTo(expectedMaxParallelism));
		assertThat(
				dest.getOperatorFactory().getChainingStrategy(),
				is(ChainingStrategy.ALWAYS));
		assertThat(dest.getSlotSharingGroup(), equalTo(SLOT_SHARE_GROUP));

		//verify dest node output
		assertThat(dest.getOutEdges().size(), equalTo(0));
	}

	private StreamGraph buildGraph(
			TestSink sink,
			RuntimeExecutionMode runtimeExecutionMode) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
		env.configure(config, getClass().getClassLoader());
		final DataStreamSource<Integer> src = env.fromElements(1, 2);
		final DataStreamSink<Integer> dataStreamSink = src.rebalance().sinkTo(sink);
		setSinkProperty(dataStreamSink);
		return env.getStreamGraph("test");
	}

	private void setSinkProperty(DataStreamSink<Integer> dataStreamSink) {
		dataStreamSink.name(NAME);
		dataStreamSink.uid(UID);
		dataStreamSink.setParallelism(SinkTransformationTranslatorTest.PARALLELISM);
		dataStreamSink.slotSharingGroup(SLOT_SHARE_GROUP);
	}

	private StreamNode findNodeNameContains(StreamGraph streamGraph, String nodeName) {
		return streamGraph
				.getStreamNodes()
				.stream()
				.filter(x -> x.getOperatorName().contains(nodeName))
				.findFirst()
				.orElseThrow(() -> new IllegalStateException(
						"Can not find the node contains " + nodeName));
	}
}
