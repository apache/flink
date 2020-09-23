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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for chaining the source operators / inputs.
 */
@SuppressWarnings("serial")
public class SourceChainingITCase extends TestLogger {

	private static final int PARALLELISM = 4;

	@ClassRule
	public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(PARALLELISM)
			.build());

	// ------------------------------------------------------------------------

	// We currently only have tests for chaining sources in N-ary Input Operators,
	// because those are the only ones where the runtime supports chaining at the moment.

	@Test
	public void testNAryInputOperatorChainCreation() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		final DataStream<Long> stream = createNAryInputProgram(env);
		final JobGraph jobGraph = sinkAndCompileJobGraph(stream);

		assertEquals(1, jobGraph.getNumberOfVertices());
	}

	@Test
	public void testNAryInputOperatorExecution() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);

		final DataStream<Long> stream = createNAryInputProgram(env);
		final List<Long> result = DataStreamUtils.collectBoundedStream(stream, "N-Ary Source Chaining Test Program");

		verifySequence(result, 1L, 30L);
	}

	@Test
	public void testNAryInputOperatorMixedInputChainCreation() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		final DataStream<Long> stream = createNAryInputProgramWithMixedInputs(env);
		final JobGraph jobGraph = sinkAndCompileJobGraph(stream);

		assertEquals(3, jobGraph.getNumberOfVertices());
	}

	@Test
	public void testNAryInputOperatorMixedInputExecution() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);

		final DataStream<Long> stream = createNAryInputProgramWithMixedInputs(env);
		final List<Long> result = DataStreamUtils.collectBoundedStream(stream, "N-Ary Source Chaining Test Program");

		verifySequence(result, 1L, 30L);
	}

	/**
	 * Creates a DataStream program as shown below.
	 * <pre>
	 *               +--------------+
	 *   (src 1) --> |              |
	 *               |     N-Ary    |
	 *   (src 2) --> |              |
	 *               |   Operator   |
	 *   (src 3) --> |              |
	 *               +--------------+
	 * </pre>
	 */
	private DataStream<Long> createNAryInputProgram(final StreamExecutionEnvironment env) {
		env.getConfig().enableObjectReuse();

		final DataStream<Long> source1 = env.fromSource(
				new NumberSequenceSource(1L, 10L),
				WatermarkStrategy.noWatermarks(),
				"source-1");

		final DataStream<Long> source2 = env.fromSource(
				new NumberSequenceSource(11L, 20L),
				WatermarkStrategy.noWatermarks(),
				"source-2");

		final DataStream<Long> source3 = env.fromSource(
				new NumberSequenceSource(21L, 30L),
				WatermarkStrategy.noWatermarks(),
				"source-3");

		return nAryInputStreamOperation(source1, source2, source3);
	}

	/**
	 * Creates a DataStream program as shown below.
	 * <pre>
	 *                         +--------------+
	 *   (src 1) --> (map) --> |              |
	 *                         |     N-Ary    |
	 *             (src 2) --> |              |
	 *                         |   Operator   |
	 *   (src 3) --> (map) --> |              |
	 *                         +--------------+
	 * </pre>
	 */
	private DataStream<Long> createNAryInputProgramWithMixedInputs(final StreamExecutionEnvironment env) {
		env.getConfig().enableObjectReuse();

		final DataStream<Long> source1 = env.fromSource(
			new NumberSequenceSource(1L, 10L),
			WatermarkStrategy.noWatermarks(),
			"source-1");

		final DataStream<Long> source2 = env.fromSource(
			new NumberSequenceSource(11L, 20L),
			WatermarkStrategy.noWatermarks(),
			"source-2");

		final DataStream<Long> source3 = env.fromSource(
			new NumberSequenceSource(21L, 30L),
			WatermarkStrategy.noWatermarks(),
			"source-3");

		final DataStream<Long> stream1 = source1.map(v -> v);
		final DataStream<Long> stream3 = source3.map(v -> v);

		return nAryInputStreamOperation(stream1, source2, stream3);
	}

	private static DataStream<Long> nAryInputStreamOperation(
			final DataStream<Long> input1,
			final DataStream<Long> input2,
			final DataStream<Long> input3) {

		final StreamExecutionEnvironment env = input1.getExecutionEnvironment();

		// this is still clumsy due to the raw API

		final MultipleInputTransformation<Long> transform = new MultipleInputTransformation<>(
			"MultipleInputOperator", new NAryUnionOpFactory(), Types.LONG, env.getParallelism());
		transform.addInput(input1.getTransformation());
		transform.addInput(input2.getTransformation());
		transform.addInput(input3.getTransformation());
		transform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
		env.addOperator(transform);

		return new MultipleConnectedStreams(env).transform(transform);
	}

	private static JobGraph sinkAndCompileJobGraph(DataStream<?> stream) {
		stream.addSink(new DiscardingSink<>());

		final StreamGraph streamGraph = stream.getExecutionEnvironment().getStreamGraph();
		return  StreamingJobGraphGenerator.createJobGraph(streamGraph);
	}

	private static void verifySequence(final List<Long> sequence, final long from, final long to) {
		final ArrayList<Long> list = new ArrayList<>(sequence); // copy to be safe against immutable lists, etc.
		list.sort(Long::compareTo);

		int pos = 0;
		for (long value = from; value <= to; value++, pos++) {
			if (value != list.get(pos)) {
				fail(String.format("Expected: Sequence [%d, %d]. Found: %s", from, to, list));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Tests mocks for N-ary input operator
	// ------------------------------------------------------------------------

	private static final class NAryUnionOp<T> extends AbstractStreamOperatorV2<T> implements MultipleInputStreamOperator<T> {

		public NAryUnionOp(StreamOperatorParameters<T> parameters) {
			super(parameters, 3);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public List<Input> getInputs() {
			return Arrays.asList(
				new PassThoughInput<>(this, 1),
				new PassThoughInput<>(this, 2),
				new PassThoughInput<>(this, 3));
		}
	}

	/**
	 * Factory for {@link MultipleInputITCase.SumAllInputOperator}.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static class NAryUnionOpFactory extends AbstractStreamOperatorFactory<Long> {
		@Override
		public <T extends StreamOperator<Long>> T createStreamOperator(StreamOperatorParameters<Long> parameters) {
			final StreamOperator<Long> operator = new NAryUnionOp<>(parameters);
			return (T) operator;
		}

		@Override
		public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
			return NAryUnionOp.class;
		}
	}

	private static final class PassThoughInput<T> extends AbstractInput<T, T> {

		PassThoughInput(AbstractStreamOperatorV2<T> owner, int inputId) {
			super(owner, inputId);
		}

		@Override
		public void processElement(StreamRecord<T> element) throws Exception {
			output.collect(element);
		}
	}
}
