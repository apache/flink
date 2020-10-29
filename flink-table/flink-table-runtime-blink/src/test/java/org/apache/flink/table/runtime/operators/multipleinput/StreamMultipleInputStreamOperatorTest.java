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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.multipleinput.TableOperatorWrapperGenerator.InputInfo;
import org.apache.flink.table.runtime.operators.multipleinput.input.InputSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link StreamMultipleInputStreamOperator}.
 */
public class StreamMultipleInputStreamOperatorTest extends MultipleInputTestBase {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testOpen() throws Exception {
		TestingStreamMultipleInputStreamOperator op = createMultipleInputStreamOperator();
		TestingTwoInputStreamOperator joinOp =
				(TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

		TableOperatorWrapper<?> aggWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
		TestingOneInputStreamOperator aggOp1 = (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

		TableOperatorWrapper<?> aggWrapper2 = op.getTailWrapper().getInputWrappers().get(1);
		TestingOneInputStreamOperator aggOp2 = (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

		op.initializeState(new StreamTaskStateInitializerImpl(new MockEnvironmentBuilder().build(), new MemoryStateBackend()));

		assertFalse(aggOp1.isOpened());
		checkBeforeInitializeState(aggOp1);
		assertFalse(aggOp2.isOpened());
		checkBeforeInitializeState(aggOp2);
		assertFalse(joinOp.isOpened());
		checkBeforeInitializeState(joinOp);

		op.open();

		assertTrue(aggOp1.isOpened());
		assertNotNull(aggOp1.getOperatorStateBackend());

		assertTrue(aggOp2.isOpened());
		assertNotNull(aggOp2.getOperatorStateBackend());

		assertTrue(joinOp.isOpened());
		assertNotNull(joinOp.getOperatorStateBackend());
	}

	@Test
	public void testOperatorDoesNotImplementStateNameAware() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Transformation<RowData> source1 = createSource(env, "source1");
		Transformation<RowData> source2 = createSource(env, "source2");
		Transformation<RowData> source3 = createSource(env, "source3");

		TwoInputTransformation<RowData, RowData, RowData> join1 = new TwoInputTransformation<>(
				source1,
				source2,
				"join1",
				new TestingOperatorWithoutStateNameAware(),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())),
				10);
		TwoInputTransformation<RowData, RowData, RowData> join2 = new TwoInputTransformation<>(
				join1,
				source3,
				"join2",
				new TestingOperatorWithoutStateNameAware(),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())),
				10);

		TableOperatorWrapperGenerator generator = new TableOperatorWrapperGenerator(
				Arrays.asList(source1, source2, source3),
				join2);
		generator.generate();

		TestingStreamMultipleInputStreamOperator op = new TestingStreamMultipleInputStreamOperator(
				createStreamOperatorParameters(),
				generator.getInputInfoList().stream().map(InputInfo::getInputSpec).collect(Collectors.toList()),
				generator.getHeadWrappers(),
				generator.getTailWrapper());

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("TestingOperatorWithoutStateNameAware must extend from StateNameAware");
		op.open();
	}

	@Test
	public void testFunctionDoesNotImplementStateNameAware() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Transformation<RowData> source1 = createSource(env, "source1");
		Transformation<RowData> source2 = createSource(env, "source2");
		Transformation<RowData> source3 = createSource(env, "source3");

		TwoInputTransformation<RowData, RowData, RowData> join1 = new TwoInputTransformation<>(
				source1,
				source2,
				"join1",
				new TestingAbstractUdfStreamOperator(),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())),
				10);
		TwoInputTransformation<RowData, RowData, RowData> join2 = new TwoInputTransformation<>(
				join1,
				source3,
				"join2",
				new TestingAbstractUdfStreamOperator(),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())),
				10);

		TableOperatorWrapperGenerator generator = new TableOperatorWrapperGenerator(
				Arrays.asList(source1, source2, source3),
				join2);
		generator.generate();

		TestingStreamMultipleInputStreamOperator op = new TestingStreamMultipleInputStreamOperator(
				createStreamOperatorParameters(),
				generator.getInputInfoList().stream().map(InputInfo::getInputSpec).collect(Collectors.toList()),
				generator.getHeadWrappers(),
				generator.getTailWrapper());

		thrown.expect(RuntimeException.class);
		thrown.expectMessage("TestingFunctionWithoutStateNameAware must extend from StateNameAware");
		op.open();
	}

	@Test
	public void testPrepareSnapshotPreBarrier() throws Exception {
		TestingStreamMultipleInputStreamOperator op = createMultipleInputStreamOperator();
		TestingTwoInputStreamOperator joinOp =
				(TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

		TableOperatorWrapper<?> aggWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
		TestingOneInputStreamOperator aggOp1 = (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

		TableOperatorWrapper<?> aggWrapper2 = op.getTailWrapper().getInputWrappers().get(1);
		TestingOneInputStreamOperator aggOp2 = (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

		assertEquals(-1, joinOp.getCheckpointIdOfPrepareSnapshotPreBarrier());
		assertEquals(-1, aggOp1.getCheckpointIdOfPrepareSnapshotPreBarrier());
		assertEquals(-1, aggOp2.getCheckpointIdOfPrepareSnapshotPreBarrier());

		op.prepareSnapshotPreBarrier(100);

		assertEquals(100, joinOp.getCheckpointIdOfPrepareSnapshotPreBarrier());
		assertEquals(100, aggOp1.getCheckpointIdOfPrepareSnapshotPreBarrier());
		assertEquals(100, aggOp2.getCheckpointIdOfPrepareSnapshotPreBarrier());
	}

	@Test
	public void testSnapshotState() throws Exception {
		TestingStreamMultipleInputStreamOperator op = createMultipleInputStreamOperator();
		TestingTwoInputStreamOperator joinOp =
				(TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

		TableOperatorWrapper<?> aggWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
		TestingOneInputStreamOperator aggOp1 = (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

		TableOperatorWrapper<?> aggWrapper2 = op.getTailWrapper().getInputWrappers().get(1);
		TestingOneInputStreamOperator aggOp2 = (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

		assertFalse(joinOp.isSnapshotStateExecuted());
		assertFalse(aggOp1.isSnapshotStateExecuted());
		assertFalse(aggOp2.isSnapshotStateExecuted());

		op.snapshotState(new StateSnapshotContextSynchronousImpl(100, 200));

		assertTrue(joinOp.isSnapshotStateExecuted());
		assertTrue(aggOp1.isSnapshotStateExecuted());
		assertTrue(aggOp2.isSnapshotStateExecuted());
	}

	@Test
	public void testNotifyCheckpointComplete() throws Exception {
		TestingStreamMultipleInputStreamOperator op = createMultipleInputStreamOperator();
		TestingTwoInputStreamOperator joinOp =
				(TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

		TableOperatorWrapper<?> aggWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
		TestingOneInputStreamOperator aggOp1 = (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

		TableOperatorWrapper<?> aggWrapper2 = op.getTailWrapper().getInputWrappers().get(1);
		TestingOneInputStreamOperator aggOp2 = (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

		assertEquals(-1, joinOp.getCheckpointIdOfNotifyCheckpointComplete());
		assertEquals(-1, aggOp1.getCheckpointIdOfNotifyCheckpointComplete());
		assertEquals(-1, aggOp2.getCheckpointIdOfNotifyCheckpointComplete());

		op.notifyCheckpointComplete(100);

		assertEquals(100, joinOp.getCheckpointIdOfNotifyCheckpointComplete());
		assertEquals(100, aggOp1.getCheckpointIdOfNotifyCheckpointComplete());
		assertEquals(100, aggOp2.getCheckpointIdOfNotifyCheckpointComplete());
	}

	@Test
	public void testNotifyCheckpointAborted() throws Exception {
		TestingStreamMultipleInputStreamOperator op = createMultipleInputStreamOperator();
		TestingTwoInputStreamOperator joinOp =
				(TestingTwoInputStreamOperator) op.getTailWrapper().getStreamOperator();

		TableOperatorWrapper<?> aggWrapper1 = op.getTailWrapper().getInputWrappers().get(0);
		TestingOneInputStreamOperator aggOp1 = (TestingOneInputStreamOperator) aggWrapper1.getStreamOperator();

		TableOperatorWrapper<?> aggWrapper2 = op.getTailWrapper().getInputWrappers().get(1);
		TestingOneInputStreamOperator aggOp2 = (TestingOneInputStreamOperator) aggWrapper2.getStreamOperator();

		assertEquals(-1, joinOp.getCheckpointIdOfNotifyCheckpointAborted());
		assertEquals(-1, aggOp1.getCheckpointIdOfNotifyCheckpointAborted());
		assertEquals(-1, aggOp2.getCheckpointIdOfNotifyCheckpointAborted());

		op.notifyCheckpointAborted(100);

		assertEquals(100, joinOp.getCheckpointIdOfNotifyCheckpointAborted());
		assertEquals(100, aggOp1.getCheckpointIdOfNotifyCheckpointAborted());
		assertEquals(100, aggOp2.getCheckpointIdOfNotifyCheckpointAborted());
	}

	private void checkBeforeInitializeState(AbstractStreamOperator<?> operator) {
		try {
			operator.getOperatorStateBackend();
			fail("This should not happen.");
		} catch (NullPointerException e) {
			// do nothing
		}
	}

	/**
	 * Create a StreamMultipleInputStreamOperator which contains the following sub-graph.
	 * <pre>
	 *
	 * source1  source2
	 *   |        |
	 *  agg1     agg2
	 *     \     /
	 *      join
	 *
	 * </pre>
	 */
	private TestingStreamMultipleInputStreamOperator createMultipleInputStreamOperator() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Transformation<RowData> source1 = createSource(env, "source1");
		Transformation<RowData> source2 = createSource(env, "source2");
		OneInputTransformation<RowData, RowData> agg1 = createOneInputTransform(
				source1,
				"agg1",
				new TestingOneInputStreamOperator(true),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
		agg1.setStateKeySelector(new TestingKeySelector());
		agg1.setStateKeyType(InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));

		OneInputTransformation<RowData, RowData> agg2 = createOneInputTransform(
				source2,
				"agg2",
				new TestingOneInputStreamOperator(true),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
		agg2.setStateKeySelector(new TestingKeySelector());
		agg2.setStateKeyType(InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));

		TwoInputTransformation<RowData, RowData, RowData> join = createTwoInputTransform(
				agg1,
				agg2,
				"join",
				new TestingTwoInputStreamOperator(true),
				InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));
		join.setStateKeySelectors(new TestingKeySelector(), new TestingKeySelector());
		join.setStateKeyType(InternalTypeInfo.of(RowType.of(DataTypes.STRING().getLogicalType())));

		TableOperatorWrapperGenerator generator = new TableOperatorWrapperGenerator(
				Arrays.asList(source1, source2),
				join);
		generator.generate();

		List<InputInfo> inputInfoList = generator.getInputInfoList();

		return new TestingStreamMultipleInputStreamOperator(
				createStreamOperatorParameters(),
				inputInfoList.stream().map(InputInfo::getInputSpec).collect(Collectors.toList()),
				generator.getHeadWrappers(),
				generator.getTailWrapper());
	}

	/**
	 * A sub class of {@link StreamMultipleInputStreamOperator} for testing.
	 */
	private static class TestingStreamMultipleInputStreamOperator extends StreamMultipleInputStreamOperator {
		private static final long serialVersionUID = 1L;
		private final TableOperatorWrapper<?> tailWrapper;

		public TestingStreamMultipleInputStreamOperator(
				StreamOperatorParameters<RowData> parameters,
				List<InputSpec> inputSpecs,
				List<TableOperatorWrapper<?>> headWrapper,
				TableOperatorWrapper<?> tailWrapper) {
			super(parameters, inputSpecs, headWrapper, tailWrapper);
			this.tailWrapper = tailWrapper;
		}

		public TableOperatorWrapper<?> getTailWrapper() {
			return tailWrapper;
		}
	}

	/**
	 * A sub class of {@link AbstractStreamOperator} for testing which does not implement StateNameAware.
	 */
	private static class TestingOperatorWithoutStateNameAware
			extends AbstractStreamOperator<RowData>
			implements TwoInputStreamOperator<RowData, RowData, RowData> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement1(StreamRecord<RowData> element) throws Exception {

		}

		@Override
		public void processElement2(StreamRecord<RowData> element) throws Exception {

		}
	}

	/**
	 * A sub class of {@link AbstractUdfStreamOperator} for testing.
	 */
	private static class TestingAbstractUdfStreamOperator
			extends AbstractUdfStreamOperator<RowData, TestingFunctionWithoutStateNameAware>
			implements TwoInputStreamOperator<RowData, RowData, RowData> {
		private static final long serialVersionUID = 1L;

		public TestingAbstractUdfStreamOperator() {
			super(new TestingFunctionWithoutStateNameAware());
		}

		@Override
		public void processElement1(StreamRecord<RowData> element) throws Exception {

		}

		@Override
		public void processElement2(StreamRecord<RowData> element) throws Exception {

		}
	}

	/**
	 * A sub class of {@link Function} for testing.
	 */
	private static class TestingFunctionWithoutStateNameAware implements Function {
		private static final long serialVersionUID = 1L;
	}
}
