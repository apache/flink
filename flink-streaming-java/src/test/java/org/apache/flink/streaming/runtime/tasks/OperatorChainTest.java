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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.InputSelector.InputSelection;
import org.apache.flink.streaming.runtime.tasks.InputSelector.SelectionChangedListener;
import org.apache.flink.types.Record;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.runtime.tasks.OperatorChain.AbstractStreamOperatorProxy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for operator chaining behaviour.
 */
public class OperatorChainTest {

	/**
	 * Test multiple head chaining.
	 *<p>
	 *     ------- chain ---------------
	 *     |                           |
	 * 0 --+---> 3 ------> 4 ------> 5 |
	 *     |     ^         ^           |
	 *     |     |         |           |
	 *     |     2         |           |
	 *     ----------------+------------
	 *                     |
	 *                     1
	 *</p>
	 */
	@Test
	public void testMultipleHeadChaining() throws Exception {
		final StreamTaskConfigCache streamConfig = new StreamTaskConfigCache(Thread.currentThread().getContextClassLoader());

		final StreamSource<String, DummySourceFunction> dummySource = new StreamSource<>(new DummySourceFunction());
		final OneInputStreamOperator<String, String> union = new DummyUnionStreamOperator();
		final TwoInputStreamOperator<String, String, String> twoInput = new DummyTwoInputStreamOperator();
		final StreamSink<String> dummySink = new DummySinkOperator(new DummySinkFunction());

		StreamNode sourceOutOfChainVertexDummy = new StreamNode(0, null, dummySource, "source out of chain dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode anotherSourceOutOfChainVertexDummy = new StreamNode(1, null, dummySource, "another source out of chain dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode sourceInChainVertexDummy = new StreamNode(2, null, dummySource, "source in chain dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode unionVertexDummy = new StreamNode(3, null, union, "union dummy", new LinkedList<>(), OneInputStreamTask.class);
		StreamNode twoInputVertexDummy = new StreamNode(4, null, twoInput, "two input dummy", new LinkedList<>(), TwoInputStreamTask.class);
		StreamNode sinkDummy = new StreamNode(5, null, dummySink, "sink dummy", new LinkedList<>(), OneInputStreamTask.class);

		final List<StreamEdge> inEdges = new LinkedList<>();
		inEdges.add(new StreamEdge(sourceOutOfChainVertexDummy, unionVertexDummy, 1, new LinkedList<>(), new BroadcastPartitioner<>(), null /* output tag */));
		inEdges.add(new StreamEdge(anotherSourceOutOfChainVertexDummy, twoInputVertexDummy, 1, new LinkedList<>(), new BroadcastPartitioner<>(), null /* output tag */));
		streamConfig.setInStreamEdgesOfChain(inEdges);

		final List<StreamEdge> outEdgesInOrder = new LinkedList<>();
//		outEdgesInOrder.add(new StreamEdge(twoInputVertexDummy, sinkDummy, 1, new LinkedList<>(), new BroadcastPartitioner<>(), null /* output tag */));
		streamConfig.setOutStreamEdgesOfChain(outEdgesInOrder);

		final Map<Integer, StreamConfig> chainedTaskConfigs = new HashMap<>();

		final StreamConfig operatorConfig2 = new StreamConfig(new Configuration());
		operatorConfig2.setOperatorID(new OperatorID());
		operatorConfig2.setStreamOperator(dummySource);
		operatorConfig2.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		operatorConfig2.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					sourceInChainVertexDummy,
					unionVertexDummy,
					1,
					new LinkedList<>(),
					new BroadcastPartitioner<>(),
					null /* output tag */)));
		operatorConfig2.setTypeSerializerOut(mock(TypeSerializer.class));
		operatorConfig2.setBufferTimeout(0);
		chainedTaskConfigs.put(2, operatorConfig2);

		final StreamConfig operatorConfig3 = new StreamConfig(new Configuration());
		operatorConfig3.setOperatorID(new OperatorID());
		operatorConfig3.setStreamOperator(union);
		operatorConfig3.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					unionVertexDummy,
					twoInputVertexDummy,
					2,
					new LinkedList<>(),
					new BroadcastPartitioner<>(),
					null /* output tag */)));
		operatorConfig3.setTypeSerializerOut(mock(TypeSerializer.class));
		operatorConfig3.setBufferTimeout(0);
		chainedTaskConfigs.put(3, operatorConfig3);

		final StreamConfig operatorConfig4 = new StreamConfig(new Configuration());
		operatorConfig4.setOperatorID(new OperatorID());
		operatorConfig4.setStreamOperator(twoInput);
		operatorConfig4.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					twoInputVertexDummy,
					sinkDummy,
					1,
					new LinkedList<>(),
					new BroadcastPartitioner<>(),
					null /* output tag */)));

		operatorConfig4.setTypeSerializerOut(mock(TypeSerializer.class));
		operatorConfig4.setBufferTimeout(0);
		chainedTaskConfigs.put(4, operatorConfig4);

		final StreamConfig operatorConfig5 = new StreamConfig(new Configuration());
		operatorConfig5.setOperatorID(new OperatorID());
		operatorConfig5.setStreamOperator(dummySink);
		operatorConfig5.setBufferTimeout(0);
		chainedTaskConfigs.put(5, operatorConfig5);

		streamConfig.setChainedNodeConfigs(chainedTaskConfigs);

		final List<Integer> heads = new ArrayList<>();
		heads.add(2);
		heads.add(3);
		heads.add(4);

		streamConfig.setChainedHeadNodeIds(heads);

		streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		final List<Record> output = new ArrayList<>();
		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setLatencyTrackingInterval(0);
		executionConfig.enableObjectReuse();

		final Configuration taskConfig = new Configuration();
		streamConfig.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);

		streamConfig.serializeTo(new StreamTaskConfig(taskConfig));
		final MockEnvironment env = new MockEnvironment(
			JobID.generate(),
			new JobVertexID(),
			"Test Task",
			32L * 1024L,
			new MockInputSplitProvider(),
			1,
			taskConfig,
			executionConfig,
			new TestTaskStateManager(),
			1024,
			1024,
			0,
			Thread.currentThread().getContextClassLoader());
		env.addOutput(output);

		final StreamTask streamTask = new NoOpStreamTask(env);
		final OperatorChain operatorChain = new OperatorChain(streamTask, StreamTask.createStreamRecordWriters(streamConfig, env));
		streamTask.setProcessingTimeService(new TestProcessingTimeService());

		assertEquals(3, operatorChain.getHeadOperators().length);

		assertNull(operatorChain.getHeadOperator(1));
		assertNull(operatorChain.getHeadOperator(5));

		// Output
		((StreamSource) operatorChain.getHeadOperator(2)).run(this, operatorChain);

		final DummyUnionStreamOperator unionOperator = (DummyUnionStreamOperator) operatorChain.getHeadOperator(3);
		assertEquals(2, unionOperator.unionRecords.size());
		assertEquals("record1", unionOperator.unionRecords.get(0));
		assertEquals("record2", unionOperator.unionRecords.get(1));

		final DummyTwoInputStreamOperator twoInputOperator = (DummyTwoInputStreamOperator) operatorChain.getHeadOperator(4);
		assertEquals(2, twoInputOperator.secondRecords.size());
		assertEquals(0, twoInputOperator.firstRecords.size());
		assertEquals("record1", twoInputOperator.secondRecords.get(0));
		assertEquals("record2", twoInputOperator.secondRecords.get(1));

		// end input

		operatorChain.getOperatorProxy(3).endInput(inEdges.get(0));
		assertFalse(unionOperator.endInputInvoked);
		assertFalse(twoInputOperator.endInput1Invoked);
		assertFalse(twoInputOperator.endInput2Invoked);

		operatorChain.getOperatorProxy(2).endInput(null);
		assertTrue(unionOperator.endInputInvoked);
		assertFalse(twoInputOperator.endInput1Invoked);
		assertTrue(twoInputOperator.endInput2Invoked);

		operatorChain.getOperatorProxy(4).endInput(inEdges.get(1));
		assertTrue(unionOperator.endInputInvoked);
		assertTrue(twoInputOperator.endInput1Invoked);
		assertTrue(twoInputOperator.endInput2Invoked);

	}

	/**
	 * Topology in chain.
	 * 1 ---> 4 ---> 5
	 *        ^
	 *        |
	 * 2----> 3
	 * Expected result: 1->2->3->4->5
	 */
	@Test
	public void testGetTopologySortedOperators() {
		final Map<Integer, StreamOperator<?>> allOperators = new HashMap<>();
		final Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();

		final StreamTaskConfigCache streamConfig = new StreamTaskConfigCache(Thread.currentThread().getContextClassLoader());
		streamConfig.setChainedHeadNodeIds(new ArrayList<Integer>() {{
			add(1);
			add(2);
		}});
		streamConfig.setInStreamEdgesOfChain(Collections.singletonList(
			new StreamEdge(
				new StreamNode(0, null, new DummyOperator(), "operator-0", null, null),
				new StreamNode(1, null, new DummyOperator(), "operator-1", null, null),
				1,
				null,
				null,
				null
			)
		));

		final StreamOperator<String> operator1 = new DummyOperator();
		final StreamOperator<String> operator2 = new DummyOperator();
		final StreamOperator<String> operator3 = new DummyOperator();
		final StreamOperator<String> operator4 = new DummyOperator();
		final StreamOperator<String> operator5 = new DummyOperator();

		allOperators.put(1, operator1);
		allOperators.put(2, operator2);
		allOperators.put(3, operator3);
		allOperators.put(4, operator4);
		allOperators.put(5, operator5);

		final StreamConfig config1 = new StreamConfig(new Configuration());
		final StreamConfig config2 = new StreamConfig(new Configuration());
		final StreamConfig config3 = new StreamConfig(new Configuration());
		final StreamConfig config4 = new StreamConfig(new Configuration());
		final StreamConfig config5 = new StreamConfig(new Configuration());

		chainedConfigs.put(1, config1);
		chainedConfigs.put(2, config2);
		chainedConfigs.put(3, config3);
		chainedConfigs.put(4, config4);
		chainedConfigs.put(5, config5);

		config1.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					new StreamNode(1, null, operator1, "operator-1", null, null),
					new StreamNode(4, null, operator2, "operator-2", null, null),
					1,
					null,
					null,
					null)));

		config2.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					new StreamNode(2, null, operator2, "operator-2", null, null),
					new StreamNode(3, null, operator3, "operator-3", null, null),
					1,
					null,
					null,
					null)));

		config3.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					new StreamNode(3, null, operator3, "operator-3", null, null),
					new StreamNode(4, null, operator4, "operator-4", null, null),
					2,
					null,
					null,
					null)));

		config4.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					new StreamNode(4, null, operator4, "operator-4", null, null),
					new StreamNode(5, null, operator5, "operator-5", null, null),
					1,
					null,
					null,
					null)));

		final List<Integer> headIds = new ArrayList<>();
		headIds.add(1);
		headIds.add(2);

		final Deque<StreamOperator<?>> topologySortedOperators =
			OperatorChain.getTopologySortedOperators(headIds, Thread.currentThread().getContextClassLoader(), allOperators, chainedConfigs);

		assertEquals(5, topologySortedOperators.size());
		assertEquals(operator1, topologySortedOperators.poll());
		assertEquals(operator2, topologySortedOperators.poll());
		assertEquals(operator3, topologySortedOperators.poll());
		assertEquals(operator4, topologySortedOperators.poll());
		assertEquals(operator5, topologySortedOperators.poll());
	}

	/**
	 * Test dynamic priority in multiple head chain.
	 *<p>
	 *     ------- chain --------------
	 *     |                          |
	 * 0 --+---> 3 ------> 4 -----> 5 |
	 *     |     ^         ^          |
	 *     |     |         |          |
	 *     ------+---------+-----------
	 *           |         |
	 *           1         2
	 *</p>
	 */
	@Test
	public void testDynamicPriorityInMultipleHeadChain() throws Exception {
		final StreamTaskConfigCache streamConfig = new StreamTaskConfigCache(Thread.currentThread().getContextClassLoader());

		final StreamSource<String, DummySourceFunction> dummySource = new StreamSource<>(new DummySourceFunction());
		// Will change selection side after processing a record
		final TwoInputStreamOperator<String, String, String> twoInput = new DummyTwoInputStreamOperator();
		final StreamSink<String> dummySink = new StreamSink<>(new DummySinkFunction());

		StreamNode sourceOutOfChainVertexDummy0 = new StreamNode(0, null, dummySource, "source 0 out of chain dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode sourceOutOfChainVertexDummy1 = new StreamNode(1, null, dummySource, "source 1 out of chain dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode sourceOutOfChainVertexDummy2 = new StreamNode(2, null, dummySource, "source 2 out of chain dummy", new LinkedList<>(), SourceStreamTask.class);
		StreamNode twoInputVertexDummy3 = new StreamNode(3, null, twoInput, "two input 3 dummy", new LinkedList<>(), TwoInputStreamTask.class);
		StreamNode twoInputVertexDummy4 = new StreamNode(4, null, twoInput, "two input 4 dummy", new LinkedList<>(), TwoInputStreamTask.class);
		StreamNode sinkDummy = new StreamNode(5, null, dummySink, "sink dummy", new LinkedList<>(), OneInputStreamTask.class);

		final List<StreamEdge> inEdges = new LinkedList<>();
		inEdges.add(new StreamEdge(sourceOutOfChainVertexDummy0, twoInputVertexDummy3, 1, new LinkedList<>(), new BroadcastPartitioner<>(), null /* output tag */));
		inEdges.add(new StreamEdge(sourceOutOfChainVertexDummy1, twoInputVertexDummy3, 2, new LinkedList<>(), new BroadcastPartitioner<>(), null /* output tag */));
		inEdges.add(new StreamEdge(sourceOutOfChainVertexDummy2, twoInputVertexDummy4, 1, new LinkedList<>(), new BroadcastPartitioner<>(), null /* output tag */));
		streamConfig.setInStreamEdgesOfChain(inEdges);

		final List<StreamEdge> outEdgesInOrder = new LinkedList<>();
		streamConfig.setOutStreamEdgesOfChain(outEdgesInOrder);

		final Map<Integer, StreamConfig> chainedTaskConfigs = new HashMap<>();

		final StreamConfig operatorConfig3 = new StreamConfig(new Configuration());
		operatorConfig3.setOperatorID(new OperatorID());
		operatorConfig3.setStreamOperator(twoInput);
		operatorConfig3.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		operatorConfig3.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					twoInputVertexDummy3,
					twoInputVertexDummy4,
					2,
					new LinkedList<>(),
					new BroadcastPartitioner<>(),
					null /* output tag */)));
		operatorConfig3.setTypeSerializerOut(mock(TypeSerializer.class));
		operatorConfig3.setBufferTimeout(0);
		chainedTaskConfigs.put(3, operatorConfig3);

		final StreamConfig operatorConfig4 = new StreamConfig(new Configuration());
		operatorConfig4.setOperatorID(new OperatorID());
		operatorConfig4.setStreamOperator(twoInput);
		operatorConfig4.setChainedOutputs(
			Collections.singletonList(
				new StreamEdge(
					twoInputVertexDummy4,
					sinkDummy,
					1,
					new LinkedList<>(),
					new BroadcastPartitioner<>(),
					null /* output tag */)));
		operatorConfig4.setTypeSerializerOut(mock(TypeSerializer.class));
		operatorConfig4.setBufferTimeout(0);
		chainedTaskConfigs.put(4, operatorConfig4);

		final StreamConfig operatorConfig5 = new StreamConfig(new Configuration());
		operatorConfig5.setOperatorID(new OperatorID());
		operatorConfig5.setStreamOperator(dummySink);
		operatorConfig5.setBufferTimeout(0);
		chainedTaskConfigs.put(5, operatorConfig5);

		streamConfig.setChainedNodeConfigs(chainedTaskConfigs);

		final List<Integer> heads = new ArrayList<>();
		heads.add(3);
		heads.add(4);

		streamConfig.setChainedHeadNodeIds(heads);

		streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		final List<Record> output = new ArrayList<>();
		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setLatencyTrackingInterval(0);
		executionConfig.enableObjectReuse();

		final Configuration taskConfig = new Configuration();
		streamConfig.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);

		streamConfig.serializeTo(new StreamTaskConfig(taskConfig));
		final MockEnvironment env = new MockEnvironment(
			JobID.generate(),
			new JobVertexID(),
			"Test Task",
			32L * 1024L,
			new MockInputSplitProvider(),
			1,
			taskConfig,
			executionConfig,
			new TestTaskStateManager(),
			1024,
			1024,
			0,
			Thread.currentThread().getContextClassLoader());
		env.addOutput(output);

		final StreamTask streamTask = new NoOpStreamTask(env);
		final OperatorChain operatorChain = new OperatorChain(streamTask, StreamTask.createStreamRecordWriters(streamConfig, env));
		final SelectionChangedMonitor monitor = new SelectionChangedMonitor();
		operatorChain.registerSelectionChangedListener(monitor);
		streamTask.setProcessingTimeService(new TestProcessingTimeService());

		for (StreamOperator operator : operatorChain.getAllOperatorsTopologySorted()) {
			operator.open();
		}

		assertEquals(2, operatorChain.getHeadOperators().length);

		assertNull(operatorChain.getHeadOperator(1));
		assertNull(operatorChain.getHeadOperator(2));
		assertNull(operatorChain.getHeadOperator(5));

		final TwoInputStreamOperator twoInputOperator3 = (TwoInputStreamOperator) operatorChain.getOperatorProxy(3);
		final TwoInputStreamOperator twoInputOperator4 = (TwoInputStreamOperator) operatorChain.getOperatorProxy(4);

		final List<InputSelection> nextSelectedInputs0 = operatorChain.getNextSelectedInputs();
		// 0 -> 3 -> 4 -> 5
		assertEquals(1, nextSelectedInputs0.size());
		assertEquals(InputSelector.InputType.EDGE, nextSelectedInputs0.get(0).getInputType());
		assertEquals(2, nextSelectedInputs0.get(0).toEdgeInputSelection().getStreamEdge().getTypeNumber());
		assertEquals(1, nextSelectedInputs0.get(0).toEdgeInputSelection().getStreamEdge().getSourceId());
		assertEquals(3, nextSelectedInputs0.get(0).toEdgeInputSelection().getStreamEdge().getTargetId());

		assertFalse(monitor.selectionChanged);
		// record-$operator-$typeNum-$recordSequence
		twoInputOperator3.processElement2(new StreamRecord<>("record-3-2-1"));

		assertTrue(monitor.selectionChanged);
		monitor.selectionChanged = false;
		// 0 -> 3
		// 2 -> 4 -> 5
		final List<InputSelection> nextSelectedInputs1 = operatorChain.getNextSelectedInputs();
		assertEquals(1, nextSelectedInputs1.size());
		assertEquals(InputSelector.InputType.EDGE, nextSelectedInputs1.get(0).getInputType());
		assertEquals(1, nextSelectedInputs1.get(0).toEdgeInputSelection().getStreamEdge().getTypeNumber());
		assertEquals(2, nextSelectedInputs1.get(0).toEdgeInputSelection().getStreamEdge().getSourceId());
		assertEquals(4, nextSelectedInputs1.get(0).toEdgeInputSelection().getStreamEdge().getTargetId());

		twoInputOperator4.processElement1(new StreamRecord<>("record-4-1-1"));

		assertTrue(monitor.selectionChanged);
		monitor.selectionChanged = false;
		// 0 -> 3 -> 4 -> 5
		final List<InputSelection> nextSelectedInputs2 = operatorChain.getNextSelectedInputs();
		assertEquals(1, nextSelectedInputs2.size());
		assertEquals(InputSelector.InputType.EDGE, nextSelectedInputs2.get(0).getInputType());
		assertEquals(1, nextSelectedInputs2.get(0).toEdgeInputSelection().getStreamEdge().getTypeNumber());
		assertEquals(0, nextSelectedInputs2.get(0).toEdgeInputSelection().getStreamEdge().getSourceId());
		assertEquals(3, nextSelectedInputs2.get(0).toEdgeInputSelection().getStreamEdge().getTargetId());

		twoInputOperator3.processElement1(new StreamRecord<>("record-3-1-1"));

		assertTrue(monitor.selectionChanged);
		monitor.selectionChanged = false;
		// 1 -> 3
		// 2 -> 4 -> 5
		final List<InputSelection> nextSelectedInputs3 = operatorChain.getNextSelectedInputs();
		assertEquals(1, nextSelectedInputs3.size());
		assertEquals(InputSelector.InputType.EDGE, nextSelectedInputs3.get(0).getInputType());
		assertEquals(1, nextSelectedInputs3.get(0).toEdgeInputSelection().getStreamEdge().getTypeNumber());
		assertEquals(2, nextSelectedInputs3.get(0).toEdgeInputSelection().getStreamEdge().getSourceId());
		assertEquals(4, nextSelectedInputs3.get(0).toEdgeInputSelection().getStreamEdge().getTargetId());

		twoInputOperator4.processElement1(new StreamRecord<>("record-4-1-2"));

		assertTrue(monitor.selectionChanged);
		monitor.selectionChanged = false;
		// 1 -> 3 -> 4 -> 5
		final List<InputSelection> nextSelectedInputs4 = operatorChain.getNextSelectedInputs();
		assertEquals(1, nextSelectedInputs4.size());
		assertEquals(InputSelector.InputType.EDGE, nextSelectedInputs4.get(0).getInputType());
		assertEquals(2, nextSelectedInputs4.get(0).toEdgeInputSelection().getStreamEdge().getTypeNumber());
		assertEquals(1, nextSelectedInputs4.get(0).toEdgeInputSelection().getStreamEdge().getSourceId());
		assertEquals(3, nextSelectedInputs4.get(0).toEdgeInputSelection().getStreamEdge().getTargetId());

		final DummyTwoInputStreamOperator realOperator3 = ((DummyTwoInputStreamOperator) ((AbstractStreamOperatorProxy) twoInputOperator3).getOperator());
		assertEquals(1, realOperator3.firstRecords.size());
		assertEquals("record-3-1-1", realOperator3.firstRecords.get(0));

		assertEquals(1, realOperator3.secondRecords.size());
		assertEquals("record-3-2-1", realOperator3.secondRecords.get(0));

		final DummyTwoInputStreamOperator realOperator4 = ((DummyTwoInputStreamOperator) ((AbstractStreamOperatorProxy) twoInputOperator4).getOperator());
		assertEquals(2, realOperator4.firstRecords.size());
		assertEquals("record-4-1-1", realOperator4.firstRecords.get(0));
		assertEquals("record-4-1-2", realOperator4.firstRecords.get(1));

		assertEquals(2, realOperator4.secondRecords.size());
		assertEquals("record-3-2-1", realOperator4.secondRecords.get(0));
		assertEquals("record-3-1-1", realOperator4.secondRecords.get(1));
	}

	private static class DummySourceFunction implements SourceFunction<String> {

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			ctx.collect("record1");
			ctx.collect("record2");
		}

		@Override
		public void cancel() {

		}
	}

	private static class DummyUnionStreamOperator extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {
		boolean endInputInvoked = false;

		List<String> unionRecords = new ArrayList<>();

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			unionRecords.add(element.getValue());
			output.collect(element);
		}

		@Override
		public void endInput() {
			if (!endInputInvoked) {
				endInputInvoked = true;
			} else {
				fail("End input should only be invoked once");
			}
		}
	}

	private static class DummyTwoInputStreamOperator extends AbstractStreamOperator<String> implements TwoInputStreamOperator<String, String, String> {
		List<String> firstRecords = new ArrayList<>();
		List<String> secondRecords = new ArrayList<>();

		boolean endInput1Invoked = false;
		boolean endInput2Invoked = false;

		private int processed = 0;

		@Override
		public TwoInputSelection firstInputSelection() {
			return (processed % 2 == 0) ? TwoInputSelection.SECOND : TwoInputSelection.FIRST;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<String> element) throws Exception {
			firstRecords.add(element.getValue());
			output.collect(element);
			processed++;
			return (processed % 2 == 0) ? TwoInputSelection.SECOND : TwoInputSelection.FIRST;
		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<String> element) throws Exception {
			secondRecords.add(element.getValue());
			output.collect(element);
			processed++;
			return (processed % 2 == 0) ? TwoInputSelection.SECOND : TwoInputSelection.FIRST;
		}

		@Override
		public void endInput1() {
			if (!endInput1Invoked) {
				endInput1Invoked = true;
			} else {
				fail("End input should only be invoked once");
			}
		}

		@Override
		public void endInput2() {
			if (!endInput2Invoked) {
				endInput2Invoked = true;
			} else {
				fail("End input should only be invoked once");
			}
		}
	}

	private static class DummySinkOperator extends StreamSink<String> {
		boolean endInputInvoked = false;
		List<String> records = new ArrayList<>();

		public DummySinkOperator(SinkFunction<String> sinkFunction) {
			super(sinkFunction);
		}

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			records.add(element.getValue());
		}

		@Override
		public void endInput() {
			if (!endInputInvoked) {
				endInputInvoked = true;
			} else {
				fail("End input should only be invoked once");
			}
		}
	}

	private static class DummySinkFunction implements SinkFunction<String> {
		@Override
		public void invoke(String value, Context context) {

		}

	}

	private static class NoOpStreamTask<T, OP extends StreamOperator<T>> extends StreamTask<T, OP> {

		NoOpStreamTask(Environment environment) {
			super(environment);
		}

		@Override
		public StreamConfig getConfiguration() {
			return new StreamConfig(getEnvironment().getTaskConfiguration());
		}

		@Override
		protected void init() throws Exception {}

		@Override
		protected void run() throws Exception {}

		@Override
		protected void cleanup() throws Exception {}

		@Override
		protected void cancelTask() throws Exception {}
	}

	private static class DummyOperator extends AbstractStreamOperator<String> {
		private static final long serialVersionUID = 1L;
	}

	private static class SelectionChangedMonitor implements SelectionChangedListener {

		public boolean selectionChanged = false;

		@Override
		public void notifySelectionChanged() {
			selectionChanged = true;
		}
	}
}

