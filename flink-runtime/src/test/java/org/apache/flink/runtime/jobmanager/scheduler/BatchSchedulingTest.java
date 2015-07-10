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

package org.apache.flink.runtime.jobmanager.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.io.network.api.reader.RecordReader;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.types.IntValue;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BatchSchedulingTest {

	// Test cluster config
	private final static int NUMBER_OF_TMS = 1;
	private final static int NUMBER_OF_SLOTS_PER_TM = 2;
	private final static int PARALLELISM = NUMBER_OF_TMS * NUMBER_OF_SLOTS_PER_TM;

	private final static TestingCluster flink = TestingUtils.startTestingCluster(
			NUMBER_OF_SLOTS_PER_TM,
			NUMBER_OF_TMS,
			TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());

	private final static List<Integer> invokeOrder = Lists.newCopyOnWriteArrayList();

	@After
	public void clearInvokeOrder() throws Exception {
		invokeOrder.clear();
	}

	@AfterClass
	public static void tearDownTestCluster() throws Exception {
		if (flink != null) {
			flink.stop();
		}
	}

	@Test
	public void testBatchSchedulingScheduleModeNotSet() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingScheduleModeNotSet");

		JobVertex v1 = new JobVertex("v1");
		v1.setInvokableClass(SourceTask.class);
		v1.setParallelism(PARALLELISM);

		JobVertex v2 = new JobVertex("v2");
		v2.setInvokableClass(UnionForwarder.class);
		v2.setParallelism(PARALLELISM);

		v2.connectNewDataSetAsInput(v1, POINTWISE);

		jobGraph.addVertex(v1);
		jobGraph.addVertex(v2);

		v1.setAsBatchSource();
		v1.addBatchSuccessors(v2);

		try {
			// The execution should fail, because we configured a successor without
			// setting the correct schedule mode.
			flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());
			fail("Did not throw expected Exception.");
		}
		catch (JobExecutionException expected) {
			assertEquals(IllegalStateException.class, expected.getCause().getClass());
		}
	}

	@Test(expected = JobExecutionException.class)
	public void testBatchSchedulingNoSourceSet() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingNoSourceSet");
		jobGraph.setScheduleMode(ScheduleMode.BATCH_FROM_SOURCES);

		JobVertex src = new JobVertex("src");

		jobGraph.addVertex(src);

		// This should throw an Exception, because schedule mode is BATCH,
		// but no source has been configured.
		flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());
	}

	@Test(expected = JobExecutionException.class)
	public void testBatchSchedulingSourceWithInput() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingSourceWithInput");
		jobGraph.setScheduleMode(ScheduleMode.BATCH_FROM_SOURCES);

		JobVertex v1 = new JobVertex("v1");

		JobVertex v2 = new JobVertex("v2");
		v2.connectNewDataSetAsInput(v1, POINTWISE);

		jobGraph.addVertex(v1);
		jobGraph.addVertex(v2);

		v2.setAsBatchSource();

		// This should throw an Exception, because the configured source has an input vertex.
		flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());
	}

	/**
	 * <pre>
	 *        O verify
	 *        |
	 *        . <------------- denotes a pipelined result
	 *        O union
	 *  +----´|`----+
	 *  |     |     |
	 *  ■     ■     ■ <------- denotes a blocking result
	 *  O     O     O
	 * src0  src1  src2
	 * </pre>
	 */
	@Test
	public void testBatchSchedulingLegWise() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingLegWise");
		jobGraph.setScheduleMode(ScheduleMode.BATCH_FROM_SOURCES);

		// Union
		JobVertex union = new JobVertex("union");
		union.setInvokableClass(UnionForwarder.class);
		union.setParallelism(PARALLELISM);
		jobGraph.addVertex(union);

		// Create source vertices
		JobVertex[] src = new JobVertex[3];
		for (int i = 0; i < src.length; i++) {
			src[i] = new JobVertex("src " + i);
			src[i].setInvokableClass(SourceTask.class);
			src[i].setParallelism(PARALLELISM);
			src[i].getConfiguration().setInteger("index", i);
			jobGraph.addVertex(src[i]);

			// Connect sources to union node
			union.connectNewDataSetAsInput(src[i], POINTWISE, BLOCKING);
		}

		// Create verify
		JobVertex verify = new JobVertex("verify");
		verify.setInvokableClass(VerifyIndexes.class);
		verify.setParallelism(PARALLELISM);
		verify.getConfiguration().setInteger("maxIndex", src.length);

		verify.connectNewDataSetAsInput(union, POINTWISE, PIPELINED);
		jobGraph.addVertex(verify);

		// Slot sharing group for the last pipeline
		SlotSharingGroup ssg = new SlotSharingGroup(union.getID(), verify.getID());
		union.setSlotSharingGroup(ssg);
		verify.setSlotSharingGroup(ssg);

		// - Configure batch scheduling ------------------------------------------------------------

		// src0 is the first to go...
		src[0].setAsBatchSource();

		src[0].addBatchSuccessors(src[1]); // src0 => src1

		src[1].addBatchSuccessors(src[2]); // src1 => src2

		src[2].addBatchSuccessors(union); // src2 => [union => verify]

		// - Expected invoke order -----------------------------------------------------------------

		setExpectedInvokeOrder(src[0], 1);
		setExpectedInvokeOrder(src[1], 2);
		setExpectedInvokeOrder(src[2], 3);
		setExpectedInvokeOrder(union, 4);
		setExpectedInvokeOrder(verify, 5);

		flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());

		// The first vertices have tight expected ordering (batch scheduling)
		verifyExpectedInvokeOrder(invokeOrder.subList(0, PARALLELISM * 4));

		// The last 2 vertices have loose expected ordering, because they are pipelined
		verifyExpectedInvokeOrder(invokeOrder.subList(PARALLELISM * 4, invokeOrder.size()),
				Sets.newHashSet(5));
	}

	/**
	 * <pre>
	 *           O verify
	 *           |
	 *           .
	 *           O union1
	 *  +-------´ `------+
	 *  |                |
	 *  ■                .
	 *  O                O union0
	 * src0        +----´|`----+
	 *             |     |     |
	 *             ■     ■     ■
	 *             O     O     O
	 *            src1  src2  src3
	 * </pre>
	 */
	@Test
	public void testBatchSchedulingLeftFirst() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingLeftFirst");
		jobGraph.setScheduleMode(ScheduleMode.BATCH_FROM_SOURCES);

		// Create source vertices
		JobVertex[] src = new JobVertex[4];
		for (int i = 0; i < src.length; i++) {
			src[i] = new JobVertex("src " + i);
			src[i].setInvokableClass(SourceTask.class);
			src[i].setParallelism(PARALLELISM);
			src[i].getConfiguration().setInteger("index", i);
			jobGraph.addVertex(src[i]);
		}

		// Union 0
		JobVertex union0 = new JobVertex("union0");
		union0.setParallelism(PARALLELISM);
		union0.setInvokableClass(UnionForwarder.class);
		jobGraph.addVertex(union0);

		// Connect sources to union0 node
		union0.connectNewDataSetAsInput(src[1], POINTWISE, BLOCKING);
		union0.connectNewDataSetAsInput(src[2], POINTWISE, BLOCKING);
		union0.connectNewDataSetAsInput(src[3], POINTWISE, BLOCKING);

		// Union
		JobVertex union1 = new JobVertex("union1");
		union1.setInvokableClass(UnionForwarder.class);
		union1.setParallelism(PARALLELISM);
		jobGraph.addVertex(union1);

		union1.connectNewDataSetAsInput(src[0], POINTWISE, BLOCKING);
		union1.connectNewDataSetAsInput(union0, POINTWISE, PIPELINED);

		// Create verify
		JobVertex verify = new JobVertex("verify");
		verify.setInvokableClass(VerifyIndexes.class);
		verify.setParallelism(PARALLELISM);
		verify.getConfiguration().setInteger("maxIndex", src.length);

		verify.connectNewDataSetAsInput(union1, POINTWISE, PIPELINED);
		jobGraph.addVertex(verify);

		// Slot sharing group for the last pipeline
		SlotSharingGroup ssg = new SlotSharingGroup(union0.getID(), union1.getID(), verify.getID());
		union0.setSlotSharingGroup(ssg);
		union1.setSlotSharingGroup(ssg);
		verify.setSlotSharingGroup(ssg);

		// - Configure batch scheduling ------------------------------------------------------------

		// src0 is the first to go...
		src[0].setAsBatchSource();

		src[0].addBatchSuccessors(src[1]); // src0 => src1

		src[1].addBatchSuccessors(src[2]); // src1 => src2

		src[2].addBatchSuccessors(src[3]); // src2 => src3

		src[3].addBatchSuccessors(union0); // src3 => [union0 => union1 => verify]

		// - Expected invoke order -----------------------------------------------------------------

		setExpectedInvokeOrder(src[0], 1);
		setExpectedInvokeOrder(src[1], 2);
		setExpectedInvokeOrder(src[2], 3);
		setExpectedInvokeOrder(src[3], 4);
		setExpectedInvokeOrder(union0, 5);
		setExpectedInvokeOrder(union1, 6);
		setExpectedInvokeOrder(verify, 7);

		flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());

		// The first vertices have tight expected ordering (batch scheduling)
		verifyExpectedInvokeOrder(invokeOrder.subList(0, PARALLELISM * 5));

		// The last 2 vertices have loose expected ordering, because they are pipelined
		verifyExpectedInvokeOrder(invokeOrder.subList(PARALLELISM * 5, invokeOrder.size()),
				Sets.newHashSet(6, 7));
	}

	/**
	 * <pre>
	 *           O verify
	 *           |
	 *           .
	 *           O union1
	 *  +-------´ `------+
	 *  |                |
	 *  .                ■
	 *  O                O union0
	 * src0        +----´|`----+
	 *             |     |     |
	 *             ■     ■     ■
	 *             O     O     O
	 *            src1  src2  src3
	 * </pre>
	 */
	@Test
	public void testBatchSchedulingRightFirst() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingRightFirst");
		jobGraph.setScheduleMode(ScheduleMode.BATCH_FROM_SOURCES);

		// Create source vertices
		JobVertex[] src = new JobVertex[4];
		for (int i = 0; i < src.length; i++) {
			src[i] = new JobVertex("src " + i);
			src[i].setInvokableClass(SourceTask.class);
			src[i].setParallelism(PARALLELISM);
			src[i].getConfiguration().setInteger("index", i);
			jobGraph.addVertex(src[i]);
		}

		// Union 0
		JobVertex union0 = new JobVertex("union0");
		union0.setInvokableClass(UnionForwarder.class);
		union0.setParallelism(PARALLELISM);
		jobGraph.addVertex(union0);

		// Connect sources to union0 node
		union0.connectNewDataSetAsInput(src[1], POINTWISE, BLOCKING);
		union0.connectNewDataSetAsInput(src[2], POINTWISE, BLOCKING);
		union0.connectNewDataSetAsInput(src[3], POINTWISE, BLOCKING);

		// Union
		JobVertex union1 = new JobVertex("union1");
		union1.setInvokableClass(UnionForwarder.class);
		union1.setParallelism(PARALLELISM);
		jobGraph.addVertex(union1);

		union1.connectNewDataSetAsInput(src[0], POINTWISE, PIPELINED);
		union1.connectNewDataSetAsInput(union0, POINTWISE, BLOCKING);

		// Create verify
		JobVertex verify = new JobVertex("verify");
		verify.setInvokableClass(VerifyIndexes.class);
		verify.setParallelism(PARALLELISM);
		verify.getConfiguration().setInteger("maxIndex", src.length);

		verify.connectNewDataSetAsInput(union1, POINTWISE, PIPELINED);
		jobGraph.addVertex(verify);

		// Slot sharing group for the last pipeline
		SlotSharingGroup ssg = new SlotSharingGroup(src[0].getID(), union1.getID(), verify.getID());
		src[0].setSlotSharingGroup(ssg);
		union1.setSlotSharingGroup(ssg);
		verify.setSlotSharingGroup(ssg);

		// - Configure batch scheduling ------------------------------------------------------------

		// src1 is the first to go...
		src[1].setAsBatchSource();

		src[1].addBatchSuccessors(src[2]); // src1 => src2

		src[2].addBatchSuccessors(src[3]); // src2 => src3

		src[3].addBatchSuccessors(union0); // src3 => union0

		union0.addBatchSuccessors(src[0]); // union0 => [src0 => union1 => verify]

		// - Expected invoke order -----------------------------------------------------------------

		setExpectedInvokeOrder(src[1], 1);
		setExpectedInvokeOrder(src[2], 2);
		setExpectedInvokeOrder(src[3], 3);
		setExpectedInvokeOrder(union0, 4);
		setExpectedInvokeOrder(src[0], 5);
		setExpectedInvokeOrder(union1, 6);
		setExpectedInvokeOrder(verify, 7);

		flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());

		// The first 5 vertices have tight expected ordering (batch scheduling)
		verifyExpectedInvokeOrder(invokeOrder.subList(0, PARALLELISM * 5));

		// The last 2 vertices have loose expected ordering, because they are pipelined
		verifyExpectedInvokeOrder(invokeOrder.subList(PARALLELISM * 5, invokeOrder.size()),
				Sets.newHashSet(6, 7));
	}

	/**
	 * <pre>
	 *     O block
	 *     |
	 *     ■
	 *     O union
	 *  +-´ `-+
	 *  |     |
	 *  ■     ■
	 *  O     O
	 * src0  src1
	 * </pre>
	 */
	@Test
	public void testBatchSchedulingUpdateRunningTaskFails() throws Exception {
		// Create the JobGraph
		JobGraph jobGraph = new JobGraph("testBatchSchedulingUpdateRunningTaskFails");
		jobGraph.setScheduleMode(ScheduleMode.BATCH_FROM_SOURCES);

		// Create source vertices
		JobVertex[] src = new JobVertex[2];
		for (int i = 0; i < src.length; i++) {
			src[i] = new JobVertex("src " + i);
			src[i].setInvokableClass(SourceTask.class);
			src[i].setParallelism(1);
			src[i].getConfiguration().setInteger("index", i);
			jobGraph.addVertex(src[i]);
		}

		// Union
		JobVertex union = new JobVertex("union");
		union.setInvokableClass(UnionForwarder.class);
		union.setParallelism(1);
		jobGraph.addVertex(union);

		// Needed for the UnionForwarder writer
		JobVertex nop = new JobVertex("nop");
		nop.setInvokableClass(Tasks.NoOpInvokable.class);
		nop.setParallelism(1);
		jobGraph.addVertex(nop);

		nop.connectNewDataSetAsInput(union, POINTWISE);

		// Connect sources to union node
		union.connectNewDataSetAsInput(src[0], POINTWISE, BLOCKING);
		union.connectNewDataSetAsInput(src[1], POINTWISE, BLOCKING);

		// Slot sharing group for the last pipeline
		SlotSharingGroup ssg = new SlotSharingGroup(src[0].getID(), union.getID(), nop.getID());
		src[0].setSlotSharingGroup(ssg);
		union.setSlotSharingGroup(ssg);
		nop.setSlotSharingGroup(ssg);

		// - Configure batch scheduling ------------------------------------------------------------

		// src0 is the first to go...
		src[0].setAsBatchSource();

		// This ordering will provoke the union vertex to be already deployed when either src0 or
		// src1 try to schedule it after being finished. This should then throw an Exception.
		src[0].addBatchSuccessors(union, src[1]); // src0 => [union] | [src1]
		src[1].addBatchSuccessors(union);

		try {
			flink.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());
			fail("Did not throw expected Exception.");
		}
		catch (JobExecutionException expected) {
			assertEquals(IllegalStateException.class, expected.getCause().getClass());
		}
	}

	// ---------------------------------------------------------------------------------------------

	public static class SourceTask extends AbstractInvokable {

		private int index;

		private RecordWriter<IntValue> writer;

		@Override
		public void registerInputOutput() {
			index = getTaskConfiguration().getInteger("index", -1);
			writer = new RecordWriter<IntValue>(getEnvironment().getWriter(0));
		}

		@Override
		public void invoke() throws Exception {
			addInvoke(getTaskConfiguration());

			writer.emit(new IntValue(index));
			writer.flush();
		}
	}

	public static class UnionForwarder extends AbstractInvokable {

		private RecordReader<IntValue> reader;

		private RecordWriter<IntValue> writer;

		@Override
		public void registerInputOutput() {
			UnionInputGate union = new UnionInputGate(getEnvironment().getAllInputGates());

			reader = new RecordReader<IntValue>(union, IntValue.class);
			writer = new RecordWriter<IntValue>(getEnvironment().getWriter(0));
		}

		@Override
		public void invoke() throws Exception {
			addInvoke(getTaskConfiguration());

			IntValue val;
			while ((val = reader.next()) != null) {
				writer.emit(val);
			}

			writer.flush();
		}
	}

	public static class VerifyIndexes extends AbstractInvokable {

		private RecordReader<IntValue> reader;

		private int maxIndex;
		private BitSet receivedIndexes;

		@Override
		public void registerInputOutput() {
			maxIndex = getTaskConfiguration().getInteger("maxIndex", -1);

			reader = new RecordReader<IntValue>(getEnvironment().getInputGate(0), IntValue.class);
			receivedIndexes = new BitSet(maxIndex);
		}

		@Override
		public void invoke() throws Exception {
			addInvoke(getTaskConfiguration());

			IntValue val;
			while ((val = reader.next()) != null) {
				int index = val.getValue();

				if (receivedIndexes.get(index)) {
					throw new IllegalStateException("Duplicate index");
				}

				receivedIndexes.set(val.getValue());
			}

			if (receivedIndexes.cardinality() != maxIndex) {
				throw new IllegalStateException("Missing index");
			}
		}
	}

	// ---------------------------------------------------------------------------------------------

	private static void setExpectedInvokeOrder(JobVertex vertex, int expected) {
		vertex.getConfiguration().setInteger("expectedOrder", expected);
	}

	private static void addInvoke(Configuration conf) {
		invokeOrder.add(conf.getInteger("expectedOrder", -1));
	}

	private static void verifyExpectedInvokeOrder(List<Integer> invokeOrder) {
		int expected = 0;

		int size = invokeOrder.size();
		// Expected orders start counting from '1'
		for (int i = 0; i < size; i++) {
			if (i % PARALLELISM == 0) {
				expected++;
			}

			if (expected != (invokeOrder.get(i))) {
				fail("Unexpected invoke order: " + invokeOrder);
			}
		}
	}

	private static void verifyExpectedInvokeOrder(List<Integer> invokeOrder, Set<Integer> expected) {
		int size = invokeOrder.size();
		for (int i = 0; i < size; i++) {
			if (!expected.contains(invokeOrder.get(i))) {
				fail("Unexpected invoke order: " + invokeOrder);
			}
		}
	}
}
