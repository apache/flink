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
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultPartitionWriter.class)
@PowerMockIgnore({"javax.management.*", "com.sun.jndi.*"})
public class GenericWriteAheadSinkTest extends WriteAheadSinkTestBase<Tuple1<Integer>, GenericWriteAheadSinkTest.ListSink> {
	@Override
	protected ListSink createSink() throws Exception {
		return new ListSink();
	}

	@Override
	protected TupleTypeInfo<Tuple1<Integer>> createTypeInfo() {
		return TupleTypeInfo.getBasicTupleTypeInfo(Integer.class);
	}

	@Override
	protected Tuple1<Integer> generateValue(int counter, int checkpointID) {
		return new Tuple1<>(counter);
	}


	@Override
	protected void verifyResultsIdealCircumstances(
		OneInputStreamOperatorTestHarness<Tuple1<Integer>, Tuple1<Integer>> harness,
		ListSink sink) {

		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (Integer i : sink.values) {
			list.remove(i);
		}
		Assert.assertTrue("The following ID's where not found in the result list: " + list.toString(), list.isEmpty());
		Assert.assertTrue("The sink emitted to many values: " + (sink.values.size() - 60), sink.values.size() == 60);
	}

	@Override
	protected void verifyResultsDataPersistenceUponMissedNotify(
		OneInputStreamOperatorTestHarness<Tuple1<Integer>, Tuple1<Integer>> harness,
		ListSink sink) {

		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 60; x++) {
			list.add(x);
		}

		for (Integer i : sink.values) {
			list.remove(i);
		}
		Assert.assertTrue("The following ID's where not found in the result list: " + list.toString(), list.isEmpty());
		Assert.assertTrue("The sink emitted to many values: " + (sink.values.size() - 60), sink.values.size() == 60);
	}

	@Override
	protected void verifyResultsDataDiscardingUponRestore(
		OneInputStreamOperatorTestHarness<Tuple1<Integer>, Tuple1<Integer>> harness,
		ListSink sink) {

		ArrayList<Integer> list = new ArrayList<>();
		for (int x = 1; x <= 20; x++) {
			list.add(x);
		}
		for (int x = 41; x <= 60; x++) {
			list.add(x);
		}

		for (Integer i : sink.values) {
			list.remove(i);
		}
		Assert.assertTrue("The following ID's where not found in the result list: " + list.toString(), list.isEmpty());
		Assert.assertTrue("The sink emitted to many values: " + (sink.values.size() - 40), sink.values.size() == 40);
	}

	@Test
	/**
	 * Verifies that exceptions thrown by a committer do not fail a job and lead to an abort of notify()
	 * and later retry of the affected checkpoints.
	 */
	public void testCommitterException() throws Exception {

		ListSink2 sink = new ListSink2();

		OneInputStreamOperatorTestHarness<Tuple1<Integer>, Tuple1<Integer>> testHarness =
				new OneInputStreamOperatorTestHarness<>(sink);

		testHarness.open();

		int elementCounter = 1;

		for (int x = 0; x < 10; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 0)));
			elementCounter++;
		}

		testHarness.snapshot(0, 0);
		testHarness.notifyOfCompletedCheckpoint(0);

		//isCommitted should have failed, thus sendValues() should never have been called
		Assert.assertTrue(sink.values.size() == 0);

		for (int x = 0; x < 10; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 1)));
			elementCounter++;
		}

		testHarness.snapshot(1, 0);
		testHarness.notifyOfCompletedCheckpoint(1);

		//previous CP should be retried, but will fail the CP commit. Second CP should be skipped.
		Assert.assertTrue(sink.values.size() == 10);

		for (int x = 0; x < 10; x++) {
			testHarness.processElement(new StreamRecord<>(generateValue(elementCounter, 2)));
			elementCounter++;
		}

		testHarness.snapshot(2, 0);
		testHarness.notifyOfCompletedCheckpoint(2);

		//all CP's should be retried and succeed; since one CP was written twice we have 2 * 10 + 10 + 10 = 40 values
		Assert.assertTrue(sink.values.size() == 40);
	}

	/**
	 * Simple sink that stores all records in a public list.
	 */
	public static class ListSink extends GenericWriteAheadSink<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		public List<Integer> values = new ArrayList<>();

		public ListSink() throws Exception {
			super(new SimpleCommitter(), TypeExtractor.getForObject(new Tuple1<>(1)).createSerializer(new ExecutionConfig()), "job");
		}

		@Override
		protected boolean sendValues(Iterable<Tuple1<Integer>> values, long timestamp) throws Exception {
			for (Tuple1<Integer> value : values) {
				this.values.add(value.f0);
			}
			return true;
		}
	}

	public static class SimpleCommitter extends CheckpointCommitter {
		private static final long serialVersionUID = 1L;

		private List<Long> checkpoints;

		@Override
		public void open() throws Exception {
		}

		@Override
		public void close() throws Exception {
		}

		@Override
		public void createResource() throws Exception {
			checkpoints = new ArrayList<>();
		}

		@Override
		public void commitCheckpoint(long checkpointID) {
			checkpoints.add(checkpointID);
		}

		@Override
		public boolean isCheckpointCommitted(long checkpointID) {
			return checkpoints.contains(checkpointID);
		}
	}

	/**
	 * Simple sink that stores all records in a public list.
	 */
	public static class ListSink2 extends GenericWriteAheadSink<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		public List<Integer> values = new ArrayList<>();

		public ListSink2() throws Exception {
			super(new FailingCommitter(), TypeExtractor.getForObject(new Tuple1<>(1)).createSerializer(new ExecutionConfig()), "job");
		}

		@Override
		protected boolean sendValues(Iterable<Tuple1<Integer>> values, long timestamp) throws Exception {
			for (Tuple1<Integer> value : values) {
				this.values.add(value.f0);
			}
			return true;
		}
	}

	public static class FailingCommitter extends CheckpointCommitter {
		private static final long serialVersionUID = 1L;

		private List<Long> checkpoints;
		private boolean failIsCommitted = true;
		private boolean failCommit = true;

		@Override
		public void open() throws Exception {
		}

		@Override
		public void close() throws Exception {
		}

		@Override
		public void createResource() throws Exception {
			checkpoints = new ArrayList<>();
		}

		@Override
		public void commitCheckpoint(long checkpointID) {
			if (failCommit) {
				failCommit = false;
				throw new RuntimeException("Expected exception");
			} else {
				checkpoints.add(checkpointID);
			}
		}

		@Override
		public boolean isCheckpointCommitted(long checkpointID) {
			if (failIsCommitted) {
				failIsCommitted = false;
				throw new RuntimeException("Expected exception");
			} else {
				return false;
			}
		}
	}
}
