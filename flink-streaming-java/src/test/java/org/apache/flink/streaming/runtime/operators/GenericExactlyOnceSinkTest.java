/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class})
@PowerMockIgnore(value = {"javax.management.*", "org.apache.log4j.*"})
public class GenericExactlyOnceSinkTest extends ExactlyOnceSinkTestBase<Tuple1<Integer>, GenericExactlyOnceSinkTest.ListSink> {
	@Override
	protected ListSink createSink() {
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
		OneInputStreamTaskTestHarness<Tuple1<Integer>, Tuple1<Integer>> harness,
		OneInputStreamTask<Tuple1<Integer>, Tuple1<Integer>> task, ListSink sink) {

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
		OneInputStreamTaskTestHarness<Tuple1<Integer>, Tuple1<Integer>> harness,
		OneInputStreamTask<Tuple1<Integer>, Tuple1<Integer>> task, ListSink sink) {

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
		OneInputStreamTaskTestHarness<Tuple1<Integer>, Tuple1<Integer>> harness,
		OneInputStreamTask<Tuple1<Integer>, Tuple1<Integer>> task, ListSink sink) {

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

	/**
	 * Simple sink that stores all records in a public list.
	 */
	public static class ListSink extends GenericExactlyOnceSink<Tuple1<Integer>> {
		public List<Integer> values = new ArrayList<>();

		public ListSink() {
			super(new SimpleCommitter());
		}

		@Override
		protected void sendValue(Iterable<Tuple1<Integer>> values) throws Exception {
			for (Tuple1<Integer> value : values) {
				this.values.add(value.f0);
			}
		}
	}

	public static class SimpleCommitter extends CheckpointCommitter {
		private List<Long> checkpoints;

		@Override
		public void open() throws Exception {
			checkpoints = new ArrayList<>();
		}

		@Override
		public void close() throws Exception {
			checkpoints.clear();
			checkpoints = null;
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
}
