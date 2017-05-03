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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.BitSet;

public class StreamOperatorSnapshotRestoreTest {

	private static final int MAX_PARALLELISM = 10;

	@Test
	public void testOperatorStatesSnapshotRestore() throws Exception {

		//-------------------------------------------------------------------------- snapshot

		TestOneInputStreamOperator op = new TestOneInputStreamOperator(false);

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						op,
						new KeySelector<Integer, Integer>() {
							@Override
							public Integer getKey(Integer value) throws Exception {
								return value;
							}
						},
						TypeInformation.of(Integer.class),
						MAX_PARALLELISM,
						1 /* num subtasks */,
						0 /* subtask index */);

		testHarness.open();

		for (int i = 0; i < 10; ++i) {
			testHarness.processElement(new StreamRecord<>(i));
		}

		OperatorStateHandles handles = testHarness.snapshot(1L, 1L);

		testHarness.close();

		//-------------------------------------------------------------------------- restore

		op = new TestOneInputStreamOperator(true);
		testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
				op,
				new KeySelector<Integer, Integer>() {
					@Override
					public Integer getKey(Integer value) throws Exception {
						return value;
					}
				},
				TypeInformation.of(Integer.class),
				MAX_PARALLELISM,
				1 /* num subtasks */,
				0 /* subtask index */);

		testHarness.initializeState(handles);

		testHarness.open();

		for (int i = 0; i < 10; ++i) {
			testHarness.processElement(new StreamRecord<>(i));
		}

		testHarness.close();
	}

	static class TestOneInputStreamOperator
			extends AbstractStreamOperator<Integer>
			implements OneInputStreamOperator<Integer, Integer> {

		private static final long serialVersionUID = -8942866418598856475L;

		public TestOneInputStreamOperator(boolean verifyRestore) {
			this.verifyRestore = verifyRestore;
		}

		private boolean verifyRestore;
		private ValueState<Integer> keyedState;
		private ListState<Integer> opState;

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (verifyRestore) {
				// check restored managed keyed state
				long exp = element.getValue() + 1;
				long act = keyedState.value();
				Assert.assertEquals(exp, act);
			} else {
				// write managed keyed state that goes into snapshot
				keyedState.update(element.getValue() + 1);
				// write managed operator state that goes into snapshot
				opState.add(element.getValue());
			}
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {

		}

		@Override
		public void snapshotState(StateSnapshotContext context) throws Exception {

			KeyedStateCheckpointOutputStream out = context.getRawKeyedOperatorStateOutput();
			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			// write raw keyed state that goes into snapshot
			int count = 0;
			for (int kg : out.getKeyGroupList()) {
				out.startNewKeyGroup(kg);
				dov.writeInt(kg + 2);
				++count;
			}

			Assert.assertEquals(MAX_PARALLELISM, count);

			// write raw operator state that goes into snapshot
			OperatorStateCheckpointOutputStream outOp = context.getRawOperatorStateOutput();
			dov = new DataOutputViewStreamWrapper(outOp);
			for (int i = 0; i < 13; ++i) {
				outOp.startNewPartition();
				dov.writeInt(42 + i);
			}
		}

		@Override
		public void initializeState(StateInitializationContext context) throws Exception {

			Assert.assertEquals(verifyRestore, context.isRestored());

			keyedState = context
					.getKeyedStateStore()
					.getState(new ValueStateDescriptor<>("managed-keyed", Integer.class, 0));

			opState = context
					.getOperatorStateStore()
					.getListState(new ListStateDescriptor<>("managed-op-state", IntSerializer.INSTANCE));

			if (context.isRestored()) {
				// check restored raw keyed state
				int count = 0;
				for (KeyGroupStatePartitionStreamProvider streamProvider : context.getRawKeyedStateInputs()) {
					try (InputStream in = streamProvider.getStream()) {
						DataInputView div = new DataInputViewStreamWrapper(in);
						Assert.assertEquals(streamProvider.getKeyGroupId() + 2, div.readInt());
						++count;
					}
				}
				Assert.assertEquals(MAX_PARALLELISM, count);

				// check restored managed operator state
				BitSet check = new BitSet(10);
				for (int v : opState.get()) {
					check.set(v);
				}

				Assert.assertEquals(10, check.cardinality());

				// check restored raw operator state
				check = new BitSet(13);
				for (StatePartitionStreamProvider streamProvider : context.getRawOperatorStateInputs()) {
					try (InputStream in = streamProvider.getStream()) {
						DataInputView div = new DataInputViewStreamWrapper(in);
						check.set(div.readInt() - 42);
					}
				}
				Assert.assertEquals(13, check.cardinality());
			}
		}
	}

}
