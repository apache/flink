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

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateHandleDummyUtil;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for {@link OperatorSnapshotFinalizer}.
 */
public class OperatorSnapshotFinalizerTest extends TestLogger {

	/**
	 * Test that the runnable futures are executed and the result is correctly extracted.
	 */
	@Test
	public void testRunAndExtract() throws Exception{

		Random random = new Random(0x42);

		KeyedStateHandle keyedTemplate =
			StateHandleDummyUtil.createNewKeyedStateHandle(new KeyGroupRange(0, 0));
		OperatorStateHandle operatorTemplate =
			StateHandleDummyUtil.createNewOperatorStateHandle(2, random);

		SnapshotResult<KeyedStateHandle> snapKeyMan = SnapshotResult.withLocalState(
			StateHandleDummyUtil.deepDummyCopy(keyedTemplate),
			StateHandleDummyUtil.deepDummyCopy(keyedTemplate));

		SnapshotResult<KeyedStateHandle> snapKeyRaw = SnapshotResult.withLocalState(
			StateHandleDummyUtil.deepDummyCopy(keyedTemplate),
			StateHandleDummyUtil.deepDummyCopy(keyedTemplate));

		SnapshotResult<OperatorStateHandle> snapOpMan = SnapshotResult.withLocalState(
			StateHandleDummyUtil.deepDummyCopy(operatorTemplate),
			StateHandleDummyUtil.deepDummyCopy(operatorTemplate));

		SnapshotResult<OperatorStateHandle> snapOpRaw = SnapshotResult.withLocalState(
			StateHandleDummyUtil.deepDummyCopy(operatorTemplate),
			StateHandleDummyUtil.deepDummyCopy(operatorTemplate));

		DoneFuture<SnapshotResult<KeyedStateHandle>> managedKeyed = new PseudoNotDoneFuture<>(snapKeyMan);
		DoneFuture<SnapshotResult<KeyedStateHandle>> rawKeyed = new PseudoNotDoneFuture<>(snapKeyRaw);
		DoneFuture<SnapshotResult<OperatorStateHandle>> managedOp = new PseudoNotDoneFuture<>(snapOpMan);
		DoneFuture<SnapshotResult<OperatorStateHandle>> rawOp = new PseudoNotDoneFuture<>(snapOpRaw);

		Assert.assertFalse(managedKeyed.isDone());
		Assert.assertFalse(rawKeyed.isDone());
		Assert.assertFalse(managedOp.isDone());
		Assert.assertFalse(rawOp.isDone());

		OperatorSnapshotFutures futures = new OperatorSnapshotFutures(managedKeyed, rawKeyed, managedOp, rawOp);
		OperatorSnapshotFinalizer operatorSnapshotFinalizer = new OperatorSnapshotFinalizer(futures);

		Assert.assertTrue(managedKeyed.isDone());
		Assert.assertTrue(rawKeyed.isDone());
		Assert.assertTrue(managedOp.isDone());
		Assert.assertTrue(rawOp.isDone());

		OperatorSubtaskState jobManagerOwnedState = operatorSnapshotFinalizer.getJobManagerOwnedState();
		Assert.assertTrue(checkResult(snapKeyMan.getJobManagerOwnedSnapshot(), jobManagerOwnedState.getManagedKeyedState()));
		Assert.assertTrue(checkResult(snapKeyRaw.getJobManagerOwnedSnapshot(), jobManagerOwnedState.getRawKeyedState()));
		Assert.assertTrue(checkResult(snapOpMan.getJobManagerOwnedSnapshot(), jobManagerOwnedState.getManagedOperatorState()));
		Assert.assertTrue(checkResult(snapOpRaw.getJobManagerOwnedSnapshot(), jobManagerOwnedState.getRawOperatorState()));

		OperatorSubtaskState taskLocalState = operatorSnapshotFinalizer.getTaskLocalState();
		Assert.assertTrue(checkResult(snapKeyMan.getTaskLocalSnapshot(), taskLocalState.getManagedKeyedState()));
		Assert.assertTrue(checkResult(snapKeyRaw.getTaskLocalSnapshot(), taskLocalState.getRawKeyedState()));
		Assert.assertTrue(checkResult(snapOpMan.getTaskLocalSnapshot(), taskLocalState.getManagedOperatorState()));
		Assert.assertTrue(checkResult(snapOpRaw.getTaskLocalSnapshot(), taskLocalState.getRawOperatorState()));
	}

	private <T extends StateObject> boolean checkResult(T expected, StateObjectCollection<T> actual) {
		if (expected == null) {
			return actual.isEmpty();
		}

		return actual.size() == 1 && expected == actual.iterator().next();
	}

	static class PseudoNotDoneFuture<T> extends DoneFuture<T> {

		private boolean done;

		PseudoNotDoneFuture(T payload) {
			super(payload);
			this.done = false;
		}

		@Override
		public void run() {
			super.run();
			this.done = true;
		}

		@Override
		public boolean isDone() {
			return done;
		}
	}
}
