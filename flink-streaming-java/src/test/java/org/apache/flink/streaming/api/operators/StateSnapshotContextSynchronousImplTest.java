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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StateSnapshotContextSynchronousImplTest {

	private StateSnapshotContextSynchronousImpl snapshotContext;

	@Before
	public void setUp() throws Exception {
		CloseableRegistry closableRegistry = new CloseableRegistry();
		CheckpointStreamFactory streamFactory = new MemCheckpointStreamFactory(1024);
		KeyGroupRange keyGroupRange = new KeyGroupRange(0, 2);
		this.snapshotContext = new StateSnapshotContextSynchronousImpl(42, 4711, streamFactory, keyGroupRange, closableRegistry);
	}

	@Test
	public void testMetaData() {
		Assert.assertEquals(42, snapshotContext.getCheckpointId());
		Assert.assertEquals(4711, snapshotContext.getCheckpointTimestamp());
	}

	@Test
	public void testCreateRawKeyedStateOutput() throws Exception {
		KeyedStateCheckpointOutputStream stream = snapshotContext.getRawKeyedOperatorStateOutput();
		Assert.assertNotNull(stream);
	}

	@Test
	public void testCreateRawOperatorStateOutput() throws Exception {
		OperatorStateCheckpointOutputStream stream = snapshotContext.getRawOperatorStateOutput();
		Assert.assertNotNull(stream);
	}
}