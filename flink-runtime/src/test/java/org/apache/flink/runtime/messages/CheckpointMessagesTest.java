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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.apache.flink.runtime.state.CheckpointStateHandles;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class CheckpointMessagesTest {

	@Test
	public void testTriggerAndConfirmCheckpoint() {
		try {
			NotifyCheckpointComplete cc = new NotifyCheckpointComplete(new JobID(), new ExecutionAttemptID(), 45287698767345L, 467L);
			testSerializabilityEqualsHashCode(cc);

			TriggerCheckpoint tc = new TriggerCheckpoint(new JobID(), new ExecutionAttemptID(), 347652734L, 7576752L);
			testSerializabilityEqualsHashCode(tc);

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testConfirmTaskCheckpointed() {
		try {
			AcknowledgeCheckpoint noState = new AcknowledgeCheckpoint(
					new JobID(), new ExecutionAttemptID(), 569345L);

			KeyGroupRange keyGroupRange = KeyGroupRange.of(42,42);

			CheckpointStateHandles checkpointStateHandles =
					new CheckpointStateHandles(
							CheckpointCoordinatorTest.generateChainedStateHandle(new MyHandle()),
							CheckpointCoordinatorTest.generateChainedPartitionableStateHandle(new JobVertexID(), 0, 2, 8),
							CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, Collections.singletonList(new MyHandle())));

			AcknowledgeCheckpoint withState = new AcknowledgeCheckpoint(
					new JobID(),
					new ExecutionAttemptID(),
					87658976143L,
					checkpointStateHandles);

			testSerializabilityEqualsHashCode(noState);
			testSerializabilityEqualsHashCode(withState);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static void testSerializabilityEqualsHashCode(Serializable o) throws IOException {
		Object copy = CommonTestUtils.createCopySerializable(o);
		assertEquals(o, copy);
		assertEquals(o.hashCode(), copy.hashCode());
		assertNotNull(o.toString());
		assertNotNull(copy.toString());
	}

	public static class MyHandle implements StreamStateHandle {

		private static final long serialVersionUID = 8128146204128728332L;

		public Serializable get(ClassLoader userCodeClassLoader) {
			return null;
		}

		@Override
		public boolean equals(Object obj) {
			return obj.getClass() == this.getClass();
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public void discardState() throws Exception {}

		@Override
		public long getStateSize() {
			return 0;
		}

		@Override
		public FSDataInputStream openInputStream() throws IOException {
			return null;
		}
	}
}
