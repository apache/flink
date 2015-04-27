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

package org.apache.flink.runtime.taskmanager;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;

/**
 * Correctness tests for hash/equals and serialization for the
 * {@link org.apache.flink.runtime.taskmanager.TaskExecutionState}.
 */
public class TaskExecutionStateTest {

	@Test
	public void testEqualsHashCode() {
		try {
			final JobID jid = new JobID();
			final ExecutionAttemptID executionId = new ExecutionAttemptID();
			final ExecutionState state = ExecutionState.RUNNING;
			final Throwable error = new RuntimeException("some test error message");
			
			TaskExecutionState s1 = new TaskExecutionState(jid, executionId, state, error);
			TaskExecutionState s2 = new TaskExecutionState(jid, executionId, state, error);
			
			assertEquals(s1.hashCode(), s2.hashCode());
			assertEquals(s1, s2);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialization() {
		try {
			final JobID jid = new JobID();
			final ExecutionAttemptID executionId = new ExecutionAttemptID();
			final ExecutionState state = ExecutionState.DEPLOYING;
			final Throwable error = new IOException("fubar");
			
			TaskExecutionState original1 = new TaskExecutionState(jid, executionId, state, error);
			TaskExecutionState original2 = new TaskExecutionState(jid, executionId, state);
			
			TaskExecutionState javaSerCopy1 = CommonTestUtils.createCopySerializable(original1);
			TaskExecutionState javaSerCopy2 = CommonTestUtils.createCopySerializable(original2);

			// equalities
			assertEquals(original1, javaSerCopy1);
			assertEquals(javaSerCopy1, original1);

			assertEquals(original2, javaSerCopy2);
			assertEquals(javaSerCopy2, original2);

			// hash codes
			assertEquals(original1.hashCode(), javaSerCopy1.hashCode());
			assertEquals(original2.hashCode(), javaSerCopy2.hashCode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
