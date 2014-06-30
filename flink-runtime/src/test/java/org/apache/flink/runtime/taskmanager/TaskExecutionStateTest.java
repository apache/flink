/**
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

import org.apache.flink.runtime.execution.ExecutionState2;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;

public class TaskExecutionStateTest {

	@Test
	public void testEqualsHashCode() {
		try {
			final JobID jid = new JobID();
			final ExecutionAttemptID executionId = new ExecutionAttemptID();
			final ExecutionState2 state = ExecutionState2.RUNNING;
			final String description = "some test description";
			
			TaskExecutionState s1 = new TaskExecutionState(jid, executionId, state, description);
			TaskExecutionState s2 = new TaskExecutionState(jid, executionId, state, description);
			
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
			final ExecutionState2 state = ExecutionState2.DEPLOYING;
			final String description = "foo bar";
			
			TaskExecutionState original = new TaskExecutionState(jid, executionId, state, description);
			
			TaskExecutionState writableCopy = CommonTestUtils.createCopyWritable(original);
			TaskExecutionState javaSerCopy = CommonTestUtils.createCopySerializable(original);
			
			assertEquals(original, writableCopy);
			assertEquals(original, javaSerCopy);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
