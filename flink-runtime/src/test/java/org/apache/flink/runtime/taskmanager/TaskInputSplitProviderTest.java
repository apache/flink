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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

public class TaskInputSplitProviderTest {

	@Test
	public void testRequestNextInputSplitWithInvalidExecutionID() throws InputSplitProviderException {

		final JobID jobID = new JobID();
		final JobVertexID vertexID = new JobVertexID();
		final ExecutionAttemptID executionID = new ExecutionAttemptID();
		final FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

		final ActorGateway gateway = new NullInputSplitGateway();


		final TaskInputSplitProvider provider = new TaskInputSplitProvider(
			gateway,
			jobID,
			vertexID,
			executionID,
			timeout);

		// The jobManager will return a
		InputSplit nextInputSplit = provider.getNextInputSplit(getClass().getClassLoader());

		assertTrue(nextInputSplit == null);
	}

	public static class NullInputSplitGateway extends BaseTestingActorGateway {

		private static final long serialVersionUID = -7733997150554492926L;

		public NullInputSplitGateway() {
			super(TestingUtils.defaultExecutionContext());
		}

		@Override
		public Object handleMessage(Object message) throws Exception {
			if(message instanceof JobManagerMessages.RequestNextInputSplit) {
				return new JobManagerMessages.NextInputSplit(null);
			} else {
				throw new Exception("Invalid message type");
			}
		}
	}
}
