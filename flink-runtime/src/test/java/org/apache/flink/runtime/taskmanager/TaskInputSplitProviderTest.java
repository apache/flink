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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.UntypedActor;
import akka.testkit.JavaTestKit;
import akka.util.Timeout;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

public class TaskInputSplitProviderTest {

	private static ActorSystem system;

	@BeforeClass
	public static void setup() throws Exception {
		system = ActorSystem.create("TestActorSystem", TestingUtils.testConfig());
	}

	@AfterClass
	public static void teardown() throws Exception {
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	@Test
	public void testRequestNextInputSplitWithInvalidExecutionID() {

		final JobID jobID = new JobID();
		final JobVertexID vertexID = new JobVertexID();
		final ExecutionAttemptID executionID = new ExecutionAttemptID();
		final Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

		final ActorRef jobManagerRef = system.actorOf(Props.create(NullInputSplitJobManager.class));

		final TaskInputSplitProvider provider = new TaskInputSplitProvider(
				jobManagerRef,
				jobID,
				vertexID,
				executionID,
				getClass().getClassLoader(),
				timeout
		);

		// The jobManager will return a
		InputSplit nextInputSplit = provider.getNextInputSplit();

		assertTrue(nextInputSplit == null);
	}

	public static class NullInputSplitJobManager extends UntypedActor {

		@Override
		public void onReceive(Object message) throws Exception {
			if(message instanceof JobManagerMessages.RequestNextInputSplit) {
				sender().tell(new JobManagerMessages.NextInputSplit(null), getSelf());
			} else {
				sender().tell(new Status.Failure(new Exception("Invalid message type")), getSelf());
			}
		}
	}
}
