/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;

class TestingDispatcherLeaderProcessFactory implements DispatcherLeaderProcessFactory {
	private final Queue<TestingDispatcherLeaderProcess> processes;

	private TestingDispatcherLeaderProcessFactory(Queue<TestingDispatcherLeaderProcess> processes) {
		this.processes = processes;
	}

	@Override
	public TestingDispatcherLeaderProcess create(UUID leaderSessionID) {
		if (processes.isEmpty()) {
			return TestingDispatcherLeaderProcess.newBuilder(leaderSessionID).build();
		} else {
			final TestingDispatcherLeaderProcess nextProcess = processes.poll();
			Preconditions.checkState(leaderSessionID.equals(nextProcess.getLeaderSessionId()));

			return nextProcess;
		}
	}

	public static TestingDispatcherLeaderProcessFactory from(TestingDispatcherLeaderProcess... processes) {
		return new TestingDispatcherLeaderProcessFactory(new ArrayDeque<>(Arrays.asList(processes)));
	}

	public static TestingDispatcherLeaderProcessFactory defaultValue() {
		return new TestingDispatcherLeaderProcessFactory(new ArrayDeque<>(0));
	}
}
