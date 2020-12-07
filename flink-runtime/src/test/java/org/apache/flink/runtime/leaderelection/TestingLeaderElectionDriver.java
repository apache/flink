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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.rpc.FatalErrorHandler;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link LeaderElectionDriver} implementation which provides some convenience functions for testing purposes. Please
 * use {@link #isLeader} and {@link #notLeader()} to manually control the leadership.
 */
public class TestingLeaderElectionDriver implements LeaderElectionDriver {

	private final Object lock = new Object();

	private final AtomicBoolean isLeader = new AtomicBoolean(false);
	private final LeaderElectionEventHandler leaderElectionEventHandler;
	private final FatalErrorHandler fatalErrorHandler;

	// Leader information on external storage
	private LeaderInformation leaderInformation = LeaderInformation.empty();

	private TestingLeaderElectionDriver(
			LeaderElectionEventHandler leaderElectionEventHandler,
			FatalErrorHandler fatalErrorHandler) {
		this.leaderElectionEventHandler = leaderElectionEventHandler;
		this.fatalErrorHandler = fatalErrorHandler;
	}

	@Override
	public void writeLeaderInformation(LeaderInformation leaderInformation) {
		this.leaderInformation = leaderInformation;
	}

	@Override
	public boolean hasLeadership() {
		return isLeader.get();
	}

	@Override
	public void close() throws Exception {
		synchronized (lock) {
			// noop
		}
	}

	public LeaderInformation getLeaderInformation() {
		return leaderInformation;
	}

	public void isLeader() {
		synchronized (lock) {
			isLeader.set(true);
			leaderElectionEventHandler.onGrantLeadership();
		}
	}

	public void notLeader() {
		synchronized (lock) {
			isLeader.set(false);
			leaderElectionEventHandler.onRevokeLeadership();
		}
	}

	public void leaderInformationChanged(LeaderInformation newLeader) {
		leaderInformation = newLeader;
		leaderElectionEventHandler.onLeaderInformationChange(newLeader);
	}

	public void onFatalError(Throwable throwable) {
		fatalErrorHandler.onFatalError(throwable);
	}

	/**
	 * Factory for create {@link TestingLeaderElectionDriver}.
	 */
	public static class TestingLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

		private TestingLeaderElectionDriver currentLeaderDriver;

		@Override
		public LeaderElectionDriver createLeaderElectionDriver(
				LeaderElectionEventHandler leaderEventHandler,
				FatalErrorHandler fatalErrorHandler,
				String leaderContenderDescription) {
			currentLeaderDriver = new TestingLeaderElectionDriver(leaderEventHandler, fatalErrorHandler);
			return currentLeaderDriver;
		}

		@Nullable
		public TestingLeaderElectionDriver getCurrentLeaderDriver() {
			return currentLeaderDriver;
		}
	}
}
