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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * {@link LeaderContender} implementation which provides some convenience functions for testing
 * purposes.
 */
public class TestingContender implements LeaderContender {
	private static final Logger LOG = LoggerFactory.getLogger(TestingContender.class);

	private final String address;
	private final LeaderElectionService leaderElectionService;
	private UUID leaderSessionID = null;
	private boolean leader = false;
	private Throwable error = null;

	private Object lock = new Object();
	private Object errorLock = new Object();

	public TestingContender(
			final String address,
			final LeaderElectionService leaderElectionService) {
		this.address = address;
		this.leaderElectionService = leaderElectionService;
	}

	/**
	 * Waits until the contender becomes the leader or until the timeout has been exceeded.
	 *
	 * @param timeout
	 * @throws TimeoutException
	 */
	public void waitForLeader(long timeout) throws TimeoutException {
		long start = System.currentTimeMillis();
		long curTimeout;

		while (!isLeader() && (curTimeout = timeout - System.currentTimeMillis() + start) > 0) {
			synchronized (lock) {
				try {
					lock.wait(curTimeout);
				} catch (InterruptedException e) {
					// we got interrupted so check again for the condition
				}
			}
		}

		if (!isLeader()) {
			throw new TimeoutException("Contender was not elected as the leader within " +
					timeout + "ms");
		}
	}

	/**
	 * Waits until an error has been found or until the timeout has been exceeded.
	 *
	 * @param timeout
	 * @throws TimeoutException
	 */
	public void waitForError(long timeout) throws TimeoutException {
		long start = System.currentTimeMillis();
		long curTimeout;

		while (error == null && (curTimeout = timeout - System.currentTimeMillis() + start) > 0) {
			synchronized (errorLock) {
				try {
					errorLock.wait(curTimeout);
				} catch (InterruptedException e) {
					// we got interrupted so check again for the condition
				}
			}
		}

		if (error == null) {
			throw new TimeoutException("Contender did not see an exception in " +
					timeout + "ms");
		}
	}

	public UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	public Throwable getError() {
		return error;
	}

	public boolean isLeader() {
		return leader;
	}

	@Override
	public void grantLeadership(UUID leaderSessionID) {
		synchronized (lock) {
			LOG.debug("Was granted leadership with session ID {}.", leaderSessionID);

			this.leaderSessionID = leaderSessionID;

			leaderElectionService.confirmLeaderSessionID(leaderSessionID);

			leader = true;

			lock.notifyAll();
		}
	}

	@Override
	public void revokeLeadership() {
		synchronized (lock) {
			LOG.debug("Was revoked leadership. Old session ID {}.", leaderSessionID);

			leader = false;
			leaderSessionID = null;

			lock.notifyAll();
		}
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public void handleError(Exception exception) {
		synchronized (errorLock) {
			this.error = exception;

			errorLock.notifyAll();
		}
	}
}
