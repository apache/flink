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

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Test {@link LeaderRetrievalListener} implementation which offers some convenience functions for
 * testing purposes.
 */
public class TestingListener implements LeaderRetrievalListener {
	private static Logger LOG = LoggerFactory.getLogger(TestingListener.class);

	private String address;
	private String oldAddress;
	private UUID leaderSessionID;
	private Exception exception;

	private Object lock = new Object();

	public String getAddress() {
		return address;
	}

	public UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	public String waitForNewLeader(long timeout) throws Exception {
		long start = System.currentTimeMillis();
		long curTimeout;

		synchronized (lock) {
			while (
				exception == null &&
					(address == null || address.equals(oldAddress)) &&
					(curTimeout = timeout - System.currentTimeMillis() + start) > 0) {
				try {
					lock.wait(curTimeout);
				} catch (InterruptedException e) {
					// we got interrupted so check again for the condition
				}
			}
		}

		if (exception != null) {
			throw exception;
		} else if (address == null || address.equals(oldAddress)) {
			throw new TimeoutException("Listener was not notified about a leader within " +
					timeout + "ms");
		}

		oldAddress = address;

		return address;
	}

	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
		synchronized (lock) {
			LOG.debug("Notified about new leader address {} with session ID {}.", leaderAddress, leaderSessionID);

			this.address = leaderAddress;
			this.leaderSessionID = leaderSessionID;

			lock.notifyAll();
		}
	}

	@Override
	public void handleError(Exception exception) {
		this.exception = exception;
	}
}
