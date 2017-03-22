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
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * Test {@link LeaderRetrievalService} implementation which directly forwards calls of
 * notifyListener to the listener.
 */
public class TestingLeaderRetrievalService implements LeaderRetrievalService {

	private volatile String leaderAddress;
	private volatile UUID leaderSessionID;

	private volatile LeaderRetrievalListener listener;

	public TestingLeaderRetrievalService(String leaderAddress, UUID leaderSessionID) {
		this.leaderAddress = leaderAddress;
		this.leaderSessionID = leaderSessionID;
	}

	@Override
	public void start(LeaderRetrievalListener listener) throws Exception {
		this.listener = Preconditions.checkNotNull(listener);

		if (leaderSessionID != null && leaderAddress != null) {
			listener.notifyLeaderAddress(leaderAddress, leaderSessionID);
		}
	}

	@Override
	public void stop() throws Exception {

	}

	public void notifyListener(String address, UUID leaderSessionID) {
		this.leaderAddress = address;
		this.leaderSessionID = leaderSessionID;

		if (listener != null) {
			listener.notifyLeaderAddress(address, leaderSessionID);
		}
	}
}
