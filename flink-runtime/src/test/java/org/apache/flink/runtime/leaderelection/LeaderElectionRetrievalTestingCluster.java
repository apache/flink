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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StreamingMode;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import scala.Option;

import java.util.UUID;

/**
 * A testing cluster which allows to manually trigger grantLeadership and notifyRetrievalListener
 * events. The grantLeadership event assigns the specified JobManager the leadership. The
 * notifyRetrievalListener notifies all listeners that the specified JobManager (index) has been
 * granted the leadership.
 */
public class LeaderElectionRetrievalTestingCluster extends TestingCluster {

	private final Configuration userConfiguration;
	private final boolean useSingleActorSystem;
	private final StreamingMode streamingMode;

	public TestingLeaderElectionService[] leaderElectionServices;
	public TestingLeaderRetrievalService[] leaderRetrievalServices;

	private int leaderElectionServiceCounter = 0;
	private int leaderRetrievalServiceCounter = 0;

	private int leaderIndex = -1;

	public LeaderElectionRetrievalTestingCluster(
			Configuration userConfiguration,
			boolean singleActorSystem,
			boolean synchronousDispatcher,
			StreamingMode streamingMode) {
		super(userConfiguration, singleActorSystem, synchronousDispatcher, streamingMode);

		this.userConfiguration = userConfiguration;
		this.useSingleActorSystem = singleActorSystem;
		this.streamingMode = streamingMode;

		leaderElectionServices = new TestingLeaderElectionService[this.numJobManagers()];
		leaderRetrievalServices = new TestingLeaderRetrievalService[this.numTaskManagers() + 1];
	}

	@Override
	public Configuration userConfiguration() {
		return this.userConfiguration;
	}

	@Override
	public StreamingMode streamingMode() {
		return streamingMode;
	}

	@Override
	public boolean useSingleActorSystem() {
		return useSingleActorSystem;
	}

	@Override
	public Option<LeaderElectionService> createLeaderElectionService() {
		leaderElectionServices[leaderElectionServiceCounter] = new TestingLeaderElectionService();

		LeaderElectionService result = leaderElectionServices[leaderElectionServiceCounter++];

		return Option.apply(result);
	}

	@Override
	public LeaderRetrievalService createLeaderRetrievalService() {
		leaderRetrievalServices[leaderRetrievalServiceCounter] = new TestingLeaderRetrievalService();

		return leaderRetrievalServices[leaderRetrievalServiceCounter++];
	}

	@Override
	public int getNumberOfJobManagers() {
		return this.configuration().getInteger(
				ConfigConstants.LOCAL_NUMBER_JOB_MANAGER,
				ConfigConstants.DEFAULT_LOCAL_NUMBER_JOB_MANAGER);
	}

	public void grantLeadership(int index, UUID leaderSessionID) {
		if(leaderIndex >= 0) {
			// first revoke leadership
			leaderElectionServices[leaderIndex].notLeader();
		}

		// make the JM with index the new leader
		leaderElectionServices[index].isLeader(leaderSessionID);

		leaderIndex = index;
	}

	public void notifyRetrievalListeners(int index, UUID leaderSessionID) {
		String address = jobManagerActors().get().apply(index).path().toString();

		for(TestingLeaderRetrievalService service: leaderRetrievalServices) {
			service.notifyListener(address, leaderSessionID);
		}
	}
}
