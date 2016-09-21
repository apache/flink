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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;

import java.util.UUID;

/**
 * This class is responsible for group the JobMasterGateway and the LeaderSessionID of a registered job master
 */
public class JobMasterRegistration implements LeaderRetrievalListener {

	private final JobMasterGateway gateway;
	private final JobID jobID;
	private final UUID leaderSessionID;
	private LeaderRetrievalListener retriever;

	public JobMasterRegistration(JobMasterGateway gateway, JobID jobID, UUID leaderSessionID) {
		this.gateway = gateway;
		this.jobID = jobID;
		this.leaderSessionID = leaderSessionID;
	}

	public JobMasterGateway getGateway() {
		return gateway;
	}

	public UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	public JobID getJobID() {
		return jobID;
	}

	@Override
	public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
		
	}

	@Override
	public void handleError(Exception exception) {

	}
}
