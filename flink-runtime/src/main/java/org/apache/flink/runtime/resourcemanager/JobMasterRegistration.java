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

import org.apache.flink.runtime.jobmaster.JobMasterGateway;

import java.io.Serializable;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for group the JobMasterGateway and the LeaderSessionID of a registered job master
 */
public class JobMasterRegistration implements Serializable {

	private static final long serialVersionUID = -2316627821716999527L;

	private final JobMasterGateway jobMasterGateway;

	private UUID jobMasterLeaderSessionID;

	public JobMasterRegistration(JobMasterGateway jobMasterGateway) {
		this.jobMasterGateway = checkNotNull(jobMasterGateway);
	}

	public JobMasterRegistration(JobMasterGateway jobMasterGateway, UUID jobMasterLeaderSessionID) {
		this.jobMasterGateway = checkNotNull(jobMasterGateway);
		this.jobMasterLeaderSessionID = jobMasterLeaderSessionID;
	}

	public JobMasterGateway getJobMasterGateway() {
		return jobMasterGateway;
	}

	public void setJobMasterLeaderSessionID(UUID leaderSessionID) {
		this.jobMasterLeaderSessionID = jobMasterLeaderSessionID;
	}

	public UUID getJobMasterLeaderSessionID() {
		return jobMasterLeaderSessionID;
	}
}
