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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import java.util.Arrays;

/**
 * Marshalling test for the {@link JobIdsWithStatusesOverview} message.
 */
public class JobIdsWithStatusesOverviewTest extends RestResponseMarshallingTestBase<JobIdsWithStatusesOverview> {

	@Override
	protected Class<JobIdsWithStatusesOverview> getTestResponseClass() {
		return JobIdsWithStatusesOverview.class;
	}

	@Override
	protected JobIdsWithStatusesOverview getTestResponseInstance() {
		return new JobIdsWithStatusesOverview(Arrays.asList(
			Tuple2.of(JobID.generate(), JobStatus.RUNNING),
			Tuple2.of(JobID.generate(), JobStatus.CANCELED),
			Tuple2.of(JobID.generate(), JobStatus.CREATED),
			Tuple2.of(JobID.generate(), JobStatus.FAILED),
			Tuple2.of(JobID.generate(), JobStatus.RESTARTING)));
	}
}
