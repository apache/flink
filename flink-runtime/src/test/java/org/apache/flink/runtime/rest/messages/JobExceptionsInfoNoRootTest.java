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

package org.apache.flink.runtime.rest.messages;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests that the {@link JobExceptionsInfo} with no root exception can be marshalled and unmarshalled.
 */
public class JobExceptionsInfoNoRootTest extends RestResponseMarshallingTestBase<JobExceptionsInfo> {
	@Override
	protected Class<JobExceptionsInfo> getTestResponseClass() {
		return JobExceptionsInfo.class;
	}

	@Override
	protected JobExceptionsInfo getTestResponseInstance() throws Exception {
		List<JobExceptionsInfo.ExecutionExceptionInfo> executionTaskExceptionInfoList = new ArrayList<>();
		executionTaskExceptionInfoList.add(new JobExceptionsInfo.ExecutionExceptionInfo(
			"exception1",
			"task1",
			"location1",
			System.currentTimeMillis()));
		executionTaskExceptionInfoList.add(new JobExceptionsInfo.ExecutionExceptionInfo(
			"exception2",
			"task2",
			"location2",
			System.currentTimeMillis()));
		return new JobExceptionsInfo(
			null,
			null,
			executionTaskExceptionInfoList,
			false);
	}
}
