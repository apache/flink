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

import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;

import java.io.IOException;
import java.util.Arrays;

/**
 * Tests for the {@link JobSubmitRequestBody}.
 */
public class JobSubmitRequestBodyTest extends RestRequestMarshallingTestBase<JobSubmitRequestBody> {

	@Override
	protected Class<JobSubmitRequestBody> getTestRequestClass() {
		return JobSubmitRequestBody.class;
	}

	@Override
	protected JobSubmitRequestBody getTestRequestInstance() throws IOException {
		return new JobSubmitRequestBody(
			"jobgraph",
			Arrays.asList("jar1", "jar2"),
			Arrays.asList(
				new JobSubmitRequestBody.DistributedCacheFile("entry1", "artifact1"),
				new JobSubmitRequestBody.DistributedCacheFile("entry2", "artifact2")));
	}
}
