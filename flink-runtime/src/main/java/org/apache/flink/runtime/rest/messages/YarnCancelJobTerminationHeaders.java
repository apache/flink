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

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.handler.job.JobCancellationHandler;

/**
 * {@link RestHandlerSpecification} for the {@link JobCancellationHandler} which is registered for
 * compatibility with the Yarn proxy as a GET call.
 *
 * <p>For more information @see <a href="https://issues.apache.org/jira/browse/YARN-2031">YARN-2031</a>.
 *
 * @deprecated This should be removed once we can send arbitrary REST calls via the Yarn proxy.
 */
@Deprecated
public class YarnCancelJobTerminationHeaders implements RestHandlerSpecification {

	private static final YarnCancelJobTerminationHeaders INSTANCE = new YarnCancelJobTerminationHeaders();

	private static final String URL = String.format("/jobs/:%s/yarn-cancel", JobIDPathParameter.KEY);

	private YarnCancelJobTerminationHeaders() {}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static YarnCancelJobTerminationHeaders getInstance() {
		return INSTANCE;
	}
}
