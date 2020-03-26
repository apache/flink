/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.cluster.JobManagerLogListHeaders;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Test for the {@link JobManagerLogListHandler}.
 */
public class JobManagerLogListHandlerTest {

	private static final DispatcherGateway mockRestfulGateway = new TestingDispatcherGateway.Builder().build();
	private HandlerRequest<EmptyRequestBody, EmptyMessageParameters> handlerRequest;
	private Configuration config;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setUp() throws Exception {
		handlerRequest = createRequest();
		config = new Configuration();
	}

	@Test
	public void testGetJobManagerLogsList() throws Exception {
		List<LogInfo> logsList = Arrays.asList(
			new LogInfo("jobmanager.log", 0L),
			new LogInfo("jobmanager.out", 0L),
			new LogInfo("test.log", 0L));
		JobManagerLogListHandler jobManagerLogListHandler = createHandler(logsList);
		LogListInfo logListInfo = jobManagerLogListHandler.handleRequest(handlerRequest, mockRestfulGateway).get();
		assertThat(logListInfo.getLogInfos(), hasSize(logsList.size()));
	}

	@Test
	public void testGetJobManagerLogsListForLogDirIsNull() throws Exception {
		JobManagerLogListHandler jobManagerLogListHandler = createHandler(Collections.emptyList());
		LogListInfo logListInfo = jobManagerLogListHandler.handleRequest(handlerRequest, mockRestfulGateway).get();
		assertThat(logListInfo.getLogInfos(), hasSize(0));
	}

	private static HandlerRequest<EmptyRequestBody, EmptyMessageParameters> createRequest() throws HandlerRequestException {
		Map<String, String> pathParameters = Collections.emptyMap();
		Map<String, List<String>> queryParameters = Collections.emptyMap();

		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			EmptyMessageParameters.getInstance(),
			pathParameters,
			queryParameters);
	}

	private JobManagerLogListHandler createHandler(List<LogInfo> logsList) {
		WebMonitorUtils.LogFileLocation logFileLocation = createLogFileLocation(logsList);
		return new JobManagerLogListHandler(
			() -> CompletableFuture.completedFuture(null),
			TestingUtils.TIMEOUT(),
			Collections.emptyMap(),
			JobManagerLogListHeaders.getInstance(),
			logFileLocation);
	}

	private WebMonitorUtils.LogFileLocation createLogFileLocation(List<LogInfo> logsList) {
		try {
			for (LogInfo logInfo : logsList) {
				File file = temporaryFolder.newFile(logInfo.getName());
				config.setString(WebOptions.LOG_PATH, file.getAbsolutePath());
			}
		} catch (Exception e) {
			throw new AssertionError("Could not setup test.", e);
		}
		return WebMonitorUtils.LogFileLocation.find(config);
	}

}
