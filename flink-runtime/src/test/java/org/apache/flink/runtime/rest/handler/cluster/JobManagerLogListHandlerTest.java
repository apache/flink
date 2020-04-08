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
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for the {@link JobManagerLogListHandler}.
 */
public class JobManagerLogListHandlerTest extends TestLogger {

	private DispatcherGateway mockRestfulGateway;

	private static HandlerRequest<EmptyRequestBody, EmptyMessageParameters> testRequest;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@BeforeClass
	public static void initHandlerRequest() throws HandlerRequestException {
		testRequest = new HandlerRequest<>(EmptyRequestBody.getInstance(), EmptyMessageParameters.getInstance(), Collections.emptyMap(), Collections.emptyMap());
	}

	@Before
	public void setUp() throws Exception {
		mockRestfulGateway = new TestingDispatcherGateway.Builder().build();
	}

	@Test
	public void testGetJobManagerLogsList() throws Exception {
		LogInfo jobmanagerLog = new LogInfo("jobmanager.log", 0L);
		LogInfo jobmanagerStdout = new LogInfo("jobmanager.out", 0L);
		LogInfo customLog = new LogInfo("test.log", 0L);
		Map<String, LogInfo> fileNameAndLogInfoMap = new HashMap<>(3);
		fileNameAndLogInfoMap.put(jobmanagerLog.getName(), jobmanagerLog);
		fileNameAndLogInfoMap.put(jobmanagerStdout.getName(), jobmanagerStdout);
		fileNameAndLogInfoMap.put(customLog.getName(), customLog);
		JobManagerLogListHandler jobManagerLogListHandler = createHandler(fileNameAndLogInfoMap.values());
		LogListInfo logListInfo = jobManagerLogListHandler.handleRequest(testRequest, mockRestfulGateway).get();
		assertThat(logListInfo.getLogInfos(), hasSize(fileNameAndLogInfoMap.size()));
		for (LogInfo logInfo : logListInfo.getLogInfos()) {
			assertEquals(logInfo, fileNameAndLogInfoMap.get(logInfo.getName()));
		}
	}

	@Test
	public void testGetJobManagerLogsListWhenLogDirIsNull() throws Exception {
		JobManagerLogListHandler jobManagerLogListHandler = createHandler(Collections.emptyList());
		LogListInfo logListInfo = jobManagerLogListHandler.handleRequest(testRequest, mockRestfulGateway).get();
		assertThat(logListInfo.getLogInfos(), is(empty()));
	}

	private JobManagerLogListHandler createHandler(Collection<LogInfo> logsList) {
		WebMonitorUtils.LogFileLocation logFileLocation = createLogFileLocation(logsList);
		return new JobManagerLogListHandler(
			() -> CompletableFuture.completedFuture(null),
			TestingUtils.TIMEOUT(),
			Collections.emptyMap(),
			JobManagerLogListHeaders.getInstance(),
			logFileLocation.logDir);
	}

	private WebMonitorUtils.LogFileLocation createLogFileLocation(Collection<LogInfo> logsList) {
		Configuration config = new Configuration();
		try {
			for (LogInfo logInfo : logsList) {
				File file = temporaryFolder.newFile(logInfo.getName());
				if ("jobmanager.log".equals(logInfo.getName())){
					config.setString(WebOptions.LOG_PATH, file.getAbsolutePath());
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not setup test.", e);
		}
		return WebMonitorUtils.LogFileLocation.find(config);
	}

}
