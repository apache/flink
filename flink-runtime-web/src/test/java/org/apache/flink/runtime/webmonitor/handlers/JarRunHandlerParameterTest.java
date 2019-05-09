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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;

import org.junit.Assert;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Tests for the parameter handling of the {@link JarRunHandler}.
 */
public class JarRunHandlerParameterTest extends JarHandlerParameterTest<JarRunRequestBody, JarRunMessageParameters> {
	private static final boolean ALLOW_NON_RESTORED_STATE_QUERY = true;
	private static final String RESTORE_PATH = "/foo/bar";

	private static JarRunHandler handler;

	@BeforeClass
	public static void setup() throws Exception {
		init();
		final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
		final Time timeout = Time.seconds(10);
		final Map<String, String> responseHeaders = Collections.emptyMap();
		final Executor executor = TestingUtils.defaultExecutor();

		handler = new JarRunHandler(
			gatewayRetriever,
			timeout,
			responseHeaders,
			JarRunHeaders.getInstance(),
			jarDir,
			new Configuration(),
			executor);
	}

	@Override
	JarRunMessageParameters getUnresolvedJarMessageParameters() {
		return handler.getMessageHeaders().getUnresolvedMessageParameters();
	}

	@Override
	JarRunMessageParameters getJarMessageParameters(ProgramArgsParType programArgsParType) {
		final JarRunMessageParameters parameters = getUnresolvedJarMessageParameters();
		parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(ALLOW_NON_RESTORED_STATE_QUERY));
		parameters.savepointPathQueryParameter.resolve(Collections.singletonList(RESTORE_PATH));
		parameters.entryClassQueryParameter.resolve(Collections.singletonList(ParameterProgram.class.getCanonicalName()));
		parameters.parallelismQueryParameter.resolve(Collections.singletonList(PARALLELISM));
		if (programArgsParType == ProgramArgsParType.String ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgsQueryParameter.resolve(Collections.singletonList(String.join(" ", PROG_ARGS)));
		}
		if (programArgsParType == ProgramArgsParType.List ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgQueryParameter.resolve(Arrays.asList(PROG_ARGS));
		}
		return parameters;
	}

	@Override
	JarRunMessageParameters getWrongJarMessageParameters(ProgramArgsParType programArgsParType) {
		List<String> wrongArgs = Arrays.stream(PROG_ARGS).map(a -> a + "wrong").collect(Collectors.toList());
		String argsWrongStr = String.join(" ", wrongArgs);
		final JarRunMessageParameters parameters = getUnresolvedJarMessageParameters();
		parameters.allowNonRestoredStateQueryParameter.resolve(Collections.singletonList(false));
		parameters.savepointPathQueryParameter.resolve(Collections.singletonList("/no/uh"));
		parameters.entryClassQueryParameter.resolve(Collections.singletonList("please.dont.run.me"));
		parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
		if (programArgsParType == ProgramArgsParType.String ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgsQueryParameter.resolve(Collections.singletonList(argsWrongStr));
		}
		if (programArgsParType == ProgramArgsParType.List ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgQueryParameter.resolve(wrongArgs);
		}
		return parameters;
	}

	@Override
	JarRunRequestBody getDefaultJarRequestBody() {
		return new JarRunRequestBody();
	}

	@Override
	JarRunRequestBody getJarRequestBody(ProgramArgsParType programArgsParType) {
		return new JarRunRequestBody(
			ParameterProgram.class.getCanonicalName(),
			getProgramArgsString(programArgsParType),
			getProgramArgsList(programArgsParType),
			PARALLELISM,
			null,
			ALLOW_NON_RESTORED_STATE_QUERY,
			RESTORE_PATH
		);
	}

	@Override
	JarRunRequestBody getJarRequestBodyWithJobId(JobID jobId) {
		return new JarRunRequestBody(null, null, null, null, jobId, null, null);
	}

	@Override
	void handleRequest(HandlerRequest<JarRunRequestBody, JarRunMessageParameters> request)
		throws Exception {
		handler.handleRequest(request, restfulGateway).get();
	}

	@Override
	JobGraph validateDefaultGraph() {
		JobGraph jobGraph = super.validateDefaultGraph();
		final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
		Assert.assertFalse(savepointRestoreSettings.allowNonRestoredState());
		Assert.assertNull(savepointRestoreSettings.getRestorePath());
		return jobGraph;
	}

	@Override
	JobGraph validateGraph() {
		JobGraph jobGraph = super.validateGraph();
		final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
		Assert.assertTrue(savepointRestoreSettings.allowNonRestoredState());
		Assert.assertEquals(RESTORE_PATH, savepointRestoreSettings.getRestorePath());
		return jobGraph;
	}
}
