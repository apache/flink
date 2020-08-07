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
import org.apache.flink.client.deployment.application.DetachedApplicationRunner;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the parameter handling of the {@link JarRunHandler}.
 */
public class JarRunHandlerParameterTest extends JarHandlerParameterTest<JarRunRequestBody, JarRunMessageParameters> {
	private static final boolean ALLOW_NON_RESTORED_STATE_QUERY = true;
	private static final String RESTORE_PATH = "/foo/bar";

	private static JarRunHandler handler;

	private static Path jarWithEagerSink;

	@BeforeClass
	public static void setup() throws Exception {
		init();
		final GatewayRetriever<TestingDispatcherGateway> gatewayRetriever = () -> CompletableFuture.completedFuture(restfulGateway);
		final Time timeout = Time.seconds(10);
		final Map<String, String> responseHeaders = Collections.emptyMap();
		final Executor executor = TestingUtils.defaultExecutor();

		final Path jarLocation = Paths.get(System.getProperty("targetDir"));
		final String parameterProgramWithEagerSink = "parameter-program-with-eager-sink.jar";
		jarWithEagerSink = Files.copy(
				jarLocation.resolve(parameterProgramWithEagerSink),
				jarDir.resolve("program-with-eager-sink.jar"));

		handler = new JarRunHandler(
			gatewayRetriever,
			timeout,
			responseHeaders,
			JarRunHeaders.getInstance(),
			jarDir,
			new Configuration(),
			executor,
			ConfigurationVerifyingDetachedApplicationRunner::new);
	}

	private static class ConfigurationVerifyingDetachedApplicationRunner extends DetachedApplicationRunner {

		public ConfigurationVerifyingDetachedApplicationRunner() {
			super(true);
		}

		@Override
		public List<JobID> run(DispatcherGateway dispatcherGateway, PackagedProgram program, Configuration configuration) {
			assertFalse(configuration.get(DeploymentOptions.ATTACHED));
			assertEquals(EmbeddedExecutor.NAME, configuration.get(DeploymentOptions.TARGET));
			return super.run(dispatcherGateway, program, configuration);
		}
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

	@Test
	public void testRestHandlerExceptionThrownWithEagerSinks() throws Exception {
		final HandlerRequest<JarRunRequestBody, JarRunMessageParameters> request = createRequest(
				getDefaultJarRequestBody(),
				getUnresolvedJarMessageParameters(),
				getUnresolvedJarMessageParameters(),
				jarWithEagerSink
		);

		try {
			handler.handleRequest(request, restfulGateway).get();
		} catch (final ExecutionException e) {
			final Throwable throwable =	 ExceptionUtils.stripCompletionException(e.getCause());
			assertThat(throwable, instanceOf(RestHandlerException.class));

			final RestHandlerException restHandlerException = (RestHandlerException) throwable;
			assertThat(restHandlerException.getHttpResponseStatus(), equalTo(HttpResponseStatus.BAD_REQUEST));

			final Optional<ProgramInvocationException> invocationException =
					ExceptionUtils.findThrowable(restHandlerException, ProgramInvocationException.class);

			if (!invocationException.isPresent()) {
				fail();
			}

			final String exceptionMsg = invocationException.get().getMessage();
			assertThat(exceptionMsg, containsString("Job was submitted in detached mode."));
			return;
		}
		fail("The test should have failed.");
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
		assertFalse(savepointRestoreSettings.allowNonRestoredState());
		Assert.assertNull(savepointRestoreSettings.getRestorePath());
		return jobGraph;
	}

	@Override
	JobGraph validateGraph() {
		JobGraph jobGraph = super.validateGraph();
		final SavepointRestoreSettings savepointRestoreSettings = jobGraph.getSavepointRestoreSettings();
		Assert.assertTrue(savepointRestoreSettings.allowNonRestoredState());
		assertEquals(RESTORE_PATH, savepointRestoreSettings.getRestorePath());
		return jobGraph;
	}
}
