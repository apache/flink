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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.webmonitor.testutils.ParameterProgram;

import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for the parameter handling of the {@link JarPlanHandler}.
 */
public class JarPlanHandlerParameterTest extends JarHandlerParameterTest<JarPlanRequestBody, JarPlanMessageParameters> {
	private static JarPlanHandler handler;

	@BeforeClass
	public static void setup() throws Exception {
		init();
		handler = new JarPlanHandler(
			localAddressFuture,
			gatewayRetriever,
			timeout,
			responseHeaders,
			JarPlanHeaders.getInstance(),
			jarDir,
			new Configuration(),
			executor,
			jobGraph -> {
				LAST_SUBMITTED_JOB_GRAPH_REFERENCE.set(jobGraph);
				return new JobPlanInfo(JsonPlanGenerator.generatePlan(jobGraph));
			});
	}

	@Override
	JarPlanMessageParameters getUnresolvedJarMessageParameters() {
		return handler.getMessageHeaders().getUnresolvedMessageParameters();
	}

	@Override
	JarPlanMessageParameters getJarMessageParameters(ProgramArgsParType programArgsParType) {
		final JarPlanMessageParameters parameters = getUnresolvedJarMessageParameters();
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
	JarPlanMessageParameters getWrongJarMessageParameters(ProgramArgsParType programArgsParType) {
		List<String> wrongArgs = Arrays.stream(PROG_ARGS).map(a -> a + "wrong").collect(Collectors.toList());
		String argsWrongStr = String.join(" ", wrongArgs);
		final JarPlanMessageParameters parameters = getUnresolvedJarMessageParameters();
		parameters.entryClassQueryParameter.resolve(Collections.singletonList("please.dont.run.me"));
		parameters.parallelismQueryParameter.resolve(Collections.singletonList(64));
		if (programArgsParType == ProgramArgsParType.String || programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgsQueryParameter.resolve(Collections.singletonList(argsWrongStr));
		}
		if (programArgsParType == ProgramArgsParType.List ||
			programArgsParType == ProgramArgsParType.Both) {
			parameters.programArgQueryParameter.resolve(wrongArgs);
		}
		return parameters;
	}

	@Override
	JarPlanRequestBody getDefaultJarRequestBody() {
		return new JarPlanRequestBody();
	}

	@Override
	JarPlanRequestBody getJarRequestBody(ProgramArgsParType programArgsParType) {
		return new JarPlanRequestBody(
			ParameterProgram.class.getCanonicalName(),
			getProgramArgsString(programArgsParType),
			getProgramArgsList(programArgsParType),
			PARALLELISM);
	}

	@Override
	void handleRequest(HandlerRequest<JarPlanRequestBody, JarPlanMessageParameters> request)
		throws Exception {
		handler.handleRequest(request, restfulGateway).get();
	}
}

