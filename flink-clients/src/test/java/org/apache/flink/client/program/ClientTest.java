/**
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

package org.apache.flink.client.program;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.Plan;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.CostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.client.AbstractJobResult.ReturnCode;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.whenNew;


/**
 * Simple and maybe stupid test to check the {@link Client} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Client.class)
public class ClientTest {

	@Mock Configuration configMock;
	@Mock PackagedProgram program;
	@Mock JobWithJars planWithJarsMock;
	@Mock Plan planMock;
	@Mock PactCompiler compilerMock;
	@Mock OptimizedPlan optimizedPlanMock;
	@Mock NepheleJobGraphGenerator generatorMock;
	@Mock JobGraph jobGraphMock;
	@Mock JobClient jobClientMock;
	@Mock JobSubmissionResult jobSubmissionResultMock;

	@Before
	public void setUp() throws Exception {
		initMocks(this);

		when(configMock.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)).thenReturn("localhost");
		when(configMock.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)).thenReturn(6123);

		when(planMock.getJobName()).thenReturn("MockPlan");
//		when(mockJarFile.getAbsolutePath()).thenReturn("mockFilePath");

		when(program.getPlanWithJars()).thenReturn(planWithJarsMock);
		when(planWithJarsMock.getPlan()).thenReturn(planMock);

		whenNew(PactCompiler.class).withArguments(any(DataStatistics.class), any(CostEstimator.class)).thenReturn(this.compilerMock);
		when(compilerMock.compile(planMock)).thenReturn(optimizedPlanMock);

		whenNew(NepheleJobGraphGenerator.class).withNoArguments().thenReturn(generatorMock);
		when(generatorMock.compileJobGraph(optimizedPlanMock)).thenReturn(jobGraphMock);

		whenNew(JobClient.class).withArguments(any(JobGraph.class), any(Configuration.class), any(ClassLoader.class)).thenReturn(this.jobClientMock);

		when(this.jobClientMock.submitJob()).thenReturn(jobSubmissionResultMock);
	}

	@Test
	public void shouldSubmitToJobClient() throws ProgramInvocationException, IOException {
		when(jobSubmissionResultMock.getReturnCode()).thenReturn(ReturnCode.SUCCESS);

		Client out = new Client(configMock, getClass().getClassLoader());
		out.run(program.getPlanWithJars(), -1, false);
		program.deleteExtractedLibraries();

		verify(this.compilerMock, times(1)).compile(planMock);
		verify(this.generatorMock, times(1)).compileJobGraph(optimizedPlanMock);
		verify(this.jobClientMock, times(1)).submitJob();
	}

	@Test(expected = ProgramInvocationException.class)
	public void shouldThrowException() throws Exception {
		when(jobSubmissionResultMock.getReturnCode()).thenReturn(ReturnCode.ERROR);

		Client out = new Client(configMock, getClass().getClassLoader());
		out.run(program.getPlanWithJars(), -1, false);
		program.deleteExtractedLibraries();

		verify(this.jobClientMock).submitJob();
	}

	@Test(expected = InvalidProgramException.class)
	public void tryLocalExecution() throws Exception {
		PackagedProgram packagedProgramMock = mock(PackagedProgram.class);

		when(packagedProgramMock.isUsingInteractiveMode()).thenReturn(true);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ExecutionEnvironment.createLocalEnvironment();
				return null;
			}
		}).when(packagedProgramMock).invokeInteractiveModeForExecution();

		new Client(configMock, getClass().getClassLoader()).run(packagedProgramMock, 1, true);
	}
}
