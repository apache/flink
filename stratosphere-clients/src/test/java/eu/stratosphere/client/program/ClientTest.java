/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.client.program;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.LocalEnvironment;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobSubmissionResult;
import eu.stratosphere.nephele.jobgraph.JobGraph;



/**
 * Simple and maybe stupid test to check the {@link Client} class.
 * However, the use of mocks can be copied copied easily from this example.
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Client.class)
public class ClientTest {

	@Mock
	Configuration configMock;

	@Mock
	PackagedProgram program;
	@Mock
	JobWithJars planWithJarsMock;
	@Mock
	Plan planMock;
	
	@Mock
	PactCompiler compilerMock;
	@Mock
	OptimizedPlan optimizedPlanMock;
	
	@Mock
	NepheleJobGraphGenerator generatorMock;
	@Mock
	JobGraph jobGraphMock;

	@Mock
	JobClient jobClientMock;
	@Mock
	JobSubmissionResult jobSubmissionResultMock;
	
	@Before
	public void setUp() throws Exception
	{
		initMocks(this);
		
		when(configMock.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)).thenReturn("localhost");
		when(configMock.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)).thenReturn(6123);
		
		when(planMock.getJobName()).thenReturn("MockPlan");
//		when(mockJarFile.getAbsolutePath()).thenReturn("mockFilePath");
		
		when(program.getPlanWithJars()).thenReturn(planWithJarsMock);
		when(planWithJarsMock.getPlan()).thenReturn(planMock);
		
		whenNew(PactCompiler.class).withArguments(any(DataStatistics.class), any(CostEstimator.class), any(InetSocketAddress.class)).thenReturn(this.compilerMock);
		when(compilerMock.compile(planMock)).thenReturn(optimizedPlanMock);
		
		whenNew(NepheleJobGraphGenerator.class).withNoArguments().thenReturn(generatorMock);
		when(generatorMock.compileJobGraph(optimizedPlanMock)).thenReturn(jobGraphMock);
		
		whenNew(JobClient.class).withArguments(any(JobGraph.class), any(Configuration.class)).thenReturn(this.jobClientMock);
		
		when(this.jobClientMock.submitJob()).thenReturn(jobSubmissionResultMock);
	}
	
	@Test
	public void shouldSubmitToJobClient() throws ProgramInvocationException, IOException
	{
		when(jobSubmissionResultMock.getReturnCode()).thenReturn(ReturnCode.SUCCESS);
		
		Client out = new Client(configMock);
		out.run(program.getPlanWithJars(), -1, false);
		program.deleteExtractedLibraries();
		
		verify(this.compilerMock, times(1)).compile(planMock);
		verify(this.generatorMock, times(1)).compileJobGraph(optimizedPlanMock);
		verify(this.jobClientMock, times(1)).submitJob();
	}
	
	/**
	 * @throws Exception
	 */
	@Test(expected=ProgramInvocationException.class)
	public void shouldThrowException() throws Exception
	{
		when(jobSubmissionResultMock.getReturnCode()).thenReturn(ReturnCode.ERROR);
		
		Client out = new Client(configMock);
		out.run(program.getPlanWithJars(), -1, false);
		program.deleteExtractedLibraries();
		
		verify(this.jobClientMock).submitJob();
	}
	

	@Test(expected=InvalidProgramException.class)
	public void tryLocalExecution() throws Exception {
		new Client(configMock);
		LocalExecutor.execute(planMock);
	}
	
	@Test(expected=InvalidProgramException.class)
	public void tryLocalEnvironmentExecution() throws Exception {
		new Client(configMock);
		new LocalEnvironment();
	}
}
