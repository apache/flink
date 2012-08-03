/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.server;

import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionRequest.ExecutionMode;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;

/**
 * @author Arvid Heise
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ SopremoExecutionThread.class, JobClient.class })
public class SopremoExecuctionThreadTest {

	private SopremoJobInfo jobInfo;

	private JobClient mockClient;

	private SopremoExecutionThread thread;

	@Before
	public void setup() throws Exception {
		this.jobInfo = new SopremoJobInfo(new ExecutionRequest(SopremoServerTest.createPlan()), new Configuration());
		this.thread = new SopremoExecutionThread(this.jobInfo, new InetSocketAddress(0)) {
			/*
			 * (non-Javadoc)
			 * @see
			 * eu.stratosphere.sopremo.server.SopremoExecutionThread#getJobGraph(eu.stratosphere.pact.common.plan.Plan)
			 */
			@Override
			JobGraph getJobGraph(Plan pactPlan) {
				return new JobGraph();
			}
		};

		this.mockClient = mock(JobClient.class);
		whenNew(JobClient.class).withArguments(any(), any(), any()).thenReturn(this.mockClient);
	}

	@Test
	public void testSuccessfulExecution() throws Exception {
		when(this.mockClient.submitJobAndWait()).thenReturn(1L);

		this.thread.run();
		Assert.assertSame(this.jobInfo.getDetail(), ExecutionState.FINISHED, this.jobInfo.getStatus());
		Assert.assertSame("", this.jobInfo.getDetail());
	}
	
	@Test	
	public void testSuccessfulExecutionWithStatistics() throws Exception {
		this.jobInfo.getInitialRequest().setMode(ExecutionMode.RUN_WITH_STATISTICS);
		when(this.mockClient.submitJobAndWait()).thenReturn(1L);

		this.thread.run();
		Assert.assertSame(this.jobInfo.getDetail(), ExecutionState.FINISHED, this.jobInfo.getStatus());
		Assert.assertNotSame("", this.jobInfo.getDetail());
	}

	@Test
	public void testFailBeforeRunning() throws Exception {
		whenNew(JobClient.class).withArguments(any(), any(), any()).thenThrow(new IOException("io"));

		this.thread.run();
		Assert.assertSame(ExecutionState.ERROR, this.jobInfo.getStatus());
		Assert.assertNotSame("", this.jobInfo.getDetail());
	}

	@Test
	public void testFailDuringRun() throws Exception {
		when(this.mockClient.submitJobAndWait()).thenThrow(new JobExecutionException("jee", false));

		this.thread.run();
		Assert.assertSame(ExecutionState.ERROR, this.jobInfo.getStatus());
		Assert.assertNotSame("", this.jobInfo.getDetail());
	}

}
