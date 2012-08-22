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
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.sopremo.SopremoTest;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.execution.ExecutionRequest;
import eu.stratosphere.sopremo.execution.ExecutionResponse;
import eu.stratosphere.sopremo.execution.ExecutionResponse.ExecutionState;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.OrExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * @author Arvid Heise
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ SopremoServer.class, SopremoExecutionThread.class })
public class SopremoServerTest {
	private SopremoServer server;

	private SopremoPlan plan = createPlan();

	private ExecutionRequest request;

	private SopremoExecutionThread mockThread;

	private SopremoJobInfo mockInfo;

	@Before
	public void setup() throws Exception {
		this.server = new SopremoServer();
		this.request = new ExecutionRequest(this.plan);

		this.mockThread = mock(SopremoExecutionThread.class);
		whenNew(SopremoExecutionThread.class).withArguments(any(), any()).thenReturn(this.mockThread);
		this.mockInfo = mock(SopremoJobInfo.class);
		whenNew(SopremoJobInfo.class).withArguments(any(), any(), any()).thenReturn(this.mockInfo);
	}

	@After
	public void teardown() {
		this.server.close();
	}

	@Test
	public void testJobEnqueueing() {
		when(this.mockInfo.getStatus()).thenReturn(ExecutionState.ENQUEUED);

		ExecutionResponse response = this.server.execute(this.request);
		Assert.assertSame(ExecutionState.ENQUEUED, response.getState());
	}

	@Test
	public void testSuccessfulExecution() {
		when(this.mockInfo.getStatus()).thenReturn(ExecutionState.ENQUEUED).thenReturn(ExecutionState.RUNNING).thenReturn(
			ExecutionState.FINISHED);

		ExecutionResponse response = this.server.execute(this.request);
		response = waitForStateToFinish(response, ExecutionState.ENQUEUED);
		response = waitForStateToFinish(response, ExecutionState.RUNNING);
		Assert.assertSame(ExecutionState.FINISHED, response.getState());
	}

	@Test
	public void testFailBeforeRunning() {
		when(this.mockInfo.getStatus()).thenReturn(ExecutionState.ENQUEUED).thenReturn(ExecutionState.ERROR);

		ExecutionResponse response = this.server.execute(this.request);
		response = waitForStateToFinish(response, ExecutionState.ENQUEUED);
		response = waitForStateToFinish(response, ExecutionState.RUNNING);
		Assert.assertSame(ExecutionState.ERROR, response.getState());
	}

	@Test
	public void testFailDuringRun() {
		when(this.mockInfo.getStatus()).thenReturn(ExecutionState.ENQUEUED, ExecutionState.RUNNING,
			ExecutionState.ERROR);

		ExecutionResponse response = this.server.execute(this.request);
		response = waitForStateToFinish(response, ExecutionState.ENQUEUED);
		response = waitForStateToFinish(response, ExecutionState.RUNNING);
		Assert.assertSame(ExecutionState.ERROR, response.getState());
	}

	private ExecutionResponse waitForStateToFinish(ExecutionResponse response, ExecutionState status) {
		return SopremoTestServer.waitForStateToFinish(this.server, response, status);
	}

	static SopremoPlan createPlan() {
		final SopremoPlan plan = new SopremoPlan();
		final Source input = new Source(SopremoTest.createTemporaryFile("input"));
		final Selection selection = new Selection().
			withCondition(
				new OrExpression(
					new UnaryExpression(JsonUtil.createPath("0", "mgr")),
					new ComparativeExpression(JsonUtil.createPath("0", "income"), BinaryOperator.GREATER,
						new ConstantExpression(30000)))).
			withInputs(input);
		final Sink output = new Sink(SopremoTest.createTemporaryFile("output")).withInputs(selection);
		plan.setSinks(output);
		return plan;
	}
}
