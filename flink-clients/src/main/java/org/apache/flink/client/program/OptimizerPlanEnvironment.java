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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.FlinkPlan;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * An {@link ExecutionEnvironment} that never executes a job but only creates the optimized plan.
 */
public class OptimizerPlanEnvironment extends ExecutionEnvironment {

	private final Optimizer compiler;

	private FlinkPlan optimizerPlan;

	public OptimizerPlanEnvironment(Optimizer compiler) {
		this.compiler = compiler;
	}

	// ------------------------------------------------------------------------
	//  Execution Environment methods
	// ------------------------------------------------------------------------

	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		Plan plan = createProgramPlan(jobName);
		this.optimizerPlan = compiler.compile(plan);

		// do not go on with anything now!
		throw new ProgramAbortException();
	}

	@Override
	public String getExecutionPlan() throws Exception {
		Plan plan = createProgramPlan(null, false);
		this.optimizerPlan = compiler.compile(plan);

		// do not go on with anything now!
		throw new ProgramAbortException();
	}

	@Override
	public void startNewSession() {
		// do nothing
	}

	public FlinkPlan getOptimizedPlan(PackagedProgram prog) throws ProgramInvocationException {

		// temporarily write syserr and sysout to a byte array.
		PrintStream originalOut = System.out;
		PrintStream originalErr = System.err;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		System.setOut(new PrintStream(baos));
		ByteArrayOutputStream baes = new ByteArrayOutputStream();
		System.setErr(new PrintStream(baes));

		setAsContext();
		try {
			prog.invokeInteractiveModeForExecution();
		}
		catch (ProgramInvocationException e) {
			throw e;
		}
		catch (Throwable t) {
			// the invocation gets aborted with the preview plan
			if (optimizerPlan != null) {
				return optimizerPlan;
			} else {
				throw new ProgramInvocationException("The program caused an error: ", t);
			}
		}
		finally {
			unsetAsContext();
			System.setOut(originalOut);
			System.setErr(originalErr);
		}

		String stdout = baos.toString();
		String stderr = baes.toString();

		throw new ProgramInvocationException(
				"The program plan could not be fetched - the program aborted pre-maturely."
						+ "\n\nSystem.err: " + (stderr.length() == 0 ? "(none)" : stderr)
						+ "\n\nSystem.out: " + (stdout.length() == 0 ? "(none)" : stdout));
	}
	// ------------------------------------------------------------------------

	private void setAsContext() {
		ExecutionEnvironmentFactory factory = new ExecutionEnvironmentFactory() {

			@Override
			public ExecutionEnvironment createExecutionEnvironment() {
				return OptimizerPlanEnvironment.this;
			}
		};
		initializeContextEnvironment(factory);
	}

	private void unsetAsContext() {
		resetContextEnvironment();
	}

	// ------------------------------------------------------------------------

	public void setPlan(FlinkPlan plan){
		this.optimizerPlan = plan;
	}

	/**
	 * A special exception used to abort programs when the caller is only interested in the
	 * program plan, rather than in the full execution.
	 */
	public static final class ProgramAbortException extends Error {
		private static final long serialVersionUID = 1L;
	}
}
