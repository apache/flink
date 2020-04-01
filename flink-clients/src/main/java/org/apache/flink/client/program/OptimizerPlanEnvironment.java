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

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * An {@link ExecutionEnvironment} that never executes a job but only extracts the {@link
 * org.apache.flink.api.dag.Pipeline}.
 */
public class OptimizerPlanEnvironment extends ExecutionEnvironment {

	private Pipeline pipeline;

	public OptimizerPlanEnvironment(Configuration configuration) {
		super(configuration);
	}

	@Override
	public JobClient executeAsync(String jobName) throws Exception {
		this.pipeline = createProgramPlan();

		// do not go on with anything now!
		throw new ProgramAbortException();
	}

	/**
	 * Retrieves the JobGraph from a PackagedProgram.
	 * @param prog The program to run
	 * @param suppressOutput Whether to suppress stdout/stderr. Output is always printed on errors.
	 * @return The Flink batch or streaming plan
	 * @throws ProgramInvocationException in case of errors.
	 */
	public Pipeline getPipeline(PackagedProgram prog, boolean suppressOutput) throws ProgramInvocationException {

		final PrintStream originalOut = System.out;
		final PrintStream originalErr = System.err;
		final ByteArrayOutputStream stdOutBuffer;
		final ByteArrayOutputStream stdErrBuffer;

		if (suppressOutput) {
			// temporarily write syserr and sysout to a byte array.
			stdOutBuffer = new ByteArrayOutputStream();
			System.setOut(new PrintStream(stdOutBuffer));
			stdErrBuffer = new ByteArrayOutputStream();
			System.setErr(new PrintStream(stdErrBuffer));
		} else {
			stdOutBuffer = null;
			stdErrBuffer = null;
		}

		setAsContext();
		try {
			prog.invokeInteractiveModeForExecution();
		}
		catch (ProgramInvocationException e) {
			throw e;
		}
		catch (Throwable t) {
			// the invocation gets aborted with the preview plan
			if (pipeline != null) {
				return pipeline;
			} else {
				throw generateException(prog, "The program caused an error: ", t, stdOutBuffer, stdErrBuffer);
			}
		}
		finally {
			unsetAsContext();
			if (suppressOutput) {
				System.setOut(originalOut);
				System.setErr(originalErr);
			}
		}

		throw generateException(prog, "The program plan could not be fetched - the program aborted pre-maturely.", stdOutBuffer, stdErrBuffer);
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

	public void setPipeline(Pipeline pipeline){
		this.pipeline = pipeline;
	}

	private static ProgramInvocationException generateException(
			PackagedProgram prog,
			String msg,
			@Nullable ByteArrayOutputStream stdout,
			@Nullable ByteArrayOutputStream stderr) {
		return generateException(prog, msg, null, stdout, stderr);
	}

	private static ProgramInvocationException generateException(
			PackagedProgram prog,
			String msg,
			@Nullable Throwable cause,
			@Nullable ByteArrayOutputStream stdoutBuffer,
			@Nullable ByteArrayOutputStream stderrBuffer) {
		Preconditions.checkState((stdoutBuffer != null) == (stderrBuffer != null),
				"Stderr/Stdout should either both be set or both be null.");
		String stdout = "";
		String stderr = "";
		if (stdoutBuffer != null) {
			stdout = stdoutBuffer.toString();
			stderr = stderrBuffer.toString();
		}
		return new ProgramInvocationException(
			String.format("%s\n\nClasspath: %s\n\nSystem.out: %s\n\nSystem.err: %s",
				msg,
				prog.getJobJarAndDependencies(),
				stdout.length() == 0 ? "(none)" : stdout,
				stderr.length() == 0 ? "(none)" : stderr),
			cause);
	}

	/**
	 * A special exception used to abort programs when the caller is only interested in the
	 * program plan, rather than in the full execution.
	 */
	public static final class ProgramAbortException extends Error {
		private static final long serialVersionUID = 1L;
	}
}
