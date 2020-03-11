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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility class for {@link PackagedProgram} related operations.
 */
public enum PackagedProgramUtils {
	;

	private static final String PYTHON_DRIVER_CLASS_NAME = "org.apache.flink.client.python.PythonDriver";

	private static final String PYTHON_GATEWAY_CLASS_NAME = "org.apache.flink.client.python.PythonGatewayServer";

	/**
	 * Creates a {@link JobGraph} with a specified {@link JobID}
	 * from the given {@link PackagedProgram}.
	 *
	 * @param packagedProgram    to extract the JobGraph from
	 * @param configuration      to use for the optimizer and job graph generator
	 * @param defaultParallelism for the JobGraph
	 * @param jobID              the pre-generated job id
	 * @return JobGraph extracted from the PackagedProgram
	 * @throws ProgramInvocationException if the JobGraph generation failed
	 */
	public static JobGraph createJobGraph(
			PackagedProgram packagedProgram,
			Configuration configuration,
			int defaultParallelism,
			@Nullable JobID jobID,
			boolean suppressOutput) throws ProgramInvocationException {
		final Pipeline pipeline = getPipelineFromProgram(packagedProgram, defaultParallelism, suppressOutput);
		final JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, defaultParallelism);

		if (jobID != null) {
			jobGraph.setJobID(jobID);
		}
		jobGraph.addJars(packagedProgram.getJobJarAndDependencies());
		jobGraph.setClasspaths(packagedProgram.getClasspaths());
		jobGraph.setSavepointRestoreSettings(packagedProgram.getSavepointSettings());

		return jobGraph;
	}

	/**
	 * Creates a {@link JobGraph} with a random {@link JobID}
	 * from the given {@link PackagedProgram}.
	 *
	 * @param packagedProgram    to extract the JobGraph from
	 * @param configuration      to use for the optimizer and job graph generator
	 * @param defaultParallelism for the JobGraph
	 * @param suppressOutput     Whether to suppress stdout/stderr during interactive JobGraph creation.
	 * @return JobGraph extracted from the PackagedProgram
	 * @throws ProgramInvocationException if the JobGraph generation failed
	 */
	public static JobGraph createJobGraph(
			PackagedProgram packagedProgram,
			Configuration configuration,
			int defaultParallelism,
			boolean suppressOutput) throws ProgramInvocationException {
		return createJobGraph(packagedProgram, configuration, defaultParallelism, null, suppressOutput);
	}

	public static Pipeline getPipelineFromProgram(
			PackagedProgram program,
			int parallelism,
			boolean suppressOutput) throws CompilerException, ProgramInvocationException {
		final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

		Thread.currentThread().setContextClassLoader(program.getUserCodeClassLoader());

		final PrintStream originalOut = System.out;
		final PrintStream originalErr = System.err;
		final ByteArrayOutputStream stdOutBuffer;
		final ByteArrayOutputStream stdErrBuffer;

		if (suppressOutput) {
			// temporarily write STDERR and STDOUT to a byte array.
			stdOutBuffer = new ByteArrayOutputStream();
			System.setOut(new PrintStream(stdOutBuffer));
			stdErrBuffer = new ByteArrayOutputStream();
			System.setErr(new PrintStream(stdErrBuffer));
		} else {
			stdOutBuffer = null;
			stdErrBuffer = null;
		}

		// temporary hack to support the optimizer plan preview
		OptimizerPlanEnvironment benv = new OptimizerPlanEnvironment(parallelism);
		benv.setAsContext();
		StreamPlanEnvironment senv = new StreamPlanEnvironment(parallelism);
		senv.setAsContext();

		try {
			program.invokeInteractiveModeForExecution();
		} catch (Throwable t) {
			if (benv.getPipeline() != null) {
				return benv.getPipeline();
			}

			if (senv.getPipeline() != null) {
				return senv.getPipeline();
			}

			if (t instanceof ProgramInvocationException) {
				throw t;
			}

			throw generateException(
				program,
				"The program caused an error: ",
				t,
				stdOutBuffer,
				stdErrBuffer);
		} finally {
			benv.unsetAsContext();
			senv.unsetAsContext();
			if (suppressOutput) {
				System.setOut(originalOut);
				System.setErr(originalErr);
			}
			Thread.currentThread().setContextClassLoader(contextClassLoader);
		}

		throw generateException(
			program,
			"The program plan could not be fetched - the program aborted pre-maturely.",
			null,
			stdOutBuffer,
			stdErrBuffer);
	}

	public static Boolean isPython(String entryPointClassName) {
		return (entryPointClassName != null) &&
			(entryPointClassName.equals(PYTHON_DRIVER_CLASS_NAME) || entryPointClassName.equals(PYTHON_GATEWAY_CLASS_NAME));
	}

	private static ProgramInvocationException generateException(
			PackagedProgram program,
			String msg,
			@Nullable Throwable cause,
			@Nullable ByteArrayOutputStream stdoutBuffer,
			@Nullable ByteArrayOutputStream stderrBuffer) {
		checkState(
			(stdoutBuffer != null) == (stderrBuffer != null),
			"Stderr/Stdout should either both be set or both be null.");

		final String stdout = (stdoutBuffer != null) ? stdoutBuffer.toString() : "";
		final String stderr = (stderrBuffer != null) ? stderrBuffer.toString() : "";

		return new ProgramInvocationException(
			String.format("%s\n\nClasspath: %s\n\nSystem.out: %s\n\nSystem.err: %s",
				msg,
				program.getJobJarAndDependencies(),
				stdout.length() == 0 ? "(none)" : stdout,
				stderr.length() == 0 ? "(none)" : stderr),
			cause);
	}
}
