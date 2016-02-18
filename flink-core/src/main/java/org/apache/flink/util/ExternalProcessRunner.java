/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for running a class in an external process. This will try to find the java
 * executable in common places and will use the classpath of the current process as the classpath
 * of the new process.
 *
 * <p>Attention: The entry point class must be in the classpath of the currently running process,
 * otherwise the newly spawned process will not find it and fail.
 */
@Internal
public class ExternalProcessRunner {
	private final Process process;

	private final Thread pipeForwarder;

	final StringWriter errorOutput = new StringWriter();

	/**
	 * Creates a new {@code ProcessRunner} that runs the given class with the given parameters.
	 * The class must have a "main" method.
	 */
	public ExternalProcessRunner(String entryPointClassName, String[] parameters) throws IOException {
		String javaCommand = getJavaCommandPath();

		List<String> commandList = new ArrayList<>();

		commandList.add(javaCommand);
		commandList.add("-classpath");
		commandList.add(getCurrentClasspath());
		commandList.add(entryPointClassName);

		Collections.addAll(commandList, parameters);

		process = new ProcessBuilder(commandList).start();

		pipeForwarder = new PipeForwarder(process.getErrorStream(), errorOutput);
	}

	/**
	 * Get the stderr stream of the process.
	 */
	public StringWriter getErrorOutput() {
		return errorOutput;
	}

	/**
	 * Start the external process, wait for it to finish and return the exit code of that process.
	 *
	 * <p>If this method is interrupted it will destroy the external process and forward the
	 * {@code InterruptedException}.
	 */
	public int run() throws Exception {
		try {
			int returnCode = process.waitFor();

			// wait to finish copying standard error stream
			pipeForwarder.join();

			if (returnCode != 0) {

				final String errorOutput = getErrorOutput().toString();
				if (!errorOutput.isEmpty()) {
					throw new RuntimeException(errorOutput);
				}

			}
			return returnCode;
		} catch (InterruptedException e) {
			try {
				Class<?> processClass = process.getClass();
				Method destroyForcibly = processClass.getMethod("destroyForcibly");
				destroyForcibly.setAccessible(true);
				destroyForcibly.invoke(process);
			} catch (NoSuchMethodException ex) {
				// we don't have destroyForcibly
				process.destroy();
			}
			throw new InterruptedException("Interrupted while waiting for external process.");
		}
	}

	/**
	 * Tries to get the java executable command with which the current JVM was started.
	 * Returns null, if the command could not be found.
	 *
	 * @return The java executable command.
	 */
	public static String getJavaCommandPath() {

		try {
			ProcessBuilder bld = new ProcessBuilder("java", "-version");
			Process process = bld.start();
			if (process.waitFor() == 0) {
				return "java";
			}
		}
		catch (Throwable t) {
			// ignore and try the second path
		}

		try {
			ProcessBuilder bld = new ProcessBuilder("java.exe", "-version");
			Process process = bld.start();
			if (process.waitFor() == 0) {
				return "java.exe";
			}
		}
		catch (Throwable t) {
			// ignore and try the second path
		}

		File javaHome = new File(System.getProperty("java.home"));

		String path1 = new File(javaHome, "java").getAbsolutePath();
		String path2 = new File(new File(javaHome, "bin"), "java").getAbsolutePath();

		try {
			ProcessBuilder bld = new ProcessBuilder(path1, "-version");
			Process process = bld.start();
			if (process.waitFor() == 0) {
				return path1;
			}
		}
		catch (Throwable t) {
			// ignore and try the second path
		}

		try {
			ProcessBuilder bld = new ProcessBuilder(path2, "-version");
			Process process = bld.start();
			if (process.waitFor() == 0) {
				return path2;
			}
		}
		catch (Throwable tt) {
			// no luck
		}

		String path3 = new File(javaHome, "java.exe").getAbsolutePath();
		String path4 = new File(new File(javaHome, "bin"), "java.exe").getAbsolutePath();

		try {
			ProcessBuilder bld = new ProcessBuilder(path3, "-version");
			Process process = bld.start();
			if (process.waitFor() == 0) {
				return path3;
			}
		}
		catch (Throwable t) {
			// ignore and try the second path
		}

		try {
			ProcessBuilder bld = new ProcessBuilder(path4, "-version");
			Process process = bld.start();
			if (process.waitFor() == 0) {
				return path4;
			}
		}
		catch (Throwable tt) {
			// no luck
		}
		return null;
	}

	/**
	 * Gets the classpath with which the current JVM was started.
	 *
	 * @return The classpath with which the current JVM was started.
	 */
	public static String getCurrentClasspath() {
		RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
		return bean.getClassPath();
	}

	/**
	 * Utility class to read the output of a process stream and forward it into a StringWriter.
	 */
	public static class PipeForwarder extends Thread {

		private final StringWriter target;
		private final InputStream source;

		public PipeForwarder(InputStream source, StringWriter target) {
			super("Pipe Forwarder");
			setDaemon(true);

			this.source = source;
			this.target = target;

			start();
		}

		@Override
		public void run() {
			try {
				int next;
				while ((next = source.read()) != -1) {
					target.write(next);
				}
			}
			catch (IOException e) {
				// terminate
			}
		}
	}
}
