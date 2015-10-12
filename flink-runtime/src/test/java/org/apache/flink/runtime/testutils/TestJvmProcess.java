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

package org.apache.flink.runtime.testutils;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.flink.runtime.testutils.CommonTestUtils.createTemporaryLog4JProperties;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getCurrentClasspath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getJavaCommandPath;
import static org.junit.Assert.fail;

/**
 * A {@link Process} running a separate JVM.
 */
public abstract class TestJvmProcess {

	private static final Logger LOG = LoggerFactory.getLogger(TestJvmProcess.class);

	/** Lock to guard {@link #createAndStart()} and {@link #destroy()} calls. */
	private final Object createDestroyLock = new Object();

	/** The java command path */
	private final String javaCommandPath;

	/** The log4j configuration path. */
	private final String log4jConfigFilePath;

	/** Shutdown hook for resource cleanup */
	private final Thread shutdownHook;

	/** JVM process memory (set for both '-Xms' and '-Xmx'). */
	private int jvmMemoryInMb = 80;

	/** The JVM process */
	private Process process;

	/** Writer for the process output */
	private volatile StringWriter processOutput;

	public TestJvmProcess() throws Exception {
		this(getJavaCommandPath(), createTemporaryLog4JProperties().getPath());
	}

	public TestJvmProcess(String javaCommandPath, String log4jConfigFilePath) {
		this.javaCommandPath = checkNotNull(javaCommandPath, "Java command path");
		this.log4jConfigFilePath = checkNotNull(log4jConfigFilePath, "log4j config file path");

		this.shutdownHook = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					destroy();
				}
				catch (Throwable t) {
					LOG.error("Error during process cleanup shutdown hook.", t);
				}
			}
		});
	}

	/**
	 * Returns the name of the process.
	 */
	public abstract String getName();

	/**
	 * Returns the arguments to the JVM.
	 *
	 * <p>These can be parsed by the main method of the entry point class.
	 */
	public abstract String[] getJvmArgs();

	/**
	 * Returns the name of the class to run.
	 *
	 * <p>Arguments to the main method can be specified via {@link #getJvmArgs()}.
	 */
	public abstract String getEntryPointClassName();

	// ---------------------------------------------------------------------------------------------

	/**
	 * Sets the memory for the process (<code>-Xms</code> and <code>-Xmx</code> flags) (>= 80).
	 *
	 * @param jvmMemoryInMb Amount of memory in Megabytes for the JVM (>= 80).
	 */
	public void setJVMMemory(int jvmMemoryInMb) {
		checkArgument(jvmMemoryInMb >= 80, "JobManager JVM Requires at least 80 MBs of memory.");
		this.jvmMemoryInMb = jvmMemoryInMb;
	}

	/**
	 * Creates and starts the {@link Process}.
	 *
	 * <strong>Important:</strong> Don't forget to call {@link #destroy()} to prevent
	 * resource leaks. The created process will be child process and is not guaranteed to
	 * terminate when the parent process terminates.
	 */
	public void createAndStart() throws IOException {
		String[] cmd = new String[] {
				javaCommandPath,
				"-Dlog.level=DEBUG",
				"-Dlog4j.configuration=file:" + log4jConfigFilePath,
				"-Xms" + jvmMemoryInMb + "m",
				"-Xmx" + jvmMemoryInMb + "m",
				"-classpath", getCurrentClasspath(),
				getEntryPointClassName() };

		String[] jvmArgs = getJvmArgs();

		if (jvmArgs != null && jvmArgs.length > 0) {
			cmd = ArrayUtils.addAll(cmd, jvmArgs);
		}

		synchronized (createDestroyLock) {
			if (process == null) {
				LOG.debug("Running command '{}'.", Arrays.toString(cmd));
				this.process = new ProcessBuilder(cmd).start();

				// Forward output
				this.processOutput = new StringWriter();
				new CommonTestUtils.PipeForwarder(process.getErrorStream(), processOutput);

				try {
					// Add JVM shutdown hook to call shutdown of service
					Runtime.getRuntime().addShutdownHook(shutdownHook);
				}
				catch (IllegalStateException ignored) {
					// JVM is already shutting down. No need to do this.
				}
				catch (Throwable t) {
					LOG.error("Cannot register process cleanup shutdown hook.", t);
				}
			}
			else {
				throw new IllegalStateException("Already running.");
			}
		}
	}

	public void printProcessLog() {
		if (processOutput == null) {
			throw new IllegalStateException("Not started");
		}

		System.out.println("-----------------------------------------");
		System.out.println(" BEGIN SPAWNED PROCESS LOG FOR " + getName());
		System.out.println("-----------------------------------------");

		String out = processOutput.toString();
		if (out == null || out.length() == 0) {
			System.out.println("(EMPTY)");
		}
		else {
			System.out.println(out);
		}

		System.out.println("-----------------------------------------");
		System.out.println("		END SPAWNED PROCESS LOG " + getName());
		System.out.println("-----------------------------------------");
	}

	public void destroy() {
		synchronized (createDestroyLock) {
			if (process != null) {
				LOG.debug("Destroying " + getName() + " process.");

				try {
					process.destroy();
				}
				catch (Throwable t) {
					LOG.error("Error while trying to destroy process.", t);
				}
				finally {
					process = null;

					if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
						try {
							Runtime.getRuntime().removeShutdownHook(shutdownHook);
						}
						catch (IllegalStateException ignored) {
							// JVM is in shutdown already, we can safely ignore this.
						}
						catch (Throwable t) {
							LOG.warn("Exception while unregistering prcess cleanup shutdown hook.");
						}
					}
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// File based synchronization utilities
	// ---------------------------------------------------------------------------------------------

	public static void touchFile(File file) throws IOException {
		if (!file.exists()) {
			new FileOutputStream(file).close();
		}
		if (!file.setLastModified(System.currentTimeMillis())) {
			throw new IOException("Could not touch the file.");
		}
	}

	public static void waitForMarkerFiles(File basedir, String prefix, int num, long timeout) {
		long now = System.currentTimeMillis();
		final long deadline = now + timeout;


		while (now < deadline) {
			boolean allFound = true;

			for (int i = 0; i < num; i++) {
				File nextToCheck = new File(basedir, prefix + i);
				if (!nextToCheck.exists()) {
					allFound = false;
					break;
				}
			}

			if (allFound) {
				return;
			}
			else {
				// not all found, wait for a bit
				try {
					Thread.sleep(10);
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}

				now = System.currentTimeMillis();
			}
		}

		fail("The tasks were not started within time (" + timeout + "msecs)");
	}

}
