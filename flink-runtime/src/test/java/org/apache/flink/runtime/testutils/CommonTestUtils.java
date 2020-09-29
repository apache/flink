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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.SupplierWithException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class contains auxiliary methods for unit tests.
 */
public class CommonTestUtils {

	private static final long RETRY_INTERVAL = 100L;

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
	 * Create a temporary log4j configuration for the test.
	 */
	public static File createTemporaryLog4JProperties() throws IOException {
		File log4jProps = File.createTempFile(
				FileUtils.getRandomFilename(""), "-log4j.properties");
		log4jProps.deleteOnExit();
		CommonTestUtils.printLog4jDebugConfig(log4jProps);

		return log4jProps;
	}

	/**
	 * Tries to get the java executable command with which the current JVM was started.
	 * Returns null, if the command could not be found.
	 *
	 * @return The java executable command.
	 */
	public static String getJavaCommandPath() {
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
		return null;
	}

	public static void printLog4jDebugConfig(File file) throws IOException {
		try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
			writer.println("rootLogger.level = INFO");
			writer.println("rootLogger.appenderRef.console.ref = ConsoleAppender");
			writer.println("appender.console.name = ConsoleAppender");
			writer.println("appender.console.type = CONSOLE");
			writer.println("appender.console.target = SYSTEM_ERR");
			writer.println("appender.console.layout.type = PatternLayout");
			writer.println("appender.console.layout.pattern = %d{HH:mm:ss,SSS} %-4r [%t] %-5p %c %x - %m%n");
			writer.println("logger.jetty.name = org.eclipse.jetty.util.log");
			writer.println("logger.jetty.level = OFF");
			writer.println("logger.zookeeper.name = org.apache.zookeeper");
			writer.println("logger.zookeeper.level = OFF");
			writer.flush();
		}
	}

	public static void waitUntilCondition(SupplierWithException<Boolean, Exception> condition, Deadline timeout) throws Exception {
		waitUntilCondition(condition, timeout, RETRY_INTERVAL);
	}

	public static void waitUntilCondition(SupplierWithException<Boolean, Exception> condition, Deadline timeout, long retryIntervalMillis) throws Exception {
		while (timeout.hasTimeLeft() && !condition.get()) {
			final long timeLeft = Math.max(0, timeout.timeLeft().toMillis());
			Thread.sleep(Math.min(retryIntervalMillis, timeLeft));
		}

		if (!timeout.hasTimeLeft()) {
			throw new TimeoutException("Condition was not met in given timeout.");
		}
	}

	public static void waitUntilJobManagerIsInitialized(SupplierWithException<JobStatus, Exception> jobStatusSupplier) throws
		Exception {
		waitUntilJobManagerIsInitialized(jobStatusSupplier, Deadline.fromNow(Duration.of(1,
			ChronoUnit.MINUTES)));
	}

	public static void waitUntilJobManagerIsInitialized(SupplierWithException<JobStatus, Exception> jobStatusSupplier, Deadline timeout) throws
		Exception {
		waitUntilCondition(() -> jobStatusSupplier.get() != JobStatus.INITIALIZING, timeout, 20L);
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

	public static boolean isStreamContentEqual(InputStream input1, InputStream input2) throws IOException {

		if (!(input1 instanceof BufferedInputStream)) {
			input1 = new BufferedInputStream(input1);
		}
		if (!(input2 instanceof BufferedInputStream)) {
			input2 = new BufferedInputStream(input2);
		}

		int ch = input1.read();
		while (-1 != ch) {
			int ch2 = input2.read();
			if (ch != ch2) {
				return false;
			}
			ch = input1.read();
		}

		int ch2 = input2.read();
		return (ch2 == -1);
	}
}
