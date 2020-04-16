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

package org.apache.flink.runtime.util;

import org.apache.flink.test.util.TestProcessBuilder;
import org.apache.flink.test.util.TestProcessBuilder.TestProcess;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.testutils.ClassLoaderUtils.ClassLoaderBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link  ExceptionUtils} which require to spawn JVM process and set JVM memory args.
 */
public class ExceptionUtilsITCases extends TestLogger {
	private static final int DIRECT_MEMORY_SIZE = 10 * 1024; // 10Kb
	private static final int DIRECT_MEMORY_ALLOCATION_PAGE_SIZE = 1024; // 1Kb
	private static final int DIRECT_MEMORY_PAGE_NUMBER = DIRECT_MEMORY_SIZE / DIRECT_MEMORY_ALLOCATION_PAGE_SIZE;
	private static final long INITIAL_BIG_METASPACE_SIZE = 128 * (1 << 20); // 128Mb

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Test
	public void testIsDirectOutOfMemoryError() throws IOException, InterruptedException {
		String className = DummyDirectAllocatingProgram.class.getName();
		String out = run(className, Collections.emptyList(), DIRECT_MEMORY_SIZE, -1);
		assertThat(out, is(""));
	}

	@Test
	public void testIsMetaspaceOutOfMemoryError() throws IOException, InterruptedException {
		String className = DummyClassLoadingProgram.class.getName();
		// load only one class and record required Metaspace
		String normalOut = run(className, getDummyClassLoadingProgramArgs(1), -1, INITIAL_BIG_METASPACE_SIZE);
		long okMetaspace = Long.parseLong(normalOut);
		// load more classes to cause 'OutOfMemoryError: Metaspace'
		String oomOut = run(className, getDummyClassLoadingProgramArgs(1000), -1, okMetaspace);
		assertThat(oomOut, is(""));
	}

	private static String run(
			String className,
			Iterable<String> args,
			long directMemorySize,
			long metaspaceSize) throws InterruptedException, IOException {
		TestProcessBuilder taskManagerProcessBuilder = new TestProcessBuilder(className);
		if (directMemorySize > 0) {
			taskManagerProcessBuilder.addJvmArg(String.format("-XX:MaxDirectMemorySize=%d", directMemorySize));
		}
		if (metaspaceSize > 0) {
			taskManagerProcessBuilder.addJvmArg("-XX:-UseCompressedOops");
			taskManagerProcessBuilder.addJvmArg(String.format("-XX:MaxMetaspaceSize=%d", metaspaceSize));
		}
		for (String arg : args) {
			taskManagerProcessBuilder.addMainClassArg(arg);
		}
		TestProcess p = taskManagerProcessBuilder.start();
		p.getProcess().waitFor();
		assertThat(p.getErrorOutput().toString().trim(), is(""));
		return p.getProcessOutput().toString().trim();
	}

	private static Collection<String> getDummyClassLoadingProgramArgs(int numberOfLoadedClasses) {
		return Arrays.asList(
			Integer.toString(numberOfLoadedClasses),
			TEMPORARY_FOLDER.getRoot().getAbsolutePath());
	}

	/**
	 * Dummy java program to generate Direct OOM.
	 */
	public static class DummyDirectAllocatingProgram {
		private DummyDirectAllocatingProgram() {
		}

		public static void main(String[] args) {
			try {
				Collection<ByteBuffer> buffers = new ArrayList<>();
				for (int page = 0; page < 2 * DIRECT_MEMORY_PAGE_NUMBER; page++) {
					buffers.add(ByteBuffer.allocateDirect(DIRECT_MEMORY_ALLOCATION_PAGE_SIZE));
				}
				output("buffers: " + buffers);
			} catch (Throwable t) {
				if (!ExceptionUtils.isDirectOutOfMemoryError(t)) {
					output("Wrong exception: " + t);
				}
			}
		}
	}

	/**
	 * Dummy java program to generate Metaspace OOM.
	 */
	public static class DummyClassLoadingProgram {
		private DummyClassLoadingProgram() {
		}

		public static void main(String[] args) {
			// trigger needed classes loaded
			output("");
			ExceptionUtils.isMetaspaceOutOfMemoryError(new Exception());

			Collection<Class<?>> classes = new ArrayList<>();
			int numberOfLoadedClasses = Integer.parseInt(args[0]);
			try {
				for (int index = 0; index < numberOfLoadedClasses; index++) {
					classes.add(loadDummyClass(index, args[1]));
				}
				String out = classes.size() > 1 ? "Exception is not thrown, metaspace usage: " : "";
				output(out + getMetaspaceUsage());
			} catch (Throwable t) {
				if (ExceptionUtils.isMetaspaceOutOfMemoryError(t)) {
					return;
				}
				output("Wrong exception: " + t);
			}
		}

		private static Class<?> loadDummyClass(int index, String folderToSaveSource) throws ClassNotFoundException, IOException {
			String className = "DummyClass" + index;
			String sourcePattern = "public class %s { @Override public String toString() { return \"%s\"; } }";
			ClassLoaderBuilder classLoaderBuilder = ClassLoaderUtils.withRoot(new File(folderToSaveSource));
			classLoaderBuilder.addClass(className, String.format(sourcePattern, className, "dummy"));
			ClassLoader classLoader = classLoaderBuilder.build();
			return Class.forName(className, true, classLoader);
		}

		private static long getMetaspaceUsage() {
			for (MemoryPoolMXBean memoryMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
				if ("Metaspace".equals(memoryMXBean.getName())) {
					return memoryMXBean.getUsage().getUsed();
				}
			}
			throw new RuntimeException("Metaspace usage is not found");
		}
	}

	private static void output(String text) {
		System.out.println(text);
	}
}
