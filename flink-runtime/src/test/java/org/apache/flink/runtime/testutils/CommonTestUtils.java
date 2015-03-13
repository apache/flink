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

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

/**
 * This class contains auxiliary methods for unit tests.
 */
public class CommonTestUtils {

	/**
	 * Creates a copy of the given {@link IOReadableWritable} object by an in-memory serialization and subsequent
	 * deserialization.
	 * 
	 * @param original
	 *        the original object to be copied
	 * @return the copy of original object created by the original object's serialization/deserialization methods
	 * @throws IOException
	 *         thrown if an error occurs while creating the copy of the object
	 */
	@SuppressWarnings("unchecked")
	public static <T extends IOReadableWritable> T createCopyWritable(final T original) throws IOException {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);

		original.write(new OutputViewDataOutputStreamWrapper(dos));

		final String className = original.getClass().getName();

		Class<T> clazz = null;

		try {
			clazz = (Class<T>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			fail(e.getMessage());
		}

		T copy = null;
		try {
			copy = clazz.newInstance();
		} catch (Throwable t) {
			t.printStackTrace();
			fail(t.getMessage());
		}

		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final DataInputStream dis = new DataInputStream(bais);

		copy.read(new InputViewDataInputStreamWrapper(dis));
		if (dis.available() > 0) {
			throw new IOException("The coped result was not fully consumed.");
		}

		return copy;
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends java.io.Serializable> T createCopySerializable(T original) throws IOException {
		if (original == null) {
			throw new IllegalArgumentException();
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(original);
		oos.close();
		baos.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		ObjectInputStream ois = new ObjectInputStream(bais);
		
		T copy;
		try {
			copy = (T) ois.readObject();
		}
		catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		
		ois.close();
		bais.close();
		
		return copy;
	}

	/**
	 * Sleeps for a given set of milliseconds, uninterruptibly. If interrupt is called,
	 * the sleep will continue nonetheless.
	 *
	 * @param msecs The number of milliseconds to sleep.
	 */
	public static void sleepUninterruptibly(long msecs) {
		
		long now = System.currentTimeMillis();
		long sleepUntil = now + msecs;
		long remaining;
		
		while ((remaining = sleepUntil - now) > 0) {
			try {
				Thread.sleep(remaining);
			}
			catch (InterruptedException e) {}
			
			now = System.currentTimeMillis();
		}
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

	/**
	 * Checks whether a process is still alive. Utility method for JVM versions before 1.8,
	 * where no direct method to check that is available.
	 *
	 * @param process The process to check.
	 * @return True, if the process is alive, false otherwise.
	 */
	public static boolean isProcessAlive(Process process) {
		if (process == null) {
			return false;

		}
		try {
			process.exitValue();
			return false;
		}
		catch(IllegalThreadStateException e) {
			return true;
		}
	}

	public static void printLog4jDebugConfig(File file) throws IOException {
		FileWriter fw = new FileWriter(file);
		try {
			PrintWriter writer = new PrintWriter(fw);

			writer.println("log4j.rootLogger=DEBUG, console");
			writer.println("log4j.appender.console=org.apache.log4j.ConsoleAppender");
			writer.println("log4j.appender.console.target = System.err");
			writer.println("log4j.appender.console.layout=org.apache.log4j.PatternLayout");
			writer.println("log4j.appender.console.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n");
			writer.println("log4j.logger.org.eclipse.jetty.util.log=OFF");

			writer.flush();
			writer.close();
		}
		finally {
			fw.close();
		}
	}
}
