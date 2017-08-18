/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * End-to-end test program for verifying that the {@code classloader.resolve-order} setting
 * is being honored by Flink. We test this by creating a fake {@code TaskManager} with a single
 * method that we call in the same package as the original Flink {@code TaskManager} and verify that
 * we get a {@link NoSuchMethodError} if we're running with {@code parent-first} class loading
 * and that we get the correct result from the method when we're running with {@code child-first}
 * class loading.
 */
public class ClassLoaderTestProgram {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final String resolveOrder = params.getRequired("resolve-order");

		if (resolveOrder.equals("parent-first")) {
			boolean caughtException = false;
			try {
				String ignored = TaskManager.getMessage();
			} catch (NoSuchMethodError e) {
				// expected
				caughtException = true;
			}
			if (!caughtException) {
				throw new RuntimeException(
					"TaskManager.getMessage() should not be available with parent-first " +
						"ClassLoader order.");
			}
		} else if (resolveOrder.equals("child-first")) {
			String message = TaskManager.getMessage();
			if (!message.equals("Hello, World!")) {
				throw new RuntimeException("Wrong message from fake TaskManager.");
			}
		} else {
			throw new RuntimeException("Unknown resolve order: " + resolveOrder);
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
			.fromElements("Hello")
			.map((MapFunction<String, String>) value -> {
				if (resolveOrder.equals("parent-first")) {
					boolean caughtException = false;
					try {
						String ignored = TaskManager.getMessage();
					} catch (NoSuchMethodError e) {
						// expected
						caughtException = true;
					}
					if (!caughtException) {
						throw new RuntimeException(
							"TaskManager.getMessage() should not be available with parent-first " +
								"ClassLoader order.");
					}
					return "NoSuchMethodError";
				} else if (resolveOrder.equals("child-first")) {
					String message = TaskManager.getMessage();
					if (!message.equals("Hello, World!")) {
						throw new RuntimeException("Wrong message from fake TaskManager.");
					}
					return message;
				} else {
					throw new RuntimeException("Unknown resolve order: " + resolveOrder);
				}
			})
			.writeAsText(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE);

		env.execute("ClassLoader Test Program");
	}
}
