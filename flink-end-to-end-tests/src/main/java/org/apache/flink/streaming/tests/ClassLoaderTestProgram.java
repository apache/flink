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

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

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

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
			.fromElements("Hello")
			.map((MapFunction<String, String>) value -> {

				String gitUrl;

				try (InputStream propFile = ClassLoaderTestProgram.class.getClassLoader().getResourceAsStream(".version.properties")) {
					Properties properties = new Properties();
					properties.load(propFile);
					gitUrl = properties.getProperty("git.remote.origin.url");
				}

				Enumeration<URL> resources = ClassLoaderTestProgram.class.getClassLoader().getResources(
					".version.properties");

				StringBuilder sortedProperties = new StringBuilder();
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					try (InputStream in = url.openStream()) {
						Properties properties = new Properties();
						properties.load(in);
						String orderedGitUrl = properties.getProperty("git.remote.origin.url");
						sortedProperties.append(orderedGitUrl);
					}
				}

				if (resolveOrder.equals("parent-first")) {
					try {
						@SuppressWarnings("unused")
						String ignored = TaskManager.getMessage();

						throw new RuntimeException(
							"TaskManager.getMessage() should not be available with parent-first " +
								"ClassLoader order.");

					} catch (NoSuchMethodError e) {
						// expected
					}
					return "NoSuchMethodError:" + gitUrl + ":" + sortedProperties;
				} else if (resolveOrder.equals("child-first")) {
					String message = TaskManager.getMessage();
					if (!message.equals("Hello, World!")) {
						throw new RuntimeException("Wrong message from fake TaskManager.");
					}
					return message + ":" + gitUrl + ":" + sortedProperties;
				} else {
					throw new RuntimeException("Unknown resolve order: " + resolveOrder);
				}
			})
			.writeAsText(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE);

		env.execute("ClassLoader Test Program");
	}
}
