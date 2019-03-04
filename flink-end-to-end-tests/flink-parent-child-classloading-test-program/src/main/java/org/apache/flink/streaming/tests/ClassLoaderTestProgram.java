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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;

/**
 * End-to-end test program for verifying that the {@code classloader.resolve-order} setting is being
 * honored by Flink. We test this by creating a {@code ParentChildTestingVehicle} with a single
 * method that we call in the same package as the {@code ParentChildTestingVehicle} in the "lib"
 * package (flink-parent-child-classloading-test-lib-package) and verify the message in the
 * end-to-end test script.
 */
public class ClassLoaderTestProgram {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env
			.fromElements("Hello")
			.map((MapFunction<String, String>) value -> {

				String messageFromPropsFile;

				try (InputStream propFile = ClassLoaderTestProgram.class.getClassLoader().getResourceAsStream("parent-child-test.properties")) {
					Properties properties = new Properties();
					properties.load(propFile);
					messageFromPropsFile = properties.getProperty("message");
				}

				// Enumerate all properties files we can find and store the messages in the
				// order we find them. The order will be different between parent-first and
				// child-first classloader mode.
				Enumeration<URL> resources = ClassLoaderTestProgram.class.getClassLoader().getResources(
					"parent-child-test.properties");

				StringBuilder orderedProperties = new StringBuilder();
				while (resources.hasMoreElements()) {
					URL url = resources.nextElement();
					try (InputStream in = url.openStream()) {
						Properties properties = new Properties();
						properties.load(in);
						String messageFromEnumeratedPropsFile = properties.getProperty("message");
						orderedProperties.append(messageFromEnumeratedPropsFile);
					}
				}

				String message = ParentChildTestingVehicle.getMessage();
				return message + ":" + messageFromPropsFile + ":" + orderedProperties;
			})
			.writeAsText(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE);

		env.execute("ClassLoader Test Program");
	}
}
