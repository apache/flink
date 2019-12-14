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

package org.apache.flink.test.classloading.jar;

import java.io.File;
import java.net.URL;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple program that verifies the classloading policy by ensuring the resource loaded is under the specified
 * directory.
 **/
public class ClassLoadingPolicyProgram {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			throw new IllegalArgumentException("Missing parameters. Expected: <resourceName> <expectedResourceDir>");
		}
		String resourceName = args[0];
		String expectedResourceDir = args[1];
		URL url = Thread.currentThread().getContextClassLoader().getResource(resourceName);
		checkNotNull(url, "Failed to find " + resourceName + " in the classpath");
		File file = new File(url.toURI());
		String actualResourceDir = file.getParentFile().getName();
		if (!actualResourceDir.equals(expectedResourceDir)) {
			String msg = "Incorrect " + resourceName + " is loaded, which should be in " + expectedResourceDir +
				", but now is in " + actualResourceDir;
			throw new RuntimeException(msg);
		}
	}
}
