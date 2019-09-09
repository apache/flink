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

package org.apache.flink.container.entrypoint.testjar;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The test job information.
 */
public class TestJobInfo {

	public static final String JOB_CLASS = "org.apache.flink.container.entrypoint.testjar.TestUserClassLoaderJob";
	public static final String JOB_LIB_CLASS = "org.apache.flink.container.entrypoint.testjar.TestUserClassLoaderJobLib";
	public static final Path JOB_JAR_PATH = Paths.get("target", "maven-test-user-classloader-job-jar.jar");
	public static final Path JOB_LIB_JAR_PATH = Paths.get("target", "maven-test-user-classloader-job-lib-jar.jar");
}
