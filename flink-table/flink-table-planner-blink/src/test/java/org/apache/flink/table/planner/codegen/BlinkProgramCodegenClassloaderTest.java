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

package org.apache.flink.table.planner.codegen;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;


/**
 * test for blink planner load and compile user class when generate code.
 */
public class BlinkProgramCodegenClassloaderTest {

	public static final String BLINK_JAR_MAIN_CLASS =
		"org.apache.flink.table.testjar.BlinkPlannerCodegenClassLoaderProgram";

	@ClassRule
	public static final TemporaryFolder FOLDER = new TemporaryFolder();

	/**
	 * This tests whether blink planner could load user classes by UserCodeClassLoader.
	 */
	@Test
	public void testBlinkPlannerProgram() throws ProgramInvocationException, IOException {
		File outputFile = FOLDER.newFile();
		File jar = new File("target/maven-test-jar.jar");
		final PackagedProgram blinkProgram = PackagedProgram.newBuilder()
			.setJarFile(jar)
			.setArguments(outputFile.toURI().toString())
			.setEntryPointClassName(BLINK_JAR_MAIN_CLASS)
			.build();
		// current classloader is provided by JVM (AppClassLoader), but classes in the jar
		// should be loaded by UserCodeClassloader (ParentFirstClassLoader or ChildFirstClassLoader)
		JobGraph jobGraph = PackagedProgramUtils.createJobGraph(blinkProgram, new Configuration(), 1, false);
		assertTrue(jobGraph.isCheckpointingEnabled());
	}
}
