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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FileUtilsTest;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AbstractUserClassPathJobGraphRetriever}.
 */
public class AbstractUserClassPathJobGraphRetrieverTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static class TestJobGraphRetriever extends AbstractUserClassPathJobGraphRetriever {
		TestJobGraphRetriever(File jobDir) throws IOException {
			super(jobDir);
		}

		@Override
		public JobGraph retrieveJobGraph(Configuration configuration) {
			throw new UnsupportedOperationException("This method should not be called.");
		}
	}

	@Test
	public void testGetUserClassPath() throws IOException {
		final File testJobDir = temporaryFolder.newFolder("_test_job");
		final Collection<Path> testFiles = FileUtilsTest.prepareTestFiles(testJobDir.toPath());
		final Path currentWorkingDirectory = FileUtils.getCurrentWorkingDirectory();
		final TestJobGraphRetriever testJobGraphRetriever = new TestJobGraphRetriever(testJobDir);

		assertThat(testJobGraphRetriever.getUserClassPaths(), containsInAnyOrder(testFiles.stream()
			.map(file -> FileUtils.relativizePath(currentWorkingDirectory, file))
			.map(FunctionUtils.uncheckedFunction(FileUtils::toURL)).toArray()));
	}

	@Test
	public void testGetUserClassPathReturnEmptyListIfJobDirIsNull() throws IOException {
		final TestJobGraphRetriever testJobGraphRetriever = new TestJobGraphRetriever(null);
		assertTrue(testJobGraphRetriever.getUserClassPaths().isEmpty());
	}
}
