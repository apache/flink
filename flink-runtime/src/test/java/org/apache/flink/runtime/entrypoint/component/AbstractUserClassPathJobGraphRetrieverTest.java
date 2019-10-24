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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FileUtilsTest;
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link AbstractUserClassPathJobGraphRetriever}.
 */
public class AbstractUserClassPathJobGraphRetrieverTest extends TestLogger {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static class TestJobGraphRetriever extends AbstractUserClassPathJobGraphRetriever {
		public TestJobGraphRetriever(@Nonnull final File jobDir) throws IOException {
			super(jobDir);
		}

		@Override
		public JobGraph retrieveJobGraph(Configuration configuration) {
			return null;
		}
	}

	@Test
	public void testGetUserClassPath() throws IOException {
		final Path testJobDir = temporaryFolder.newFolder("_test_job").toPath();
		final Tuple3<Collection<File>, Collection<File>, Collection<URL>>
			result = FileUtilsTest.prepareTestFiles(testJobDir);
		final TestJobGraphRetriever testJobGraphRetriever = new TestJobGraphRetriever(testJobDir.toFile());
		assertTrue(CollectionUtils.isEqualCollection(result.f2, testJobGraphRetriever.getUserClassPaths()));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTheJobGraphRetrieverThrowExceptionBecauseJobDirDoesNotHaveAnyJars() throws IOException {
		final Path testJobDir = temporaryFolder.newFolder("_test_job_").toPath();
		new TestJobGraphRetriever(testJobDir.toFile());
	}

	@Test
	public void testGetUserClassPathReturnEmptyListIfJobDirIsNull() throws IOException {
		final TestJobGraphRetriever testJobGraphRetriever = new TestJobGraphRetriever(null);
		assertTrue(Collections.<URL>emptyList() == testJobGraphRetriever.getUserClassPaths());
	}
}
