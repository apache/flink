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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.assigners.SimpleSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.testutils.TestingFileEnumerator;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import java.io.File;
import java.util.Collections;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for the {@link ContinuousFileSplitEnumerator}.
 */
public class ContinuousFileSplitEnumeratorTest {

	// this is no JUnit temporary folder, because we don't create actual files, we just
	// need some random file path.
	private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

	private static long splitId = 1L;

	@Test
	public void testDiscoverSplitWhenNoReaderRegistered() throws Exception {
		final TestingFileEnumerator fileEnumerator = new TestingFileEnumerator();
		final TestingSplitEnumeratorContext<FileSourceSplit> context = new TestingSplitEnumeratorContext<>(4);
		final ContinuousFileSplitEnumerator enumerator = createEnumerator(fileEnumerator, context);

		// make one split available and trigger the periodic discovery
		final FileSourceSplit split = createRandomSplit();
		fileEnumerator.addSplits(split);
		context.triggerAllActions();

		assertThat(enumerator.snapshotState().getSplits(), contains(split));
	}

	@Test
	public void testDiscoverWhenReaderRegistered() throws Exception {
		final TestingFileEnumerator fileEnumerator = new TestingFileEnumerator();
		final TestingSplitEnumeratorContext<FileSourceSplit> context = new TestingSplitEnumeratorContext<>(4);
		final ContinuousFileSplitEnumerator enumerator = createEnumerator(fileEnumerator, context);

		// register one reader, and let it request a split
		context.registerReader(2, "localhost");
		enumerator.addReader(2);
		enumerator.handleSplitRequest(2, "localhost");

		// make one split available and trigger the periodic discovery
		final FileSourceSplit split = createRandomSplit();
		fileEnumerator.addSplits(split);
		context.triggerAllActions();

		assertThat(enumerator.snapshotState().getSplits(), empty());
		assertThat(context.getSplitAssignments().get(2).getAssignedSplits(), contains(split));
	}

	@Test
	public void testRequestingReaderUnavailableWhenSplitDiscovered() throws Exception {
		final TestingFileEnumerator fileEnumerator = new TestingFileEnumerator();
		final TestingSplitEnumeratorContext<FileSourceSplit> context = new TestingSplitEnumeratorContext<>(4);
		final ContinuousFileSplitEnumerator enumerator = createEnumerator(fileEnumerator, context);

		// register one reader, and let it request a split
		context.registerReader(2, "localhost");
		enumerator.addReader(2);
		enumerator.handleSplitRequest(2, "localhost");

		// remove the reader (like in a failure)
		context.registeredReaders().remove(2);

		// make one split available and trigger the periodic discovery
		final FileSourceSplit split = createRandomSplit();
		fileEnumerator.addSplits(split);
		context.triggerAllActions();

		assertFalse(context.getSplitAssignments().containsKey(2));
		assertThat(enumerator.snapshotState().getSplits(), contains(split));
	}

	// ------------------------------------------------------------------------
	//  test setup helpers
	// ------------------------------------------------------------------------

	private static FileSourceSplit createRandomSplit() {
		return new FileSourceSplit(
				String.valueOf(splitId++),
				Path.fromLocalFile(new File(TMP_DIR, "foo")),
				0L,
				0L);
	}

	private static ContinuousFileSplitEnumerator createEnumerator(
			final FileEnumerator fileEnumerator,
			final SplitEnumeratorContext<FileSourceSplit> context) {

		final ContinuousFileSplitEnumerator enumerator = new ContinuousFileSplitEnumerator(
				context,
				fileEnumerator,
				new SimpleSplitAssigner(Collections.emptyList()),
				new Path[] { Path.fromLocalFile(TMP_DIR) },
				Collections.emptySet(),
				10L);
		enumerator.start();
		return enumerator;
	}
}
