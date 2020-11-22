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

package org.apache.flink.connector.file.src.testutils;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * A {@link FileEnumerator} where splits are manually added during tests.
 */
public class TestingFileEnumerator implements FileEnumerator {

	private final ArrayDeque<FileSourceSplit> splits = new ArrayDeque<>();

	public TestingFileEnumerator(FileSourceSplit... initialSplits) {
		addSplits(initialSplits);
	}

	@Override
	public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits) throws IOException {
		synchronized (splits) {
			final ArrayList<FileSourceSplit> currentSplits = new ArrayList<>(splits);
			splits.clear();
			return currentSplits;
		}
	}

	public void addSplits(FileSourceSplit... newSplits) {
		synchronized (splits) {
			splits.addAll(Arrays.asList(newSplits));
		}
	}
}
