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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;

/**
 * A {@code SplitReader} that returns a pre-defined set of records (by split).
 */
public class TestingSplitReader<E, SplitT extends SourceSplit> implements SplitReader<E, SplitT> {

	private final ArrayDeque<RecordsWithSplitIds<E>> fetches;

	@SafeVarargs
	public TestingSplitReader(RecordsWithSplitIds<E>... fetches) {
		this.fetches = new ArrayDeque<>(fetches.length);
		this.fetches.addAll(Arrays.asList(fetches));
	}

	@Override
	public RecordsWithSplitIds<E> fetch() throws IOException {
		if (!fetches.isEmpty()) {
			return fetches.removeFirst();
		} else {
			// block until woken up
			synchronized (fetches) {
				try {
					fetches.wait();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return null;
			}
		}
	}

	@Override
	public void handleSplitsChanges(SplitsChange<SplitT> splitsChanges) {}

	@Override
	public void wakeUp() {
		synchronized (fetches) {
			fetches.notifyAll();
		}
	}
}
