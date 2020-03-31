/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.base.source.reader.fetcher;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import java.util.List;
import java.util.function.Supplier;

/**
 * A Fetcher manager with a single fetcher and assign all the splits to it.
 */
public class SingleThreadFetcherManager<E, SplitT extends SourceSplit>
		extends SplitFetcherManager<E, SplitT> {

	public SingleThreadFetcherManager(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
		super(futureNotifier, elementsQueue, splitReaderSupplier);
	}

	@Override
	public void addSplits(List<SplitT> splitsToAdd) {
		SplitFetcher<E, SplitT> fetcher = fetchers.get(0);
		if (fetcher == null) {
			fetcher = createSplitFetcher();
			// Add the splits to the fetchers.
			fetcher.addSplits(splitsToAdd);
			startFetcher(fetcher);
		} else {
			fetcher.addSplits(splitsToAdd);
		}
	}
}
