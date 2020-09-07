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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import java.util.function.Supplier;

/**
 * A abstract {@link SourceReader} implementation that assign all the splits to a single thread to consume.
 * @param <E>
 * @param <T>
 * @param <SplitT>
 * @param <SplitStateT>
 */
public abstract class SingleThreadMultiplexSourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
	extends SourceReaderBase<E, T, SplitT, SplitStateT> {

	public SingleThreadMultiplexSourceReaderBase(
		FutureNotifier futureNotifier,
		FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
		Supplier<SplitReader<E, SplitT>> splitReaderSupplier,
		RecordEmitter<E, T, SplitStateT> recordEmitter,
		Configuration config,
		SourceReaderContext context) {
		super(
			futureNotifier,
			elementsQueue,
			new SingleThreadFetcherManager<>(futureNotifier, elementsQueue, splitReaderSupplier),
			recordEmitter,
			config,
			context);
	}
}
