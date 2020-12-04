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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.FileSourceSplitState;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;

import java.util.Map;

/**
 * A {@link SourceReader} that read records from {@link FileSourceSplit}.
 */
@Internal
public final class FileSourceReader<T, SplitT extends FileSourceSplit>
		extends SingleThreadMultiplexSourceReaderBase<RecordAndPosition<T>, T, SplitT, FileSourceSplitState<SplitT>> {

	public FileSourceReader(SourceReaderContext readerContext, BulkFormat<T, SplitT> readerFormat, Configuration config) {
		super(
			() -> new FileSourceSplitReader<>(config, readerFormat),
			new FileSourceRecordEmitter<>(),
			config,
			readerContext);
	}

	@Override
	public void start() {
		// we request a split only if we did not get splits during the checkpoint restore
		if (getNumberOfCurrentlyAssignedSplits() == 0) {
			context.sendSplitRequest();
		}
	}

	@Override
	protected void onSplitFinished(Map<String, FileSourceSplitState<SplitT>> finishedSplitIds) {
		context.sendSplitRequest();
	}

	@Override
	protected FileSourceSplitState<SplitT> initializedState(SplitT split) {
		return new FileSourceSplitState<>(split);
	}

	@Override
	protected SplitT toSplitType(String splitId, FileSourceSplitState<SplitT> splitState) {
		return splitState.toFileSourceSplit();
	}
}
