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
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.event.NoMoreSplitsEvent;
import org.apache.flink.connector.base.source.event.RequestSplitEvent;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A SplitEnumerator implementation for bounded / batch {@link FileSource} input.
 *
 * <p>This enumerator takes all files that are present in the configured input directories and assigns
 * them to the readers. Once all files are processed, the source is finished.
 *
 * <p>The implementation of this class is rather thin. The actual logic for creating the set of
 * FileSourceSplits to process, and the logic to decide which reader gets what split, are in
 * {@link FileEnumerator} and in {@link FileSplitAssigner}, respectively.
 */
@Internal
public class StaticFileSplitEnumerator implements SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> {

	private static final Logger LOG = LoggerFactory.getLogger(StaticFileSplitEnumerator.class);

	private final SplitEnumeratorContext<FileSourceSplit> context;

	private final FileSplitAssigner splitAssigner;

	// ------------------------------------------------------------------------

	public StaticFileSplitEnumerator(
			SplitEnumeratorContext<FileSourceSplit> context,
			FileSplitAssigner splitAssigner) {
		this.context = checkNotNull(context);
		this.splitAssigner = checkNotNull(splitAssigner);
	}

	@Override
	public void start() {
		// no resources to start
	}

	@Override
	public void close() throws IOException {
		// no resources to close
	}

	@Override
	public void addReader(int subtaskId) {
		// this source is purely lazy-pull-based, nothing to do upon registration
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		if (sourceEvent instanceof RequestSplitEvent) {
			final RequestSplitEvent requestSplitEvent = (RequestSplitEvent) sourceEvent;
			assignNextEvents(subtaskId, requestSplitEvent.hostName());
		}
		else {
			LOG.error("Received unrecognized event: {}", sourceEvent);
		}
	}

	@Override
	public void addSplitsBack(List<FileSourceSplit> splits, int subtaskId) {
		LOG.debug("File Source Enumerator adds splits back: {}", splits);
		splitAssigner.addSplits(splits);
	}

	@Override
	public PendingSplitsCheckpoint snapshotState() {
		return PendingSplitsCheckpoint.fromCollectionSnapshot(splitAssigner.remainingSplits());
	}

	// ------------------------------------------------------------------------

	private void assignNextEvents(int subtask, @Nullable String hostname) {
		if (LOG.isInfoEnabled()) {
			final String hostInfo = hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
			LOG.info("Subtask {} {} is requesting a file source split", subtask, hostInfo);
		}

		final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
		if (nextSplit.isPresent()) {
			final FileSourceSplit split = nextSplit.get();
			context.assignSplit(split, subtask);
			LOG.info("Assigned split to subtask {} : {}", subtask, split);
		}
		else {
			context.sendEventToSourceReader(subtask, new NoMoreSplitsEvent());
			LOG.info("No more splits available for subtask {}", subtask);
		}
	}
}
