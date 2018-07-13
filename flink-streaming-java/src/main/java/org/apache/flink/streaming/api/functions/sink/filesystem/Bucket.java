/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.Writer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.ResumableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A bucket is the directory organization of the output of the {@link StreamingFileSink}.
 *
 * <p>For each incoming  element in the {@code BucketingSink}, the user-specified
 * {@link org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer Bucketer} is
 * queried to see in which bucket this element should be written to.
 */
public class Bucket<IN> {

	private static final String PART_PREFIX = "part";

	private final Path bucketPath;

	private int subtaskIndex;

	private long partCounter;

	private final Writer<IN> outputFormatWriter;

	private final ResumableWriter fsWriter;

	private final CurrentPartFileHandler<IN> handler;

	private List<ResumableWriter.CommitRecoverable> pending = new ArrayList<>();

	private Map<Long, List<ResumableWriter.CommitRecoverable>> pendingPerCheckpoint = new HashMap<>();

	public Bucket(
			ResumableWriter fsWriter,
			int subtaskIndex,
			Path bucketPath,
			long initialPartCounter,
			Writer<IN> writer,
			BucketState bucketstate) throws IOException {

		this(fsWriter, subtaskIndex, bucketPath, initialPartCounter, writer);

		// the constructor must have already initialized the filesystem writer
		Preconditions.checkState(fsWriter != null);

		// we try to resume the previous in-progress file, if the filesystem
		// supports such operation. If not, we just commit the file and start fresh.

		final ResumableWriter.ResumeRecoverable resumable = bucketstate.getCurrentInProgress();
		if (resumable != null) {
			Preconditions.checkNotNull(resumable);
			handler.resumeFrom(fsWriter.recover(resumable), bucketstate.getCreationTime());
		}

		// we commit pending files for previous checkpoints to the last successful one
		// (from which we are recovering from)
		for (List<ResumableWriter.CommitRecoverable> commitables: bucketstate.getPendingPerCheckpoint().values()) {
			for (ResumableWriter.CommitRecoverable commitable: commitables) {
				fsWriter.recoverForCommit(commitable).commitAfterRecovery();
			}
		}
	}

	public Bucket(
			ResumableWriter fsWriter,
			int subtaskIndex,
			Path bucketPath,
			long initialPartCounter,
			Writer<IN> writer) {

		this.fsWriter = Preconditions.checkNotNull(fsWriter);
		this.subtaskIndex = subtaskIndex;
		this.bucketPath = Preconditions.checkNotNull(bucketPath);
		this.partCounter = initialPartCounter;
		this.outputFormatWriter = Preconditions.checkNotNull(writer);
		this.handler = new CurrentPartFileHandler<>();
	}

	public RollingPolicy.PartFileInfoHandler getCurrentPartFileInfo() {
		return handler;
	}

	public Path getBucketPath() {
		return bucketPath;
	}

	public long getPartCounter() {
		return partCounter;
	}

	public boolean isActive() {
		return handler.isOpen() || !pending.isEmpty() || !pendingPerCheckpoint.isEmpty();
	}

	void write(IN element, long currentTime) throws IOException {
		Preconditions.checkState(handler.isOpen());
		handler.write(element, outputFormatWriter, currentTime);
	}

	void rollPartPartFile(final long currentTime) throws IOException {
		closePartFile();
		handler.open(fsWriter, getNewPartPath(), currentTime);
		this.partCounter++;
	}

	void merge(final Bucket<IN> bucket) throws IOException {
		Preconditions.checkNotNull(bucket);
		Preconditions.checkState(bucket.getBucketPath().equals(getBucketPath()));

		// there should be no pending files in the "to-merge" states.
		Preconditions.checkState(bucket.pending.isEmpty());
		Preconditions.checkState(bucket.pendingPerCheckpoint.isEmpty());

		ResumableWriter.CommitRecoverable commitable = bucket.closePartFile();
		if (commitable != null) {
			pending.add(commitable);
		}
	}

	ResumableWriter.CommitRecoverable closePartFile() throws IOException {
		ResumableWriter.CommitRecoverable commitable = null;
		if (handler.isOpen()) {
			commitable = handler.close();
			pending.add(commitable);
		}
		return commitable;
	}

	public void commitUpToCheckpoint(long checkpointId) throws IOException {
		Preconditions.checkNotNull(fsWriter);

		Iterator<Map.Entry<Long, List<ResumableWriter.CommitRecoverable>>> it =
				pendingPerCheckpoint.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<ResumableWriter.CommitRecoverable>> entry = it.next();
			if (entry.getKey() <= checkpointId) {
				for (ResumableWriter.CommitRecoverable commitable : entry.getValue()) {
					fsWriter.recoverForCommit(commitable).commit();
				}
				it.remove();
			}
		}
	}

	public BucketState snapshot(long checkpointId) throws IOException {
		final ResumableWriter.ResumeRecoverable resumable = handler.persist();
		if (!pending.isEmpty()) {
			pendingPerCheckpoint.put(checkpointId, pending);
			pending = new ArrayList<>();
		}
		return new BucketState(bucketPath, handler.getCreationTime(), resumable, pendingPerCheckpoint);
	}

	private Path getNewPartPath() {
		return new Path(bucketPath, PART_PREFIX + '-' + subtaskIndex + '-' + partCounter);
	}
}
