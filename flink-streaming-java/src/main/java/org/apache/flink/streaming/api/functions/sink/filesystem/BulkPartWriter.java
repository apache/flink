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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link PartFileWriter} for bulk-encoding formats that use an {@link BulkPartWriter}.
 * This also implements the {@link PartFileInfo}.
 */
@Internal
final class BulkPartWriter<IN, BucketID> extends PartFileWriter<IN, BucketID> {

	private final BulkWriter<IN> writer;

	private BulkPartWriter(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final BulkWriter<IN> writer,
			final long creationTime) {
		super(bucketId, currentPartStream, creationTime);
		this.writer = Preconditions.checkNotNull(writer);
	}

	@Override
	void write(IN element, long currentTime) throws IOException {
		writer.addElement(element);
		markWrite(currentTime);
	}

	@Override
	RecoverableWriter.ResumeRecoverable persist() {
		throw new UnsupportedOperationException("Bulk Part Writers do not support \"pause and resume\" operations.");
	}

	@Override
	RecoverableWriter.CommitRecoverable closeForCommit() throws IOException {
		writer.flush();
		writer.finish();
		return super.closeForCommit();
	}

	/**
	 * A factory that creates {@link BulkPartWriter BulkPartWriters}.
	 * @param <IN> The type of input elements.
	 * @param <BucketID> The type of ids for the buckets, as returned by the {@link BucketAssigner}.
	 */
	static class Factory<IN, BucketID> implements PartFileWriter.PartFileFactory<IN, BucketID> {

		private final BulkWriter.Factory<IN> writerFactory;

		Factory(BulkWriter.Factory<IN> writerFactory) {
			this.writerFactory = writerFactory;
		}

		@Override
		public PartFileWriter<IN, BucketID> resumeFrom(
				final BucketID bucketId,
				final RecoverableFsDataOutputStream stream,
				final RecoverableWriter.ResumeRecoverable resumable,
				final long creationTime) throws IOException {

			Preconditions.checkNotNull(stream);
			Preconditions.checkNotNull(resumable);

			final BulkWriter<IN> writer = writerFactory.create(stream);
			return new BulkPartWriter<>(bucketId, stream, writer, creationTime);
		}

		@Override
		public PartFileWriter<IN, BucketID> openNew(
				final BucketID bucketId,
				final RecoverableFsDataOutputStream stream,
				final Path path,
				final long creationTime) throws IOException {

			Preconditions.checkNotNull(stream);
			Preconditions.checkNotNull(path);

			final BulkWriter<IN> writer = writerFactory.create(stream);
			return new BulkPartWriter<>(bucketId, stream, writer, creationTime);
		}
	}
}
