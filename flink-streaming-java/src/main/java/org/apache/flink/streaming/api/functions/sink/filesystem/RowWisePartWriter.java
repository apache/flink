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
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketers.Bucketer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link PartFileWriter} for row-wise formats that use an {@link Encoder}.
 * This also implements the {@link PartFileInfo}.
 */
@Internal
final class RowWisePartWriter<IN, BucketID> extends PartFileWriter<IN, BucketID> {

	private final Encoder<IN> encoder;

	private RowWisePartWriter(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final Encoder<IN> encoder,
			final long creationTime) {
		super(bucketId, currentPartStream, creationTime);
		this.encoder = Preconditions.checkNotNull(encoder);
	}

	@Override
	void write(IN element, long currentTime) throws IOException {
		encoder.encode(element, currentPartStream);
		markWrite(currentTime);
	}

	/**
	 * A factory that creates {@link RowWisePartWriter RowWisePartWriters}.
	 * @param <IN> The type of input elements.
	 * @param <BucketID> The type of ids for the buckets, as returned by the {@link Bucketer}.
	 */
	static class Factory<IN, BucketID> implements PartFileWriter.PartFileFactory<IN, BucketID> {

		private final Encoder<IN> encoder;

		Factory(Encoder<IN> encoder) {
			this.encoder = encoder;
		}

		@Override
		public PartFileWriter<IN, BucketID> resumeFrom(
				final BucketID bucketId,
				final RecoverableWriter fileSystemWriter,
				final RecoverableWriter.ResumeRecoverable resumable,
				final long creationTime) throws IOException {

			Preconditions.checkNotNull(fileSystemWriter);
			Preconditions.checkNotNull(resumable);

			final RecoverableFsDataOutputStream stream = fileSystemWriter.recover(resumable);
			return new RowWisePartWriter<>(bucketId, stream, encoder, creationTime);
		}

		@Override
		public PartFileWriter<IN, BucketID> openNew(
				final BucketID bucketId,
				final RecoverableWriter fileSystemWriter,
				final Path path,
				final long creationTime) throws IOException {

			Preconditions.checkNotNull(fileSystemWriter);
			Preconditions.checkNotNull(path);

			final RecoverableFsDataOutputStream stream = fileSystemWriter.open(path);
			return new RowWisePartWriter<>(bucketId, stream, encoder, creationTime);
		}
	}
}
