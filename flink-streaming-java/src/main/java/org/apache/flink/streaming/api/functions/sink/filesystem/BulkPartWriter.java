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
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A {@link InProgressFileWriter} for bulk-encoding formats that use an {@link BulkPartWriter}.
 * This also implements the {@link PartFileInfo}.
 */
@Internal
final class BulkPartWriter<IN, BucketID> extends OutputStreamBasedPartFileWriter<IN, BucketID>  {

	private final BulkWriter<IN> writer;

	BulkPartWriter(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream currentPartStream,
			final BulkWriter<IN> writer,
			final long creationTime) {
		super(bucketId, currentPartStream, creationTime);
		this.writer = Preconditions.checkNotNull(writer);
	}

	@Override
	public void write(IN element, long currentTime) throws IOException {
		writer.addElement(element);
		markWrite(currentTime);
	}

	@Override
	public InProgressFileRecoverable persist() {
		throw new UnsupportedOperationException("Bulk Part Writers do not support \"pause and resume\" operations.");
	}

	@Override
	public PendingFileRecoverable closeForCommit() throws IOException {
		writer.flush();
		writer.finish();
		return super.closeForCommit();
	}
}
