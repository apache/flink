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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Data object to commit an S3 MultiPartUpload.
 */
public final class S3Committer implements RecoverableFsDataOutputStream.Committer {

	private static final Logger LOG = LoggerFactory.getLogger(S3Committer.class);

	private final S3AccessHelper s3AccessHelper;

	private final String uploadId;

	private final String objectName;

	private final List<PartETag> parts;

	private final long totalLength;

	S3Committer(S3AccessHelper s3AccessHelper, String objectName, String uploadId, List<PartETag> parts, long totalLength) {
		this.s3AccessHelper = checkNotNull(s3AccessHelper);
		this.objectName = checkNotNull(objectName);
		this.uploadId = checkNotNull(uploadId);
		this.parts = checkNotNull(parts);
		this.totalLength = totalLength;
	}

	@Override
	public void commit() throws IOException {
		if (totalLength > 0L) {
			LOG.info("Committing {} with MPU ID {}", objectName, uploadId);

			final AtomicInteger errorCount = new AtomicInteger();
			s3AccessHelper.commitMultiPartUpload(objectName, uploadId, parts, totalLength, errorCount);

			if (errorCount.get() == 0) {
				LOG.debug("Successfully committed {} with MPU ID {}", objectName, uploadId);
			} else {
				LOG.debug("Successfully committed {} with MPU ID {} after {} retries.", objectName, uploadId, errorCount.get());
			}
		} else {
			LOG.debug("No data to commit for file: {}", objectName);
		}
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		if (totalLength > 0L) {
			LOG.info("Trying to commit after recovery {} with MPU ID {}", objectName, uploadId);

			try {
				s3AccessHelper.commitMultiPartUpload(objectName, uploadId, parts, totalLength, new AtomicInteger());
			} catch (IOException e) {
				LOG.info("Failed to commit after recovery {} with MPU ID {}. " +
						"Checking if file was committed before...", objectName, uploadId);
				LOG.trace("Exception when committing:", e);

				try {
					ObjectMetadata metadata = s3AccessHelper.getObjectMetadata(objectName);
					if (totalLength != metadata.getContentLength()) {
						String message = String.format("Inconsistent result for object %s: conflicting lengths. " +
										"Recovered committer for upload %s indicates %s bytes, present object is %s bytes",
								objectName, uploadId, totalLength, metadata.getContentLength());
						LOG.warn(message);
						throw new IOException(message, e);
					}
				} catch (FileNotFoundException fnf) {
					LOG.warn("Object {} not existing after failed recovery commit with MPU ID {}", objectName, uploadId);
					throw new IOException(String.format("Recovering commit failed for object %s. " +
							"Object does not exist and MultiPart Upload %s is not valid.", objectName, uploadId), e);
				}
			}
		} else {
			LOG.debug("No data to commit for file: {}", objectName);
		}
	}

	@Override
	public RecoverableWriter.CommitRecoverable getRecoverable() {
		return new S3Recoverable(objectName, uploadId, parts, totalLength);
	}
}
