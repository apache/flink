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

package org.apache.flink.table.filesystem.stream.compact;

import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;

/**
 * The {@link CompactWriter} to delegate {@link BucketWriter}.
 */
public class CompactBucketWriter<T> implements CompactWriter<T> {

	private final BucketWriter<T, String> bucketWriter;
	private final InProgressFileWriter<T, String> writer;

	private CompactBucketWriter(
			BucketWriter<T, String> bucketWriter,
			InProgressFileWriter<T, String> writer) {
		this.bucketWriter = bucketWriter;
		this.writer = writer;
	}

	@Override
	public void write(T record) throws IOException {
		// The currentTime is useless
		this.writer.write(record, 0);
	}

	@Override
	public void commit() throws IOException {
		bucketWriter.recoverPendingFile(writer.closeForCommit()).commit();
	}

	public static <T> CompactWriter.Factory<T> factory(
			SupplierWithException<BucketWriter<T, String>, IOException> factory) {
		return new Factory<>(factory);
	}

	/**
	 * Factory to create {@link CompactBucketWriter}.
	 */
	private static class Factory<T> implements CompactWriter.Factory<T> {

		private final SupplierWithException<BucketWriter<T, String>, IOException> factory;

		private BucketWriter<T, String> bucketWriter;

		public Factory(SupplierWithException<BucketWriter<T, String>, IOException> factory) {
			this.factory = factory;
		}

		@Override
		public CompactWriter<T> create(CompactContext context) throws IOException {
			// The writer is not Serializable
			if (bucketWriter == null) {
				bucketWriter = factory.get();
			}

			// creationTime are useless
			return new CompactBucketWriter<>(
					bucketWriter, bucketWriter.openNewInProgressFile(
							context.getPartition(), context.getPath(), 0));
		}
	}
}
