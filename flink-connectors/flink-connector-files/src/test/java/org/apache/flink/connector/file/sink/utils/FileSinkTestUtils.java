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

package org.apache.flink.connector.file.sink.utils;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.types.StringValue;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utils for tests related to {@link FileSink}.
 */
public class FileSinkTestUtils {
	/**
	 * A type of testing {@link InProgressFileWriter.PendingFileRecoverable}.
	 */
	public static class TestPendingFileRecoverable
			extends StringValue
			implements InProgressFileWriter.PendingFileRecoverable {
		// Nope
	}

	/**
	 * A type of testing {@link InProgressFileWriter.InProgressFileRecoverable}.
	 */
	public static class TestInProgressFileRecoverable
			extends StringValue
			implements InProgressFileWriter.InProgressFileRecoverable {
		// Nope
	}

	/**
	 * The test serializer for the {@link TestPendingFileRecoverable} and {@link TestInProgressFileRecoverable}.
	 */
	public static class SimpleVersionedWrapperSerializer<T>
			implements SimpleVersionedSerializer<T> {

		private final Supplier<T> factory;

		public SimpleVersionedWrapperSerializer(Supplier<T> factory) {
			this.factory = factory;
		}

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(T obj) throws IOException {
			checkState(obj instanceof StringValue, "Only subclass of StringValue is supported");
			return SimpleVersionedStringSerializer.INSTANCE.serialize(((StringValue) obj).getValue());
		}

		@Override
		public T deserialize(int version, byte[] serialized) throws IOException {
			String value = SimpleVersionedStringSerializer.INSTANCE.deserialize(
					SimpleVersionedStringSerializer.INSTANCE.getVersion(),
					serialized);
			T t = factory.get();
			checkState(t instanceof StringValue, "Only subclass of StringValue is supported");
			((StringValue) t).setValue(value);
			return t;
		}
	}

	/**
	 * A simple {@link BucketAssigner} that accepts {@code String}'s
	 * and returns the element itself as the bucket id.
	 */
	public static class StringIdentityBucketAssigner implements BucketAssigner<String, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getBucketId(String element, BucketAssigner.Context context) {
			return element;
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}
}
