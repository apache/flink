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

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.core.fs.Path;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests the serialization and deserialization for {@link FileWriterBucketState}.
 */
public class FileWriterBucketStateSerializerTest {

	@Test
	public void testWithoutInProgressFile() throws IOException {
		FileWriterBucketState bucketState = new FileWriterBucketState(
				"bucketId",
				new Path("file:///tmp/bucketId"),
				1429537268,
				null);
		FileWriterBucketState deserialized = serializeAndDeserialize(bucketState);
		assertBucketStateEquals(bucketState, deserialized);
	}

	@Test
	public void testWithInProgressFile() throws IOException {
		FileWriterBucketState bucketState = new FileWriterBucketState(
				"bucketId",
				new Path("file:///tmp/bucketId"),
				1429537268,
				new FileSinkTestUtils.TestInProgressFileRecoverable());
		FileWriterBucketState deserialized = serializeAndDeserialize(bucketState);
		assertBucketStateEquals(bucketState, deserialized);
	}

	private void assertBucketStateEquals(
			FileWriterBucketState bucketState,
			FileWriterBucketState deserialized) {
		assertEquals(bucketState.getBucketId(), deserialized.getBucketId());
		assertEquals(bucketState.getBucketPath(), deserialized.getBucketPath());
		assertEquals(
				bucketState.getInProgressFileCreationTime(),
				deserialized.getInProgressFileCreationTime());
		assertEquals(
				bucketState.getInProgressFileRecoverable(),
				deserialized.getInProgressFileRecoverable());
	}

	private FileWriterBucketState serializeAndDeserialize(FileWriterBucketState bucketState) throws IOException {
		FileWriterBucketStateSerializer serializer = new FileWriterBucketStateSerializer(
				new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
						FileSinkTestUtils.TestInProgressFileRecoverable::new),
				new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
						FileSinkTestUtils.TestPendingFileRecoverable::new));
		byte[] data = serializer.serialize(bucketState);
		return serializer.deserialize(serializer.getVersion(), data);
	}

}
