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

package org.apache.flink.fs.gcs.common.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link GcsRecoverableFsDataOutputStream}.
 */
public class GcsRecoverableFsDataOutputStreamTest {

	private Storage storage;

	@Before
	public void beforeTest() throws IOException {
		this.storage = StorageOptions
			.newBuilder()
			.build()
			.getService();
	}

	@Test
	public void simpleUsageTest() throws IOException {
		final String bucket = "fokko-yolo";
		final String object = "simple-usage-test.txt";
		final String path = String.format("gs://%s/%s", bucket, object);
		final String payload = "hello world";

		final GcsRecoverableFsDataOutputStream streamUnderTest =
			new GcsRecoverableFsDataOutputStream(storage, new GcsRecoverable(new Path(path)));
		streamUnderTest.write(bytesOf(payload));

		RecoverableFsDataOutputStream.Committer committer = streamUnderTest.closeForCommit();
		committer.commit();

		assertEquals(readBucketToEnd(bucket, object), payload);
	}

	// ------------------------------------------------------------------------------------------------------------
	// Utils
	// ------------------------------------------------------------------------------------------------------------

	private String readBucketToEnd(String bucket, String object) {
		Blob blob = storage.get(bucket, object);
		return new String(blob.getContent(), StandardCharsets.UTF_8);
	}

	private static byte[] bytesOf(String str) {
		return str.getBytes(StandardCharsets.UTF_8);
	}
}
