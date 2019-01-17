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

package org.apache.flink.core.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests for {@link StreamCompressionDecorator}
 */
public class StreamCompressionTest {

	@Test
	public void testStreamCompression() throws IOException {
		for (CompressionType compressionType : CompressionTypes.values()) {
			StreamCompressionDecorator streamCompressionDecorator = compressionType.getStreamCompressionDecorator();

			int dataSize = ThreadLocalRandom.current().nextInt(1, 1024 * 1024);
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

			try (OutputStream compressedOutputStream = streamCompressionDecorator.decorateWithCompression(outputStream)) {
				for (int i = 0; i < dataSize; i++) {
					compressedOutputStream.write(i % 256);
				}
			}

			ByteArrayInputStream compressedInputStream = new ByteArrayInputStream(outputStream.toByteArray());
			try (InputStream inputStream = streamCompressionDecorator.decorateWithCompression(compressedInputStream)) {
				for (int i = 0; i < dataSize; i++) {
					Assert.assertEquals(i % 256, inputStream.read());
				}
				Assert.assertTrue(inputStream.read() < 0);
			}
		}
	}
}
