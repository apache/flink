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

package org.apache.flink.formats.compress.writers;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.formats.compress.extractor.Extractor;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link BulkWriter} implementation that does not compress data. This is essentially a no-op
 * writer for use with {@link org.apache.flink.formats.compress.CompressWriterFactory} for the case
 * that no compression codec is specified.
 *
 * @param <T> The type of element to write.
 */
public class NoCompressionBulkWriter<T> implements BulkWriter<T> {

	private final Extractor<T> extractor;
	private final FSDataOutputStream outputStream;

	public NoCompressionBulkWriter(FSDataOutputStream outputStream, Extractor<T> extractor) {
		this.outputStream = checkNotNull(outputStream);
		this.extractor = checkNotNull(extractor);
	}

	@Override
	public void addElement(T element) throws IOException {
		outputStream.write(extractor.extract(element));
	}

	@Override
	public void flush() throws IOException {
		outputStream.flush();
	}

	@Override
	public void finish() throws IOException {
		outputStream.sync();
	}
}
