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

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;

import java.io.IOException;

/**
 * The {@link CompactReader} to delegate {@link FileInputFormat}.
 */
public class FileInputFormatCompactReader<T> implements CompactReader<T> {

	private final FileInputFormat<T> format;

	private FileInputFormatCompactReader(FileInputFormat<T> format) {
		this.format = format;
	}

	@Override
	public T read() throws IOException {
		if (format.reachedEnd()) {
			return null;
		}
		return format.nextRecord(null);
	}

	@Override
	public void close() throws IOException {
		format.close();
	}

	public static <T> CompactReader.Factory<T> factory(FileInputFormat<T> format) {
		return new Factory<>(format);
	}

	/**
	 * Factory to create {@link FileInputFormatCompactReader}.
	 */
	private static class Factory<T> implements CompactReader.Factory<T> {

		private final FileInputFormat<T> format;

		public Factory(FileInputFormat<T> format) {
			this.format = format;
		}

		@Override
		public CompactReader<T> create(CompactContext context) throws IOException {
			long len = context.getFileSystem().getFileStatus(context.getPath()).getLen();
			format.open(new FileInputSplit(0, context.getPath(), 0, len, null));
			return new FileInputFormatCompactReader<>(format);
		}
	}
}
