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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A {@link HadoopPathBasedBulkWriter.Factory} implementation used in tests.
 */
public class TestHadoopPathBasedBulkWriterFactory implements HadoopPathBasedBulkWriter.Factory<String> {

	@Override
	public HadoopPathBasedBulkWriter<String> create(Path targetFilePath, Path inProgressFilePath) {
		try {
			FileSystem fileSystem = FileSystem.get(inProgressFilePath.toUri(), new Configuration());
			FSDataOutputStream output = fileSystem.create(inProgressFilePath);
			return new FSDataOutputStreamBulkWriterHadoop(output);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}

		return null;
	}

	private static class FSDataOutputStreamBulkWriterHadoop implements HadoopPathBasedBulkWriter<String> {
		private final FSDataOutputStream outputStream;

		public FSDataOutputStreamBulkWriterHadoop(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public long getSize() throws IOException {
			return outputStream.getPos();
		}

		@Override
		public void dispose() {
			IOUtils.closeQuietly(outputStream);
		}

		@Override
		public void addElement(String element) throws IOException {
			outputStream.writeBytes(element + "\n");
		}

		@Override
		public void flush() throws IOException {
			outputStream.flush();
		}

		@Override
		public void finish() throws IOException {
			outputStream.flush();
			outputStream.close();
		}
	}
}
