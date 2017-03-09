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

package org.apache.flink.runtime.io.network.partition;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Writer for writing buffer to DFS file.
 */
public class DFSFileWriter {
	private static final long defaultFlushLength = HadoopFileSystem.getHadoopConfiguration().getLong("dfs.flush.size", 64 *  1024 * 1024);

	private static final int replica = HadoopFileSystem.getHadoopConfiguration().getInt("dfs.file.replica", 2);

	private static final Logger LOG = LoggerFactory.getLogger(DFSFileWriter.class);

	/** DFS output stream. */
	private FSDataOutputStream outputStream;

	/** length for deciding when to call flush. */
	private long unFlushedLen;

	public DFSFileWriter(String filename) {
		try {
			FileSystem fs = FileSystem.get(new URI("hdfs:/tmp"), HadoopFileSystem.getHadoopConfiguration());
			if (fs == null) {
				LOG.warn("Fail to get HadoopFileSystem");
			}
			outputStream = fs.create(new Path(filename), (short)replica);
			LOG.info("Create file(" + filename + ") as result file.");
		}
		catch (Exception e) {
			LOG.warn("Fail to create file due to " + e.getMessage());
			outputStream = null;
		}
		unFlushedLen = 0;
	}


	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * writer buffer to dfs file
	 */
	void write(Buffer buffer) throws IOException {
		if (outputStream == null) {
			throw new IOException("OutputStream is not successfully created.");
		}

		// first write a header and then data
		outputStream.writeInt(buffer.isBuffer() ? 1 : 0);
		outputStream.writeInt(buffer.getSize());
		outputStream.write(buffer.getNioBuffer().array(), 0, buffer.getSize());
		unFlushedLen += buffer.getSize();

		// automatically call flush per 64M
		if (unFlushedLen  > defaultFlushLength) {
			outputStream.flush();
			unFlushedLen = 0;
		}
	}

	void close() throws IOException {
		outputStream.close();
	}

	void flush() throws IOException {
		outputStream.flush();
	}
}
