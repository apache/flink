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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.EOFException;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * A DFS file reader, which read data from a dfs file.
 */
public class DFSFileReader {

	private static final Logger LOG = LoggerFactory.getLogger(DFSFileReader.class);

	/** DFS input stream. */
	private FSDataInputStream input;

	/**
	 * Flag indicating whether the dfs file is opened for read.
	 */
	private boolean isOpen;

	private DFSInputChannel channel;

	/**
	 * read thread which read data from dfs file and put data into DFSInputChannel's buffer.
	 */
	private DFSReadThread readThread;

	/**
	 * the path of the dfs file
	 */
	private String path;

	public DFSFileReader(DFSInputChannel channel, String path) {
		this.channel = channel;
		this.path = path;
	}


	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a subpartition, begin to read the dfs file.
	 */
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (!isOpen) {
			// Create a input stream and start the read thread
			FileSystem fs = null;
			try {
				fs = FileSystem.get(new URI("hdfs:/tmp"), HadoopFileSystem.getHadoopConfiguration());
			}
			catch (Exception e) {
				throw new InterruptedException("Open dfs file read fail " + e.getMessage());
			}
			String filename = path + "/" + subpartitionIndex;
			input = fs.open(new Path(filename));
			isOpen = true;
			readThread = new DFSReadThread(input, channel);
			readThread.setDaemon(true);
			readThread.start();
			LOG.info("Begin to read from dfs file(" + filename + ").");
		}
	}

	/**
	 * Retriggers a remote subpartition request.
	 */
	void requestSubpartition(final int subpartitionIndex,
									final DFSInputChannel inputChannel,
									int delayMs) throws IOException, InterruptedException {
		//do nothing
	}

	public void close() {
		try {
			readThread.shutdown();
			readThread.join();
		}
		catch (InterruptedException e) {
			LOG.warn("Read thread interrupt exception: " + e.getMessage());
		}
	}

	public boolean isOpen() {
		return isOpen;
	}

	/**
	 * read thread, for reading data from dfs file
	 */
	public static class DFSReadThread extends Thread {

		private final FSDataInputStream inputStream;

		private final DFSInputChannel inputChannel;

		private volatile boolean alive;

		public DFSReadThread(FSDataInputStream inputStream, DFSInputChannel inputChannel) {
			this.inputStream = inputStream;
			this.inputChannel = inputChannel;
			this.alive = true;
		}

		public void shutdown() {
			this.alive = false;
		}

		@Override
		public void run() {
			ByteBuffer header = ByteBuffer.allocateDirect(8);
			BufferProvider bufferProvider = inputChannel.getBufferProvider();
			if (bufferProvider == null) {
				return;
			}
			LOG.debug("dfs read thread start.");
			while(this.alive) {
				try {
					//first read the header, and then data
					boolean isBuffer = inputStream.readInt() == 1 ? true : false;
					int size = inputStream.readInt();
					if (size > 0)
					{
						Buffer buffer;
						if (!isBuffer) {
							byte[] byteArray = new byte[size];
							inputStream.readFully(byteArray, 0, size);

							MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
							buffer = new Buffer(memSeg, FreeingBufferRecycler.INSTANCE, false);
						}
						else {
							buffer = bufferProvider.requestBuffer();
							if (buffer != null) {
								buffer.setSize(size);
							}
							inputStream.readFully(buffer.getNioBuffer().array(), 0, size);
						}

						inputChannel.onBuffer(buffer);
					}
					else {
						continue;
					}
				}
				catch (EOFException e) {
					LOG.debug("dfs read reach end.");
					break;
				}
				catch (IOException e) {
					LOG.warn("dfs read thread fail " + e.getMessage());
					inputChannel.onError(e);
					break;
				}
				catch (Exception e) {
					LOG.warn("dfs read thread fail with " + e.getClass().getName());
					inputChannel.onError(e);
					break;
				}
			}
			LOG.debug("dfs read thread stopped.");
		}
	}
}
