/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.StringUtils;

public class BufferSpiller {
	
	/** The random number generator for temp file names */
	private static final Random RND = new Random();

	/** The counter that selects the next directory to spill into */
	private static final AtomicInteger DIRECTORY_INDEX = new AtomicInteger(0);
	
	
	/** The directories to spill to */
	private final File tempDir;

	private File spillFile;
	
	private FileChannel spillingChannel;
	
	

	public BufferSpiller(IOManager ioManager) throws IOException {
		File[] tempDirs = ioManager.getSpillingDirectories();
		this.tempDir = tempDirs[DIRECTORY_INDEX.getAndIncrement() % tempDirs.length];
		createSpillingChannel();
	}

	/**
	 * Dumps the contents of the buffer to disk and recycles the buffer.
	 */
	public void spill(Buffer buffer) throws IOException {
		try {
			spillingChannel.write(buffer.getNioBuffer());
			buffer.recycle();
		}
		catch (IOException e) {
			close();
			throw e;
		}
	}

	@SuppressWarnings("resource")
	private void createSpillingChannel() throws IOException {
		this.spillFile = new File(tempDir, randomString(RND) + ".buffer");
		this.spillingChannel = new RandomAccessFile(spillFile, "rw").getChannel();
	}



	public void close() throws IOException {
		if (spillingChannel != null && spillingChannel.isOpen()) {
			spillingChannel.close();
		}
	}

	public void resetSpillFile() throws IOException {
		close();
		createSpillingChannel();
	}

	public File getSpillFile() {
		return spillFile;
	}
	
	// ------------------------------------------------------------------------

	private static String randomString(Random random) {
		final byte[] bytes = new byte[20];
		random.nextBytes(bytes);
		return StringUtils.byteToHexString(bytes);
	}
}
