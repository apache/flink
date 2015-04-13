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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.StringUtils;

public class BufferSpiller {

	protected static Random rnd = new Random();

	private File spillFile;
	protected FileChannel spillingChannel;
	private String tempDir;

	public BufferSpiller() throws IOException {
		String tempDirString = GlobalConfiguration.getString(
				ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
				ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		String[] tempDirs = tempDirString.split(",|" + File.pathSeparator);

		tempDir = tempDirs[rnd.nextInt(tempDirs.length)];

		createSpillingChannel();
	}

	/**
	 * Dumps the contents of the buffer to disk and recycles the buffer.
	 */
	public void spill(Buffer buffer) throws IOException {
		try {
			spillingChannel.write(buffer.getNioBuffer());
			buffer.recycle();
		} catch (IOException e) {
			close();
			throw new IOException(e);
		}

	}

	@SuppressWarnings("resource")
	private void createSpillingChannel() throws IOException {
		this.spillFile = new File(tempDir, randomString(rnd) + ".buffer");
		this.spillingChannel = new RandomAccessFile(spillFile, "rw").getChannel();
	}

	private static String randomString(Random random) {
		final byte[] bytes = new byte[20];
		random.nextBytes(bytes);
		return StringUtils.byteToHexString(bytes);
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

}
