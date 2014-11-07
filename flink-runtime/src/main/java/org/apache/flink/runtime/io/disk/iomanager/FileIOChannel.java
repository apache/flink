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

package org.apache.flink.runtime.io.disk.iomanager;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.flink.util.StringUtils;

/**
 * A Channel represents a collection of files that belong logically to the same resource. An example is a collection of
 * files that contain sorted runs of data from the same stream, that will later on be merged together.
 */
public interface FileIOChannel {
	
	/**
	 * Gets the channel ID of this I/O channel.
	 * 
	 * @return The channel ID.
	 */
	FileIOChannel.ID getChannelID();
	
	/**
	 * Checks whether the channel has been closed.
	 * 
	 * @return True if the channel has been closed, false otherwise.
	 */
	boolean isClosed();

	/**
	* Closes the channel. For asynchronous implementations, this method waits until all pending requests are
	* handled. Even if an exception interrupts the closing, the underlying <tt>FileChannel</tt> is closed.
	* 
	* @throws IOException Thrown, if an error occurred while waiting for pending requests.
	*/
	void close() throws IOException;

	/**
	 * Deletes the file underlying this I/O channel.
	 *  
	 * @throws IllegalStateException Thrown, when the channel is still open.
	 */
	void deleteChannel();
	
	/**
	* Closes the channel and deletes the underlying file.
	* For asynchronous implementations, this method waits until all pending requests are handled;
	* 
	* @throws IOException Thrown, if an error occurred while waiting for pending requests.
	*/
	public void closeAndDelete() throws IOException;
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	/**
	 * An ID identifying an underlying file channel.
	 */
	public static class ID {
		
		private static final int RANDOM_BYTES_LENGTH = 16;
		
		private final String path;
		
		private final int threadNum;

		protected ID(String path, int threadNum) {
			this.path = path;
			this.threadNum = threadNum;
		}

		protected ID(String basePath, int threadNum, Random random) {
			this.path = basePath + File.separator + randomString(random) + ".channel";
			this.threadNum = threadNum;
		}

		/**
		 * Returns the path to the underlying temporary file.
		 */
		public String getPath() {
			return path;
		}
		
		int getThreadNum() {
			return this.threadNum;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ID) {
				ID other = (ID) obj;
				return this.path.equals(other.path) && this.threadNum == other.threadNum;
			} else {
				return false;
			}
		}
		
		@Override
		public int hashCode() {
			return path.hashCode();
		}
		
		@Override
		public String toString() {
			return path;
		}
		
		private static final String randomString(final Random random) {
			final byte[] bytes = new byte[RANDOM_BYTES_LENGTH];
			random.nextBytes(bytes);
			return StringUtils.byteToHexString(bytes);
		}
	}

	/**
	 * An enumerator for channels that logically belong together.
	 */
	public static final class Enumerator {
		
		private static final String FORMAT = "%s%s%s.%06d.channel";

		private final String[] paths;
		
		private final String namePrefix;

		private int counter;

		protected Enumerator(String[] basePaths, Random random) {
			this.paths = basePaths;
			this.namePrefix = ID.randomString(random);
			this.counter = 0;
		}

		public ID next() {
			final int threadNum = counter % paths.length;
			return new ID(String.format(FORMAT, this.paths[threadNum], File.separator, namePrefix, (counter++)), threadNum);
		}
	}
}
