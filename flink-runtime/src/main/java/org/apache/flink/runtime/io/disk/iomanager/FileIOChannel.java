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

import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Channel represents a collection of files that belong logically to the same resource. An example
 * is a collection of files that contain sorted runs of data from the same stream, that will later
 * on be merged together.
 */
public interface FileIOChannel {

    /**
     * Gets the channel ID of this I/O channel.
     *
     * @return The channel ID.
     */
    ID getChannelID();

    /** Gets the size (in bytes) of the file underlying the channel. */
    long getSize() throws IOException;

    /**
     * Checks whether the channel has been closed.
     *
     * @return True if the channel has been closed, false otherwise.
     */
    boolean isClosed();

    /**
     * Closes the channel. For asynchronous implementations, this method waits until all pending
     * requests are handled. Even if an exception interrupts the closing, the underlying
     * <tt>FileChannel</tt> is closed.
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

    FileChannel getNioFileChannel();

    /**
     * Closes the channel and deletes the underlying file. For asynchronous implementations, this
     * method waits until all pending requests are handled.
     *
     * @throws IOException Thrown, if an error occurred while waiting for pending requests.
     */
    void closeAndDelete() throws IOException;

    // --------------------------------------------------------------------------------------------
    // --------------------------------------------------------------------------------------------

    /** An ID identifying an underlying file channel. */
    class ID {

        private static final int RANDOM_BYTES_LENGTH = 16;

        private final File path;

        private final int threadNum;

        private ID(File path, int threadNum) {
            this.path = path;
            this.threadNum = threadNum;
        }

        public ID(File basePath, int threadNum, Random random) {
            this.path = new File(basePath, randomString(random) + ".channel");
            this.threadNum = threadNum;
        }

        /** Returns the path to the underlying temporary file. */
        public String getPath() {
            return path.getAbsolutePath();
        }

        /** Returns the path to the underlying temporary file as a File. */
        public File getPathFile() {
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
            return path.getAbsolutePath();
        }

        private static String randomString(Random random) {
            byte[] bytes = new byte[RANDOM_BYTES_LENGTH];
            random.nextBytes(bytes);
            return StringUtils.byteToHexString(bytes);
        }
    }

    /** An enumerator for channels that logically belong together. */
    final class Enumerator {

        private static AtomicInteger globalCounter = new AtomicInteger();

        private final File[] paths;

        private final String namePrefix;

        private int localCounter;

        public Enumerator(File[] basePaths, Random random) {
            this.paths = basePaths;
            this.namePrefix = ID.randomString(random);
            this.localCounter = 0;
        }

        public ID next() {
            // The local counter is used to increment file names while the global counter is used
            // for indexing the directory and associated read and write threads. This performs a
            // round-robin among all spilling operators and avoids I/O bunching.
            int threadNum = globalCounter.getAndIncrement() % paths.length;
            String filename = String.format("%s.%06d.channel", namePrefix, (localCounter++));
            return new ID(new File(paths[threadNum], filename), threadNum);
        }
    }
}
