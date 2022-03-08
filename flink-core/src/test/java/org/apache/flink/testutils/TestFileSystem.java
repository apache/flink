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

package org.apache.flink.testutils;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalDataOutputStream;
import org.apache.flink.core.fs.local.LocalFileStatus;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test file system. This also has a service entry in the test resources, to be loaded during
 * tests.
 */
public class TestFileSystem extends LocalFileSystem {

    public static final String SCHEME = "test";

    // number of (input) stream opened
    private static final AtomicInteger streamOpenCounter = new AtomicInteger(0);

    // current number of created, unclosed (output) stream
    private static final Map<Path, Integer> currentUnclosedOutputStream = new ConcurrentHashMap<>();

    public static int getNumtimeStreamOpened() {
        return streamOpenCounter.get();
    }

    public static void resetStreamOpenCounter() {
        streamOpenCounter.set(0);
    }

    public static int getNumberOfUnclosedOutputStream(Path path) {
        return currentUnclosedOutputStream.getOrDefault(path, 0);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        streamOpenCounter.incrementAndGet();
        return super.open(f, bufferSize);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        streamOpenCounter.incrementAndGet();
        return super.open(f);
    }

    @Override
    public FSDataOutputStream create(final Path filePath, final WriteMode overwrite)
            throws IOException {
        currentUnclosedOutputStream.compute(filePath, (k, v) -> v == null ? 1 : v + 1);
        LocalDataOutputStream stream = (LocalDataOutputStream) super.create(filePath, overwrite);
        return new TestOutputStream(stream, filePath);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        LocalFileStatus status = (LocalFileStatus) super.getFileStatus(f);
        return new LocalFileStatus(status.getFile(), this);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        FileStatus[] stati = super.listStatus(f);
        LocalFileStatus[] newStati = new LocalFileStatus[stati.length];
        for (int i = 0; i < stati.length; i++) {
            newStati[i] = new LocalFileStatus(((LocalFileStatus) stati[i]).getFile(), this);
        }
        return newStati;
    }

    @Override
    public URI getUri() {
        return URI.create(SCHEME + ":///");
    }

    // ------------------------------------------------------------------------

    private static final class TestOutputStream extends FSDataOutputStream {

        private final LocalDataOutputStream stream;
        private final Path path;

        private TestOutputStream(LocalDataOutputStream stream, Path path) {
            this.stream = stream;
            this.path = path;
        }

        @Override
        public long getPos() throws IOException {
            return stream.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            stream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            stream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        @Override
        public void sync() throws IOException {
            stream.sync();
        }

        @Override
        public void close() throws IOException {
            currentUnclosedOutputStream.compute(
                    path, (k, v) -> Preconditions.checkNotNull(v) == 1 ? null : v - 1);
            stream.close();
        }
    }

    // ------------------------------------------------------------------------

    public static final class TestFileSystemFactory implements FileSystemFactory {

        @Override
        public String getScheme() {
            return SCHEME;
        }

        @Override
        public FileSystem create(URI fsUri) throws IOException {
            return new TestFileSystem();
        }
    }
}
