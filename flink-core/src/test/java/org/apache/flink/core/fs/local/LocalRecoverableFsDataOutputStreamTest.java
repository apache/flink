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

package org.apache.flink.core.fs.local;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/** Unit tests for {@link LocalRecoverableFsDataOutputStream}. */
public class LocalRecoverableFsDataOutputStreamTest
        extends AbstractRecoverableFsDataOutputStreamTest {

    @Override
    public Tuple2<RecoverableFsDataOutputStream, Closeable> createTestInstance(
            Path target, Path temp, List<Event> testLog) throws IOException {
        final FileChannel fileChannel =
                new TestFileChannel(
                        FileChannel.open(
                                temp, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW),
                        testLog);

        final TestOutputStream fos =
                new TestOutputStream(
                        new BufferedOutputStream(Channels.newOutputStream(fileChannel)), testLog);

        // Create test object
        final RecoverableFsDataOutputStream testOutStreamInstance =
                new LocalRecoverableFsDataOutputStream(
                        target.toFile(), temp.toFile(), fileChannel, fos);

        return new Tuple2<>(testOutStreamInstance, fos::actualClose);
    }

    private static class TestOutputStream extends FilterOutputStream {

        private final List<Event> events;

        public TestOutputStream(OutputStream out, List<Event> events) {
            super(out);
            this.events = events;
        }

        @Override
        public void flush() throws IOException {
            super.flush();
            events.add(Event.FLUSH);
        }

        @Override
        public void close() {
            events.add(Event.CLOSE);
            // Do nothing on close.
        }

        public void actualClose() throws IOException {
            super.close();
        }
    }

    static class TestFileChannel extends FileChannel {

        final FileChannel delegate;

        private final List<Event> events;

        TestFileChannel(FileChannel delegate, List<Event> events) {
            this.delegate = delegate;
            this.events = events;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return delegate.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return delegate.read(dsts, offset, length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return delegate.write(src);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return delegate.write(srcs, offset, length);
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            return delegate.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            return delegate.truncate(size);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            delegate.force(metaData);
            events.add(Event.SYNC);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target)
                throws IOException {
            return delegate.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count)
                throws IOException {
            return delegate.transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return delegate.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            return delegate.write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return delegate.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return delegate.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return delegate.tryLock(position, size, shared);
        }

        @Override
        protected void implCloseChannel() {}
    }
}
