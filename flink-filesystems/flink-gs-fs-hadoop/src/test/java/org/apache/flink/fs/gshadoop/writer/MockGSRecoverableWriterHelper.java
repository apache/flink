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

package org.apache.flink.fs.gshadoop.writer;

import com.google.cloud.storage.BlobId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Mocked implementation of the recoverable writer adapter, which tracks files as maps of {@link
 * org.apache.flink.fs.gshadoop.writer.MockGSBlob} instances and enables test assertions.
 */
public class MockGSRecoverableWriterHelper extends GSRecoverableWriterHelper {

    private final Storage storage;

    public int maxWriteBytes;

    Map<BlobId, MockGSBlob> blobs;

    public MockGSRecoverableWriterHelper() {
        this.storage = new Storage(this);
        this.maxWriteBytes = Integer.MAX_VALUE;
        this.blobs = new HashMap<>();
    }

    @Override
    public Storage getStorage() {
        return storage;
    }

    MockGSBlob getBlob(BlobId blobId) {
        return blobs.computeIfAbsent(blobId, k -> new MockGSBlob());
    }

    static class Child {

        protected final transient MockGSRecoverableWriterHelper parent;

        protected Child(MockGSRecoverableWriterHelper parent) {
            this.parent = parent;
        }

        // for deserialization of WriteState
        protected Child() {
            this(null);
        }
    }

    static class Storage extends Child implements GSRecoverableWriterHelper.Storage {

        Storage(MockGSRecoverableWriterHelper parent) {
            super(parent);
        }

        @Override
        public GSRecoverableWriterHelper.Writer createWriter(
                BlobId blobId, String contentType, int chunkSize) {

            // force creation of the blob
            parent.getBlob(blobId);

            return new Writer(parent, blobId, 0);
        }

        @Override
        public boolean exists(BlobId blobId) {
            return parent.blobs.containsKey(blobId);
        }

        @Override
        public void copy(BlobId sourceBlobId, BlobId targetBlobId) {
            MockGSBlob blob = parent.blobs.get(sourceBlobId);
            if (blob == null) {
                throw new RuntimeException();
            }
            parent.blobs.put(targetBlobId, blob);
        }

        @Override
        public boolean delete(BlobId blobId) {
            Object removedBlob = parent.blobs.remove(blobId);
            return removedBlob != null;
        }
    }

    static class Writer extends Child implements GSRecoverableWriterHelper.Writer {

        private final BlobId blobId;

        private int position;

        Writer(MockGSRecoverableWriterHelper parent, BlobId blobId, int position) {
            super(parent);
            this.blobId = blobId;
            this.position = position;
        }

        @Override
        public int write(ByteBuffer byteBuffer) throws IOException {
            MockGSBlob blob = parent.getBlob(blobId);
            blob.setPosition(position);

            int writeCount = Math.min(byteBuffer.remaining(), parent.maxWriteBytes);
            blob.write(byteBuffer.array(), byteBuffer.position(), writeCount);

            position += writeCount;
            return writeCount;
        }

        @Override
        public void close() throws IOException {
            parent.getBlob(blobId).close();
        }

        @Override
        public WriterState capture() {
            return new WriteState(parent, blobId, position);
        }
    }

    static class WriteState extends Child implements GSRecoverableWriterHelper.WriterState {

        private static final long serialVersionUID = 1L;

        private final BlobId blobId;

        private final int position;

        WriteState(MockGSRecoverableWriterHelper parent, BlobId blobId, int position) {
            super(parent);
            this.blobId = blobId;
            this.position = position;
        }

        @Override
        public GSRecoverableWriterHelper.Writer restore() {
            return new Writer(parent, blobId, position);
        }
    }
}
