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

import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The default implementation of the recoverable writer helper, which uses Google Storage directly.
 */
public class DefaultGSRecoverableWriterHelper extends GSRecoverableWriterHelper {

    private final Storage storage;

    /**
     * Constructs a recoverable writer helper.
     *
     * @param storage The {@link com.google.cloud.storage.Storage} instance
     */
    public DefaultGSRecoverableWriterHelper(com.google.cloud.storage.Storage storage) {
        this.storage = new Storage(storage);
    }

    @Override
    public GSRecoverableWriterHelper.Storage getStorage() {
        return storage;
    }

    /**
     * Implements the Storage interface by delegating calls to a true {@link
     * com.google.cloud.storage.Storage} instance.
     */
    static class Storage implements GSRecoverableWriterHelper.Storage {

        private final com.google.cloud.storage.Storage storage;

        /**
         * Constructs the storage wrapper.
         *
         * @param storage The underlying Google {@link com.google.cloud.storage.Storage} instance
         */
        public Storage(com.google.cloud.storage.Storage storage) {
            this.storage = storage;
        }

        @Override
        public GSRecoverableWriterHelper.Writer createWriter(
                BlobId blobId, String contentType, int chunkSize) {

            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build();
            WriteChannel writeChannel = storage.writer(blobInfo);
            if (chunkSize > 0) {
                writeChannel.setChunkSize(chunkSize);
            }
            return new Writer(writeChannel);
        }

        @Override
        public boolean exists(BlobId blobId) {
            Blob blob = storage.get(blobId);
            return blob != null;
        }

        @Override
        public void copy(BlobId source, BlobId target) {
            storage.get(source).copyTo(target).getResult();
        }

        @Override
        public boolean delete(BlobId blobId) {
            return storage.delete(blobId);
        }
    }

    /**
     * Implements the Writer interface by delegating calls to a true {@link
     * com.google.cloud.WriteChannel} instance.
     */
    static class Writer implements GSRecoverableWriterHelper.Writer {

        private final WriteChannel writeChannel;

        /**
         * Constructs the Writer wrapper.
         *
         * @param writeChannel The underlying {@link com.google.cloud.WriteChannel} instance
         */
        public Writer(WriteChannel writeChannel) {
            this.writeChannel = writeChannel;
        }

        @Override
        public int write(ByteBuffer byteBuffer) throws IOException {
            return writeChannel.write(byteBuffer);
        }

        @Override
        public void close() throws IOException {
            writeChannel.close();
        }

        @Override
        public GSRecoverableWriterHelper.WriterState capture() {
            RestorableState<WriteChannel> restorableState = writeChannel.capture();
            return new WriterState(restorableState);
        }
    }

    /**
     * Implements the WriterState interface by delegating calls to a true {@link
     * com.google.cloud.RestorableState} instance.
     *
     * <p>This is serializable because the RestorableState it wraps is serializable, and this is the
     * only way to serialize/deserialize it.
     */
    static class WriterState implements GSRecoverableWriterHelper.WriterState {

        private static final long serialVersionUID = 1L;

        private final RestorableState<WriteChannel> restorableState;

        /**
         * Constructs the WriterState wrapper.
         *
         * @param restorableState The {@link com.google.cloud.RestorableState} instance
         */
        public WriterState(RestorableState<WriteChannel> restorableState) {
            this.restorableState = restorableState;
        }

        @Override
        public GSRecoverableWriterHelper.Writer restore() {
            WriteChannel writeChannel = restorableState.restore();
            return new Writer(writeChannel);
        }
    }
}
