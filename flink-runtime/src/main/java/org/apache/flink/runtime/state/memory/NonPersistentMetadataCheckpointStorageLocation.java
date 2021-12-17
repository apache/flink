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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.util.UUID;

/**
 * A checkpoint storage location for the {@link MemoryStateBackend} in case no durable persistence
 * for metadata has been configured.
 */
public class NonPersistentMetadataCheckpointStorageLocation extends MemCheckpointStreamFactory
        implements CheckpointStorageLocation {

    /** The external pointer returned for checkpoints that are not externally addressable. */
    public static final String EXTERNAL_POINTER = "<checkpoint-not-externally-addressable>";

    public NonPersistentMetadataCheckpointStorageLocation(int maxStateSize) {
        super(maxStateSize);
    }

    @Override
    public CheckpointMetadataOutputStream createMetadataOutputStream() throws IOException {
        return new MetadataOutputStream();
    }

    @Override
    public void disposeOnFailure() {}

    @Override
    public CheckpointStorageLocationReference getLocationReference() {
        return CheckpointStorageLocationReference.getDefault();
    }

    // ------------------------------------------------------------------------
    //  CompletedCheckpointStorageLocation
    // ------------------------------------------------------------------------

    /**
     * A {@link CompletedCheckpointStorageLocation} that is not persistent and only holds the
     * metadata in an internal byte array.
     */
    private static class NonPersistentCompletedCheckpointStorageLocation
            implements CompletedCheckpointStorageLocation {

        private static final long serialVersionUID = 1L;

        private final ByteStreamStateHandle metaDataHandle;

        NonPersistentCompletedCheckpointStorageLocation(ByteStreamStateHandle metaDataHandle) {
            this.metaDataHandle = metaDataHandle;
        }

        @Override
        public String getExternalPointer() {
            return EXTERNAL_POINTER;
        }

        @Override
        public StreamStateHandle getMetadataHandle() {
            return metaDataHandle;
        }

        @Override
        public void disposeStorageLocation() {}
    }

    // ------------------------------------------------------------------------
    //  CheckpointMetadataOutputStream
    // ------------------------------------------------------------------------

    private static class MetadataOutputStream extends CheckpointMetadataOutputStream {

        private final ByteArrayOutputStreamWithPos os = new ByteArrayOutputStreamWithPos();

        private boolean closed;

        @Override
        public void write(int b) throws IOException {
            os.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            os.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            os.flush();
        }

        @Override
        public long getPos() throws IOException {
            return os.getPosition();
        }

        @Override
        public void sync() throws IOException {}

        @Override
        public CompletedCheckpointStorageLocation closeAndFinalizeCheckpoint() throws IOException {
            synchronized (this) {
                if (!closed) {
                    closed = true;

                    byte[] bytes = os.toByteArray();
                    ByteStreamStateHandle handle =
                            new ByteStreamStateHandle(UUID.randomUUID().toString(), bytes);
                    return new NonPersistentCompletedCheckpointStorageLocation(handle);
                } else {
                    throw new IOException("Already closed");
                }
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                os.reset();
            }
        }
    }
}
