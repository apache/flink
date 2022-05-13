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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The serializer for the recoverable writer state. */
class GSResumeRecoverableSerializer implements SimpleVersionedSerializer<GSResumeRecoverable> {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(GSResumeRecoverableSerializer.class);

    /** Current version of serializer. */
    private static final int SERIALIZER_VERSION = 1;

    /** The one and only instance of the serializer. */
    public static final GSResumeRecoverableSerializer INSTANCE =
            new GSResumeRecoverableSerializer();

    private GSResumeRecoverableSerializer() {}

    /**
     * The serializer version. Note that, if the version of {@link GSResumeRecoverableSerializer}
     * changes, this version must also change, because this serializer depends on that serializer.
     *
     * @return The serializer version.
     */
    @Override
    public int getVersion() {
        return SERIALIZER_VERSION;
    }

    @Override
    public byte[] serialize(GSResumeRecoverable recoverable) throws IOException {
        LOGGER.trace("Serializing recoverable {}", recoverable);
        Preconditions.checkNotNull(recoverable);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {

                // start by serializing the commit recoverable part
                GSCommitRecoverableSerializer.serializeCommitRecoverable(
                        recoverable, dataOutputStream);

                // position
                dataOutputStream.writeLong(recoverable.position);

                // closed
                dataOutputStream.writeBoolean(recoverable.closed);
            }

            outputStream.flush();
            return outputStream.toByteArray();
        }
    }

    @Override
    public GSResumeRecoverable deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version > 0);
        Preconditions.checkNotNull(serialized);

        // ensure this serializer can deserialize data with this version
        if (version > SERIALIZER_VERSION) {
            throw new IOException(
                    String.format(
                            "Serialized data with version %d cannot be read by serializer with version %d",
                            version, SERIALIZER_VERSION));
        }

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized)) {

            try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {

                // deserialize the commit recoverable part
                GSCommitRecoverable commitRecoverable =
                        GSCommitRecoverableSerializer.deserializeCommitRecoverable(dataInputStream);

                // position
                long position = dataInputStream.readLong();

                // closed
                boolean closed = dataInputStream.readBoolean();

                GSResumeRecoverable resumeRecoverable =
                        new GSResumeRecoverable(
                                commitRecoverable.finalBlobIdentifier,
                                commitRecoverable.componentObjectIds,
                                position,
                                closed);
                LOGGER.trace("Deserialized resume recoverable {}", resumeRecoverable);
                return resumeRecoverable;
            }
        }
    }
}
