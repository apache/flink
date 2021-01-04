/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code SimpleVersionedSerializer} used to serialize the {@link FileWriterBucketState
 * BucketState}.
 */
@Internal
public class FileWriterBucketStateSerializer
        implements SimpleVersionedSerializer<FileWriterBucketState> {

    private static final int MAGIC_NUMBER = 0x1e764b79;

    private final SimpleVersionedSerializer<InProgressFileRecoverable>
            inProgressFileRecoverableSerializer;

    private final SimpleVersionedSerializer<PendingFileRecoverable>
            pendingFileRecoverableSerializer;

    public FileWriterBucketStateSerializer(
            SimpleVersionedSerializer<InProgressFileRecoverable>
                    inProgressFileRecoverableSerializer,
            SimpleVersionedSerializer<PendingFileRecoverable> pendingFileRecoverableSerializer) {
        this.inProgressFileRecoverableSerializer =
                checkNotNull(inProgressFileRecoverableSerializer);
        this.pendingFileRecoverableSerializer = checkNotNull(pendingFileRecoverableSerializer);
    }

    @Override
    public int getVersion() {
        return 3;
    }

    @Override
    public byte[] serialize(FileWriterBucketState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV3(state, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public FileWriterBucketState deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            case 2:
                validateMagicNumber(in);
                return deserializeV2(in);
            case 3:
                validateMagicNumber(in);
                return deserializeV3(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV3(FileWriterBucketState state, DataOutputView dataOutputView)
            throws IOException {
        SimpleVersionedSerialization.writeVersionAndSerialize(
                SimpleVersionedStringSerializer.INSTANCE, state.getBucketId(), dataOutputView);
        dataOutputView.writeUTF(state.getBucketPath().toString());
        dataOutputView.writeLong(state.getInProgressFileCreationTime());

        // put the current open part file
        if (state.hasInProgressFileRecoverable()) {
            InProgressFileRecoverable inProgressFileRecoverable =
                    state.getInProgressFileRecoverable();
            dataOutputView.writeBoolean(true);
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    inProgressFileRecoverableSerializer, inProgressFileRecoverable, dataOutputView);
        } else {
            dataOutputView.writeBoolean(false);
        }
    }

    private FileWriterBucketState deserializeV1(DataInputView in) throws IOException {
        final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer =
                getCommitableSerializer();
        final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer =
                getResumableSerializer();

        return internalDeserialize(
                in,
                dataInputView ->
                        new OutputStreamBasedPartFileWriter
                                .OutputStreamBasedInProgressFileRecoverable(
                                SimpleVersionedSerialization.readVersionAndDeSerialize(
                                        resumableSerializer, dataInputView)),
                (version, bytes) ->
                        new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable(
                                commitableSerializer.deserialize(version, bytes)));
    }

    private FileWriterBucketState deserializeV2(DataInputView in) throws IOException {
        return internalDeserialize(
                in,
                dataInputView ->
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                inProgressFileRecoverableSerializer, dataInputView),
                pendingFileRecoverableSerializer::deserialize);
    }

    private FileWriterBucketState deserializeV3(DataInputView in) throws IOException {
        return internalDeserialize(
                in,
                dataInputView ->
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                inProgressFileRecoverableSerializer, dataInputView),
                null);
    }

    private FileWriterBucketState internalDeserialize(
            DataInputView dataInputView,
            FunctionWithException<DataInputView, InProgressFileRecoverable, IOException>
                    inProgressFileParser,
            @Nullable
                    BiFunctionWithException<Integer, byte[], PendingFileRecoverable, IOException>
                            pendingFileParser)
            throws IOException {

        String bucketId =
                SimpleVersionedSerialization.readVersionAndDeSerialize(
                        SimpleVersionedStringSerializer.INSTANCE, dataInputView);
        String bucketPathStr = dataInputView.readUTF();
        long creationTime = dataInputView.readLong();

        // then get the current resumable stream
        InProgressFileRecoverable current = null;
        if (dataInputView.readBoolean()) {
            current = inProgressFileParser.apply(dataInputView);
        }

        HashMap<Long, List<InProgressFileWriter.PendingFileRecoverable>>
                pendingFileRecoverablesPerCheckpoint = new HashMap<>();
        if (pendingFileParser != null) {
            final int pendingFileRecoverableSerializerVersion = dataInputView.readInt();
            final int numCheckpoints = dataInputView.readInt();

            for (int i = 0; i < numCheckpoints; i++) {
                final long checkpointId = dataInputView.readLong();
                final int numOfPendingFileRecoverables = dataInputView.readInt();

                final List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables =
                        new ArrayList<>(numOfPendingFileRecoverables);
                for (int j = 0; j < numOfPendingFileRecoverables; j++) {
                    final byte[] bytes = new byte[dataInputView.readInt()];
                    dataInputView.readFully(bytes);
                    pendingFileRecoverables.add(
                            pendingFileParser.apply(
                                    pendingFileRecoverableSerializerVersion, bytes));
                }
                pendingFileRecoverablesPerCheckpoint.put(checkpointId, pendingFileRecoverables);
            }
        }

        return new FileWriterBucketState(
                bucketId,
                new Path(bucketPathStr),
                creationTime,
                current,
                pendingFileRecoverablesPerCheckpoint);
    }

    private void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }

    private SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable>
            getResumableSerializer() {
        final OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverableSerializer
                outputStreamBasedInProgressFileRecoverableSerializer =
                        (OutputStreamBasedPartFileWriter
                                        .OutputStreamBasedInProgressFileRecoverableSerializer)
                                inProgressFileRecoverableSerializer;
        return outputStreamBasedInProgressFileRecoverableSerializer.getResumeSerializer();
    }

    private SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable>
            getCommitableSerializer() {
        final OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverableSerializer
                outputStreamBasedPendingFileRecoverableSerializer =
                        (OutputStreamBasedPartFileWriter
                                        .OutputStreamBasedPendingFileRecoverableSerializer)
                                pendingFileRecoverableSerializer;
        return outputStreamBasedPendingFileRecoverableSerializer.getCommitSerializer();
    }
}
