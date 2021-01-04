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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileBasedStateOutputStream;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Interface that provides access to a CheckpointStateOutputStream and a method to provide the
 * {@link SnapshotResult}. This abstracts from different ways that a result is obtained from
 * checkpoint output streams.
 */
public interface CheckpointStreamWithResultProvider extends Closeable {

    Logger LOG = LoggerFactory.getLogger(CheckpointStreamWithResultProvider.class);

    /** Closes the stream ans returns a snapshot result with the stream handle(s). */
    @Nonnull
    SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult() throws IOException;

    /** Returns the encapsulated output stream. */
    @Nonnull
    CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream();

    @Override
    default void close() throws IOException {
        getCheckpointOutputStream().close();
    }

    /**
     * Implementation of {@link CheckpointStreamWithResultProvider} that only creates the
     * primary/remote/jm-owned state.
     */
    class PrimaryStreamOnly implements CheckpointStreamWithResultProvider {

        @Nonnull private final CheckpointStreamFactory.CheckpointStateOutputStream outputStream;

        public PrimaryStreamOnly(
                @Nonnull CheckpointStreamFactory.CheckpointStateOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Nonnull
        @Override
        public SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult()
                throws IOException {
            return SnapshotResult.of(outputStream.closeAndGetHandle());
        }

        @Nonnull
        @Override
        public CheckpointStreamFactory.CheckpointStateOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }

    /**
     * Implementation of {@link CheckpointStreamWithResultProvider} that creates both, the
     * primary/remote/jm-owned state and the secondary/local/tm-owned state.
     */
    class PrimaryAndSecondaryStream implements CheckpointStreamWithResultProvider {

        private static final Logger LOG = LoggerFactory.getLogger(PrimaryAndSecondaryStream.class);

        @Nonnull private final DuplicatingCheckpointOutputStream outputStream;

        public PrimaryAndSecondaryStream(
                @Nonnull CheckpointStreamFactory.CheckpointStateOutputStream primaryOut,
                CheckpointStreamFactory.CheckpointStateOutputStream secondaryOut)
                throws IOException {
            this(new DuplicatingCheckpointOutputStream(primaryOut, secondaryOut));
        }

        PrimaryAndSecondaryStream(@Nonnull DuplicatingCheckpointOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Nonnull
        @Override
        public SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult()
                throws IOException {

            final StreamStateHandle primaryStreamStateHandle;

            try {
                primaryStreamStateHandle = outputStream.closeAndGetPrimaryHandle();
            } catch (IOException primaryEx) {
                try {
                    outputStream.close();
                } catch (IOException closeEx) {
                    primaryEx = ExceptionUtils.firstOrSuppressed(closeEx, primaryEx);
                }
                throw primaryEx;
            }

            StreamStateHandle secondaryStreamStateHandle = null;

            try {
                secondaryStreamStateHandle = outputStream.closeAndGetSecondaryHandle();
            } catch (IOException secondaryEx) {
                LOG.warn("Exception from secondary/local checkpoint stream.", secondaryEx);
            }

            if (primaryStreamStateHandle != null) {
                if (secondaryStreamStateHandle != null) {
                    return SnapshotResult.withLocalState(
                            primaryStreamStateHandle, secondaryStreamStateHandle);
                } else {
                    return SnapshotResult.of(primaryStreamStateHandle);
                }
            } else {
                return SnapshotResult.empty();
            }
        }

        @Nonnull
        @Override
        public DuplicatingCheckpointOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }

    @Nonnull
    static CheckpointStreamWithResultProvider createSimpleStream(
            @Nonnull CheckpointedStateScope checkpointedStateScope,
            @Nonnull CheckpointStreamFactory primaryStreamFactory)
            throws IOException {

        CheckpointStreamFactory.CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);

        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }

    @Nonnull
    static CheckpointStreamWithResultProvider createDuplicatingStream(
            @Nonnegative long checkpointId,
            @Nonnull CheckpointedStateScope checkpointedStateScope,
            @Nonnull CheckpointStreamFactory primaryStreamFactory,
            @Nonnull LocalRecoveryDirectoryProvider secondaryStreamDirProvider)
            throws IOException {

        CheckpointStreamFactory.CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);

        try {
            File outFile =
                    new File(
                            secondaryStreamDirProvider.subtaskSpecificCheckpointDirectory(
                                    checkpointId),
                            String.valueOf(UUID.randomUUID()));
            Path outPath = new Path(outFile.toURI());

            CheckpointStreamFactory.CheckpointStateOutputStream secondaryOut =
                    new FileBasedStateOutputStream(outPath.getFileSystem(), outPath);

            return new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(
                    primaryOut, secondaryOut);
        } catch (IOException secondaryEx) {
            LOG.warn(
                    "Exception when opening secondary/local checkpoint output stream. "
                            + "Continue only with the primary stream.",
                    secondaryEx);
        }

        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }

    /**
     * Helper method that takes a {@link SnapshotResult<StreamStateHandle>} and a {@link
     * KeyGroupRangeOffsets} and creates a {@link SnapshotResult<KeyGroupsStateHandle>} by combining
     * the key groups offsets with all the present stream state handles.
     */
    @Nonnull
    static SnapshotResult<KeyedStateHandle> toKeyedStateHandleSnapshotResult(
            @Nonnull SnapshotResult<StreamStateHandle> snapshotResult,
            @Nonnull KeyGroupRangeOffsets keyGroupRangeOffsets) {

        StreamStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();

        if (jobManagerOwnedSnapshot != null) {

            KeyedStateHandle jmKeyedState =
                    new KeyGroupsStateHandle(keyGroupRangeOffsets, jobManagerOwnedSnapshot);
            StreamStateHandle taskLocalSnapshot = snapshotResult.getTaskLocalSnapshot();

            if (taskLocalSnapshot != null) {

                KeyedStateHandle localKeyedState =
                        new KeyGroupsStateHandle(keyGroupRangeOffsets, taskLocalSnapshot);
                return SnapshotResult.withLocalState(jmKeyedState, localKeyedState);
            } else {

                return SnapshotResult.of(jmKeyedState);
            }
        } else {

            return SnapshotResult.empty();
        }
    }
}
