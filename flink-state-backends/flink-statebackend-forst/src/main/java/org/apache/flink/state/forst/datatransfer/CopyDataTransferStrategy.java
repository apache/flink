/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.datatransfer;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.state.forst.fs.filemapping.FileBackedMappingEntrySource;
import org.apache.flink.state.forst.fs.filemapping.HandleBackedMappingEntrySource;
import org.apache.flink.state.forst.fs.filemapping.MappingEntry;
import org.apache.flink.state.forst.fs.filemapping.MappingEntrySource;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

/**
 * Data transfer strategy for ForSt DB without a remote DB path. It always copies the file to/from
 * checkpoint storage when transferring data.
 */
public class CopyDataTransferStrategy extends DataTransferStrategy {

    private static final int READ_BUFFER_SIZE = 64 * 1024;

    CopyDataTransferStrategy() {
        super(new LocalFileSystem());
    }

    CopyDataTransferStrategy(@Nonnull FileSystem dbFileSystem) {
        super(dbFileSystem);
    }

    @Override
    public HandleAndLocalPath transferToCheckpoint(
            Path dbFilePath,
            long maxTransferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        if (maxTransferBytes < 0) {
            // Means transfer whole file to checkpoint storage.
            maxTransferBytes = Long.MAX_VALUE;
        }

        return copyFileToCheckpoint(
                dbFileSystem,
                dbFilePath,
                maxTransferBytes,
                checkpointStreamFactory,
                stateScope,
                closeableRegistry,
                tmpResourcesRegistry);
    }

    @Override
    public void transferFromCheckpoint(
            StreamStateHandle sourceHandle, Path targetPath, CloseableRegistry closeableRegistry)
            throws IOException {
        LOG.trace("Copy file from checkpoint: {}, {}, {}", sourceHandle, targetPath, dbFileSystem);
        copyFileFromCheckpoint(dbFileSystem, sourceHandle, targetPath, closeableRegistry);
    }

    @Override
    public String toString() {
        return "CopyDataTransferStrategy{" + ", dbFileSystem=" + dbFileSystem + '}';
    }

    private static HandleAndLocalPath copyFileToCheckpoint(
            FileSystem dbFileSystem,
            Path filePath,
            long maxTransferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {
        StreamStateHandle handleByDuplicating =
                duplicateFileToCheckpoint(
                        dbFileSystem, filePath, checkpointStreamFactory, stateScope);
        if (handleByDuplicating != null) {
            LOG.trace("Duplicate file to checkpoint: {} {}", filePath, handleByDuplicating);
            return HandleAndLocalPath.of(handleByDuplicating, filePath.getName());
        }

        HandleAndLocalPath handleAndLocalPath =
                HandleAndLocalPath.of(
                        writeFileToCheckpoint(
                                dbFileSystem,
                                filePath,
                                maxTransferBytes,
                                checkpointStreamFactory,
                                stateScope,
                                closeableRegistry,
                                tmpResourcesRegistry),
                        filePath.getName());
        LOG.trace("Write file to checkpoint: {}, {}", filePath, handleAndLocalPath.getHandle());
        return handleAndLocalPath;
    }

    /**
     * Duplicate file to checkpoint storage by calling {@link CheckpointStreamFactory#duplicate} if
     * possible.
     */
    private static @Nullable StreamStateHandle duplicateFileToCheckpoint(
            FileSystem dbFileSystem,
            Path filePath,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope)
            throws IOException {

        StreamStateHandle stateHandle = getStateHandle(dbFileSystem, filePath);

        if (!checkpointStreamFactory.canFastDuplicate(stateHandle, stateScope)) {
            return null;
        }

        List<StreamStateHandle> result =
                checkpointStreamFactory.duplicate(
                        Collections.singletonList(stateHandle), stateScope);
        return result.get(0);
    }

    private static StreamStateHandle getStateHandle(FileSystem dbFileSystem, Path filePath)
            throws IOException {
        Path sourceFilePath = filePath;
        if (dbFileSystem instanceof ForStFlinkFileSystem) {
            MappingEntry mappingEntry =
                    ((ForStFlinkFileSystem) dbFileSystem).getMappingEntry(filePath);
            Preconditions.checkNotNull(
                    mappingEntry, "File mapping entry not found for %s", filePath);

            MappingEntrySource source = mappingEntry.getSource();
            if (source instanceof HandleBackedMappingEntrySource) {
                // return the state handle stored in MappingEntry
                return ((HandleBackedMappingEntrySource) source).getStateHandle();
            } else {
                // use file path stored in MappingEntry
                sourceFilePath = ((FileBackedMappingEntrySource) source).getFilePath();
            }
        }

        // construct a FileStateHandle base on source file
        FileSystem sourceFileSystem = sourceFilePath.getFileSystem();
        long fileLength = sourceFileSystem.getFileStatus(sourceFilePath).getLen();
        return new FileStateHandle(sourceFilePath, fileLength);
    }

    /** Write file to checkpoint storage through {@link CheckpointStateOutputStream}. */
    private static @Nullable StreamStateHandle writeFileToCheckpoint(
            FileSystem dbFileSystem,
            Path filePath,
            long maxTransferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {
        InputStream inputStream = null;
        CheckpointStateOutputStream outputStream = null;

        try {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];

            inputStream = dbFileSystem.open(filePath, READ_BUFFER_SIZE);
            closeableRegistry.registerCloseable(inputStream);

            outputStream = checkpointStreamFactory.createCheckpointStateOutputStream(stateScope);
            closeableRegistry.registerCloseable(outputStream);

            while (maxTransferBytes > 0) {
                int maxReadBytes = (int) Math.min(maxTransferBytes, READ_BUFFER_SIZE);
                int readBytes = inputStream.read(buffer, 0, maxReadBytes);

                if (readBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, readBytes);

                maxTransferBytes -= readBytes;
            }

            final StreamStateHandle result;
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                result = outputStream.closeAndGetHandle();
                outputStream = null;
            } else {
                result = null;
            }
            tmpResourcesRegistry.registerCloseable(
                    () -> StateUtil.discardStateObjectQuietly(result));

            return result;
        } finally {
            if (closeableRegistry.unregisterCloseable(inputStream)) {
                IOUtils.closeQuietly(inputStream);
            }
            if (closeableRegistry.unregisterCloseable(outputStream)) {
                IOUtils.closeQuietly(outputStream);
            }
        }
    }

    private static void copyFileFromCheckpoint(
            FileSystem dbFileSystem,
            StreamStateHandle sourceHandle,
            Path targetPath,
            CloseableRegistry closeableRegistry)
            throws IOException {

        if (closeableRegistry.isClosed()) {
            // This means other transfer which is registered with the same registry failed, return
            // directly for fast fail.
            return;
        }

        try {
            FSDataInputStream input = sourceHandle.openInputStream();
            closeableRegistry.registerCloseable(input);

            FSDataOutputStream output =
                    dbFileSystem.create(targetPath, FileSystem.WriteMode.NO_OVERWRITE);
            closeableRegistry.registerCloseable(output);

            byte[] buffer = new byte[READ_BUFFER_SIZE];
            while (true) {
                int numBytes = input.read(buffer);
                if (numBytes == -1) {
                    break;
                }
                output.write(buffer, 0, numBytes);
            }
            closeableRegistry.unregisterAndCloseAll(output, input);
        } catch (Exception ex) {
            // Quickly close all open streams. This also stops all concurrent transfers because they
            // are registered with the same registry.
            LOG.info("closing: {}, {}, {}", sourceHandle, targetPath, ex);
            IOUtils.closeQuietly(closeableRegistry);
            throw new IOException(ex);
        }
    }
}
