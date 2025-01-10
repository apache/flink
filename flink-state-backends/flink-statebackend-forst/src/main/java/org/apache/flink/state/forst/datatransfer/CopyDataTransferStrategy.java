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
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    public IncrementalKeyedStateHandle.HandleAndLocalPath transferToCheckpoint(
            Path dbFilePath,
            long maxTransferBytes,
            CheckpointStreamFactory checkpointStreamFactory,
            CheckpointedStateScope stateScope,
            CloseableRegistry closeableRegistry,
            CloseableRegistry tmpResourcesRegistry)
            throws IOException {

        LOG.trace("Copy file to checkpoint: {}", dbFilePath);
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

    private static IncrementalKeyedStateHandle.HandleAndLocalPath copyFileToCheckpoint(
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

            return IncrementalKeyedStateHandle.HandleAndLocalPath.of(result, filePath.getName());

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
