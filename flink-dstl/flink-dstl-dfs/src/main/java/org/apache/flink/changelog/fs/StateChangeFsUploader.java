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

package org.apache.flink.changelog.fs;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;

class StateChangeFsUploader implements StateChangeUploader {
    private static final Logger LOG = LoggerFactory.getLogger(StateChangeFsUploader.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final StateChangeFormat format;
    private final boolean compression;
    private final int bufferSize;
    private final FsStateChangelogCleaner cleaner;

    public StateChangeFsUploader(
            Path basePath,
            FileSystem fileSystem,
            boolean compression,
            int bufferSize,
            FsStateChangelogCleaner cleaner) {
        this.basePath = basePath;
        this.fileSystem = fileSystem;
        this.format = new StateChangeFormat();
        this.compression = compression;
        this.bufferSize = bufferSize;
        this.cleaner = cleaner;
    }

    @Override
    public void upload(Collection<StateChangeSetUpload> changeSets) throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload {} changesets to {}", changeSets.size(), fileName);
        Path path = new Path(basePath, fileName);

        FSDataOutputStream fsStream = fileSystem.create(path, NO_OVERWRITE);
        try {
            fsStream.write(compression ? 1 : 0);
            OutputStreamWithPos stream = wrap(fsStream);
            upload(changeSets, path, stream);
        } catch (IOException e) {
            fsStream.close(); // normally, the stream in closed in upload
            changeSets.forEach(cs -> cs.fail(e));
            handleError(path, e);
        }
    }

    private OutputStreamWithPos wrap(FSDataOutputStream fsStream) throws IOException {
        StreamCompressionDecorator instance =
                compression
                        ? SnappyStreamCompressionDecorator.INSTANCE
                        : UncompressedStreamCompressionDecorator.INSTANCE;
        OutputStream compressed =
                compression ? instance.decorateWithCompression(fsStream) : fsStream;
        return new OutputStreamWithPos(new BufferedOutputStream(compressed, bufferSize));
    }

    private void handleError(Path path, IOException e) throws IOException {
        try {
            fileSystem.delete(path, true);
        } catch (IOException cleanupError) {
            LOG.warn("unable to delete after failure: " + path, cleanupError);
            e.addSuppressed(cleanupError);
        }
        throw e;
    }

    private void upload(
            Collection<StateChangeSetUpload> changeSets, Path path, OutputStreamWithPos os)
            throws IOException {
        final Map<StateChangeSetUpload, Long> offsets = format.write(os, changeSets);
        if (offsets.isEmpty()) {
            LOG.info(
                    "nothing to upload (cancelled concurrently), cancelling upload {} {}",
                    changeSets.size(),
                    path);
            os.close();
            fileSystem.delete(path, true);
        } else {
            final long size = os.getPos();
            os.close();
            final StreamStateHandle handle = new FileStateHandle(path, size);
            offsets.forEach(
                    (upload, offset) ->
                            upload.complete(
                                    new StoreResult(
                                            handle,
                                            offset,
                                            upload.getSequenceNumber(),
                                            upload.getSize()),
                                    cleaner));
            LOG.debug("uploaded to {}", handle);
        }
    }

    private String generateFileName() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {}
}
