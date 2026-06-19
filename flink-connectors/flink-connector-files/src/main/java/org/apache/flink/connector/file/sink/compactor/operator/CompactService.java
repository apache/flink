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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.connector.file.sink.compactor.OutputStreamBasedFileCompactor;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter.Type;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedCompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.RecordWiseCompactingFileWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkState;

/** The asynchronous file compaction service. */
@Internal
public class CompactService {
    private static final String COMPACTED_PREFIX = "compacted-";

    private final int numCompactThreads;
    private final FileCompactor fileCompactor;
    private final CompactingFileWriter.Type compactingWriterType;
    private final BucketWriter<?, String> bucketWriter;

    private transient ExecutorService compactService;

    public CompactService(
            int numCompactThreads,
            FileCompactor fileCompactor,
            BucketWriter<?, String> bucketWriter) {
        this.numCompactThreads = numCompactThreads;
        this.fileCompactor = fileCompactor;
        this.bucketWriter = bucketWriter;
        this.compactingWriterType = getWriterType(fileCompactor);
    }

    public void open() {
        compactService =
                Executors.newFixedThreadPool(
                        Math.max(1, Math.min(numCompactThreads, Hardware.getNumberCPUCores())),
                        new ExecutorThreadFactory("compact-executor"));
    }

    public void submit(
            CompactorRequest request,
            CompletableFuture<Iterable<FileSinkCommittable>> resultFuture) {
        compactService.submit(
                () -> {
                    try {
                        Iterable<FileSinkCommittable> result = compact(request);
                        resultFuture.complete(result);
                    } catch (Exception e) {
                        resultFuture.completeExceptionally(e);
                    }
                });
    }

    public void close() {
        if (compactService != null) {
            compactService.shutdownNow();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Iterable<FileSinkCommittable> compact(CompactorRequest request) throws Exception {
        List<FileSinkCommittable> results = new ArrayList<>(request.getCommittableToPassthrough());

        List<Path> compactingFiles = getCompactingPath(request);
        if (compactingFiles.isEmpty()) {
            return results;
        }

        Path targetPath = assembleCompactedFilePath(compactingFiles.get(0));
        CompactingFileWriter compactingFileWriter =
                bucketWriter.openNewCompactingFile(
                        compactingWriterType,
                        request.getBucketId(),
                        targetPath,
                        System.currentTimeMillis());
        if (compactingWriterType == Type.RECORD_WISE) {
            ((RecordWiseFileCompactor) fileCompactor)
                    .compact(
                            compactingFiles,
                            ((RecordWiseCompactingFileWriter) compactingFileWriter)::write);
        } else if (compactingWriterType == CompactingFileWriter.Type.OUTPUT_STREAM) {
            ((OutputStreamBasedFileCompactor) fileCompactor)
                    .compact(
                            compactingFiles,
                            ((OutputStreamBasedCompactingFileWriter) compactingFileWriter)
                                    .asOutputStream());
        }
        PendingFileRecoverable compactedPendingFile = compactingFileWriter.closeForCommit();

        FileSinkCommittable compacted =
                new FileSinkCommittable(request.getBucketId(), compactedPendingFile);
        results.add(compacted);
        for (Path f : compactingFiles) {
            // cleanup compacted files
            results.add(new FileSinkCommittable(request.getBucketId(), f));
        }

        return results;
    }

    private List<Path> getCompactingPath(CompactorRequest request) throws IOException {
        List<FileSinkCommittable> compactingCommittable = request.getCommittableToCompact();
        List<Path> compactingFiles = new ArrayList<>();

        for (FileSinkCommittable committable : compactingCommittable) {
            PendingFileRecoverable pendingFile = committable.getPendingFile();
            checkState(
                    pendingFile != null, "Illegal committable to compact, pending file is null.");

            Path pendingPath = pendingFile.getPath();
            checkState(
                    pendingPath != null && pendingPath.getName().startsWith("."),
                    "Illegal pending file to compact, path should start with . but is "
                            + pendingPath);

            // commit the pending file and compact the committed file
            bucketWriter.recoverPendingFile(pendingFile).commitAfterRecovery();
            compactingFiles.add(pendingPath);
        }
        return compactingFiles;
    }

    private static Path assembleCompactedFilePath(Path uncompactedPath) {
        String uncompactedName = uncompactedPath.getName();
        if (uncompactedName.startsWith(".")) {
            uncompactedName = uncompactedName.substring(1);
        }
        return new Path(uncompactedPath.getParent(), COMPACTED_PREFIX + uncompactedName);
    }

    private static CompactingFileWriter.Type getWriterType(FileCompactor fileCompactor) {
        if (fileCompactor instanceof OutputStreamBasedFileCompactor) {
            return CompactingFileWriter.Type.OUTPUT_STREAM;
        } else if (fileCompactor instanceof RecordWiseFileCompactor) {
            return CompactingFileWriter.Type.RECORD_WISE;
        } else {
            throw new UnsupportedOperationException(
                    "Unable to crate compacting file writer for compactor:"
                            + fileCompactor.getClass());
        }
    }
}
