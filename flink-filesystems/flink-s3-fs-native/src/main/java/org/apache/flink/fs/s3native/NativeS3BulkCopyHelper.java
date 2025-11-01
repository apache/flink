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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.PathsCopyingFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedCopy;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Internal
public class NativeS3BulkCopyHelper {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3BulkCopyHelper.class);

    private final S3TransferManager transferManager;
    private final int maxConcurrentCopies;

    public NativeS3BulkCopyHelper(S3TransferManager transferManager, int maxConcurrentCopies) {
        this.transferManager = transferManager;
        this.maxConcurrentCopies = maxConcurrentCopies;
    }

    public void copyFiles(
            List<PathsCopyingFileSystem.CopyRequest> requests, ICloseableRegistry closeableRegistry)
            throws IOException {

        if (requests.isEmpty()) {
            return;
        }

        LOG.info("Starting bulk copy of {} files using S3TransferManager", requests.size());

        List<CompletableFuture<CompletedCopy>> copyFutures = new ArrayList<>();

        for (int i = 0; i < requests.size(); i++) {
            PathsCopyingFileSystem.CopyRequest request = requests.get(i);
            String sourceUri = request.getSource().toUri().toString();
            if (sourceUri.startsWith("s3://") || sourceUri.startsWith("s3a://")) {
                copyS3ToLocal(request, copyFutures);
            } else {
                throw new UnsupportedOperationException(
                        "Only S3 to local copies are currently supported: " + sourceUri);
            }

            if (copyFutures.size() >= maxConcurrentCopies || i == requests.size() - 1) {
                waitForCopies(copyFutures);
                copyFutures.clear();
            }
        }

        LOG.info("Completed bulk copy of {} files", requests.size());
    }

    private void copyS3ToLocal(
            PathsCopyingFileSystem.CopyRequest request,
            List<CompletableFuture<CompletedCopy>> copyFutures)
            throws IOException {

        String sourceUri = request.getSource().toUri().toString();
        String bucket = extractBucket(sourceUri);
        String key = extractKey(sourceUri);
        File destFile = new File(request.getDestination().getPath());

        Files.createDirectories(destFile.getParentFile().toPath());

        DownloadFileRequest downloadRequest =
                DownloadFileRequest.builder()
                        .getObjectRequest(req -> req.bucket(bucket).key(key))
                        .destination(destFile.toPath())
                        .build();

        FileDownload download = transferManager.downloadFile(downloadRequest);

        CompletableFuture<CompletedCopy> future =
                download.completionFuture()
                        .thenApply(
                                completed -> {
                                    LOG.debug("Successfully copied {} to {}", sourceUri, destFile);
                                    return null;
                                });

        copyFutures.add(future);
    }

    private void waitForCopies(List<CompletableFuture<CompletedCopy>> futures) throws IOException {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Bulk copy interrupted", e);
        } catch (ExecutionException e) {
            throw new IOException("Bulk copy failed", e.getCause());
        }
    }

    private String extractBucket(String s3Uri) {
        String uri = s3Uri.replaceFirst("s3a://", "s3://");
        int bucketStart = uri.indexOf("://") + 3;
        int bucketEnd = uri.indexOf("/", bucketStart);
        if (bucketEnd == -1) {
            return uri.substring(bucketStart);
        }
        return uri.substring(bucketStart, bucketEnd);
    }

    private String extractKey(String s3Uri) {
        String uri = s3Uri.replaceFirst("s3a://", "s3://");
        int bucketStart = uri.indexOf("://") + 3;
        int keyStart = uri.indexOf("/", bucketStart);
        if (keyStart == -1) {
            return "";
        }
        return uri.substring(keyStart + 1);
    }

    public void close() {
        if (transferManager != null) {
            transferManager.close();
        }
    }
}
