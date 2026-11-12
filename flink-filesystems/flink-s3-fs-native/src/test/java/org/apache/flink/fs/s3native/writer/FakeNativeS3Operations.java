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

package org.apache.flink.fs.s3native.writer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link NativeS3ObjectOperations} fake for the recoverable writer tests. It keeps
 * multipart uploads, completed objects and standalone objects in memory and has hooks to inject
 * part-upload and abort failures, so {@link NativeS3RecoverableWriter} and {@link
 * NativeS3RecoverableFsDataOutputStream} can be tested without an S3 endpoint.
 */
final class FakeNativeS3Operations extends NativeS3ObjectOperations {

    final Map<String, byte[]> committedObjects = new HashMap<>();
    final Map<String, Map<Integer, byte[]>> openMultipartUploads = new ConcurrentHashMap<>();

    /** Standalone objects written with {@link #putObject}, such as the persisted tail objects. */
    final Map<String, byte[]> objects = new HashMap<>();

    volatile boolean failUploadPart = false;
    volatile boolean deletePartFileAfterUpload = false;
    int abortAttempts = 0;
    final AtomicInteger uploadPartAttempts = new AtomicInteger();

    /** If set, part uploads do not finish before the latch is counted down. */
    volatile CountDownLatch uploadPartGate;

    final AtomicInteger inFlightUploads = new AtomicInteger();
    final AtomicInteger maxInFlightUploads = new AtomicInteger();

    private final AtomicInteger uploadIdSeq = new AtomicInteger();

    FakeNativeS3Operations() {
        super(/* s3Client */ null, /* transferManager */ null, "test-bucket", false);
    }

    @Override
    public String startMultiPartUpload(String key) {
        String uploadId = "U" + uploadIdSeq.incrementAndGet();
        openMultipartUploads.put(uploadId, new ConcurrentHashMap<>());
        return uploadId;
    }

    @Override
    public UploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File file, long length)
            throws IOException {
        maxInFlightUploads.accumulateAndGet(inFlightUploads.incrementAndGet(), Math::max);
        try {
            awaitUploadPartGate();
            return doUploadPart(uploadId, partNumber, file, length);
        } finally {
            inFlightUploads.decrementAndGet();
        }
    }

    private void awaitUploadPartGate() throws IOException {
        final CountDownLatch gate = uploadPartGate;
        if (gate == null) {
            return;
        }
        try {
            // Fail instead of hanging forever if a test never opens the gate, e.g. because the
            // stream unexpectedly uploads synchronously on the writing thread.
            if (!gate.await(30, TimeUnit.SECONDS)) {
                throw new IOException("the part upload gate was not opened");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for the part upload gate", e);
        }
    }

    private UploadPartResult doUploadPart(String uploadId, int partNumber, File file, long length)
            throws IOException {
        uploadPartAttempts.incrementAndGet();
        if (failUploadPart) {
            throw new IOException("injected uploadPart failure for uploadId: " + uploadId);
        }
        Map<Integer, byte[]> parts = openMultipartUploads.get(uploadId);
        if (parts == null) {
            throw new IOException("unknown uploadId: " + uploadId);
        }
        byte[] data = Files.readAllBytes(file.toPath());
        if (data.length != length) {
            throw new IOException(
                    "part length mismatch: expected " + length + ", got " + data.length);
        }
        parts.put(partNumber, data);
        if (deletePartFileAfterUpload) {
            Files.delete(file.toPath());
        }
        return new UploadPartResult(partNumber, "etag-" + uploadId + "-" + partNumber);
    }

    @Override
    public CompleteMultipartUploadResult commitMultiPartUpload(
            String key, String uploadId, List<UploadPartResult> parts, long length)
            throws IOException {
        Map<Integer, byte[]> uploaded = openMultipartUploads.remove(uploadId);
        if (uploaded == null) {
            throw new IOException("unknown uploadId: " + uploadId);
        }
        List<Integer> ordered = new ArrayList<>(parts.size());
        for (UploadPartResult p : parts) {
            ordered.add(p.getPartNumber());
        }
        Collections.sort(ordered);
        ByteArrayOutputStream merged = new ByteArrayOutputStream();
        for (int n : ordered) {
            byte[] partData = uploaded.get(n);
            if (partData == null) {
                throw new IOException("missing part " + n + " for uploadId " + uploadId);
            }
            merged.write(partData);
        }
        byte[] finalBytes = merged.toByteArray();
        if (finalBytes.length != length) {
            throw new IOException(
                    "committed length mismatch: expected " + length + ", got " + finalBytes.length);
        }
        committedObjects.put(key, finalBytes);
        return new CompleteMultipartUploadResult(
                "test-bucket", key, "final-etag-" + uploadId, null);
    }

    @Override
    public void abortMultiPartUpload(String key, String uploadId) throws IOException {
        abortAttempts++;
        openMultipartUploads.remove(uploadId);
    }

    @Override
    public PutObjectResult putObject(String key, File inputFile) throws IOException {
        objects.put(key, Files.readAllBytes(inputFile.toPath()));
        return new PutObjectResult("etag-" + key);
    }

    @Override
    public long getObject(String key, File targetLocation) throws IOException {
        byte[] data = objects.get(key);
        if (data == null) {
            throw new IOException("Failed to get object for key: " + key);
        }
        Files.write(targetLocation.toPath(), data);
        return data.length;
    }

    @Override
    public boolean deleteObject(String key) {
        return objects.remove(key) != null;
    }
}
