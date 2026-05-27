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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory implementation for {@link NativeS3ObjectOperations}.
 *
 * <p>Backs every reachable S3 operation with hash maps so writer/committer logic can be exercised
 * without an S3 endpoint (no MinIO/Testcontainers required). The parent's {@code S3Client} / {@code
 * S3TransferManager} constructor arguments are passed as {@code null} because no overridden method
 * dereferences them.
 *
 * <p><b>State exposure:</b> the storage maps are exposed as public final fields so tests can
 * inspect them, corrupt them, or simulate object loss directly:
 *
 * <ul>
 *   <li>{@link #storedObjects} — keys written via {@link #putObject(String, File)} (e.g. the
 *       incomplete-tail side objects persisted by {@link NativeS3RecoverableFsDataOutputStream}).
 *   <li>{@link #committedObjects} — keys finalized via {@link #commitMultiPartUpload}.
 *   <li>{@link #openMultipartUploads} — uploadId → partNumber → bytes for in-flight MPUs; entries
 *       are removed on commit or abort.
 * </ul>
 *
 * <p>{@link #getObject} reads from <em>both</em> {@link #storedObjects} and {@link
 * #committedObjects} so tests can fetch a committed object the same way real S3 would serve it.
 *
 * <p><b>Thread safety:</b> not thread-safe. Use a single thread per instance, matching the
 * single-thread invariant of the production {@link NativeS3RecoverableFsDataOutputStream}.
 */
public final class InMemoryNativeS3Operations extends NativeS3ObjectOperations {

    public static final String DEFAULT_BUCKET = "test-bucket";

    /** Keys written via {@link #putObject(String, File)}. */
    public final Map<String, byte[]> storedObjects = new HashMap<>();

    /** Keys finalized via {@link #commitMultiPartUpload}. */
    public final Map<String, byte[]> committedObjects = new HashMap<>();

    /** uploadId → partNumber → uploaded bytes for in-flight MPUs. */
    public final Map<String, Map<Integer, byte[]>> openMultipartUploads = new HashMap<>();

    private final String bucketName;
    private final AtomicInteger uploadIdSeq = new AtomicInteger();

    public InMemoryNativeS3Operations() {
        this(DEFAULT_BUCKET);
    }

    public InMemoryNativeS3Operations(String bucketName) {
        super(/* s3Client */ null, /* transferManager */ null, bucketName, /* useAsync */ false);
        this.bucketName = bucketName;
    }

    @Override
    public String startMultiPartUpload(String key) {
        String uploadId = "U" + uploadIdSeq.incrementAndGet();
        openMultipartUploads.put(uploadId, new HashMap<>());
        return uploadId;
    }

    @Override
    public UploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File file, long length)
            throws IOException {
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
        return new UploadPartResult(partNumber, "etag-" + uploadId + "-" + partNumber);
    }

    @Override
    public PutObjectResult putObject(String key, File file) throws IOException {
        storedObjects.put(key, Files.readAllBytes(file.toPath()));
        return new PutObjectResult("etag-" + UUID.randomUUID());
    }

    @Override
    public long getObject(String key, File targetLocation) throws IOException {
        byte[] data = storedObjects.get(key);
        if (data == null) {
            data = committedObjects.get(key);
        }
        if (data == null) {
            throw new IOException("not found: " + key);
        }
        Files.write(targetLocation.toPath(), data);
        return data.length;
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
        return new CompleteMultipartUploadResult(bucketName, key, "final-etag-" + uploadId, null);
    }

    @Override
    public void abortMultiPartUpload(String key, String uploadId) {
        openMultipartUploads.remove(uploadId);
    }

    @Override
    public boolean deleteObject(String key) {
        return storedObjects.remove(key) != null || committedObjects.remove(key) != null;
    }
}
