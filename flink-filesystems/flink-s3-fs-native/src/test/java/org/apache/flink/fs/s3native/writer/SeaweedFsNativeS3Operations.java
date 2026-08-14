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

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link NativeS3ObjectOperations} backed by a real (SeaweedFS) S3 endpoint, plus byte-level
 * helpers so writer/committer tests can inspect and tamper with the objects that actually landed in
 * S3.
 *
 * <p>A real endpoint reproduces the S3 behaviors the recovery tests actually depend on: the 5 MiB
 * multipart minimum part size, real {@code NoSuchKeyException} semantics on {@code headObject}, and
 * genuine network-level {@code GetObject}/{@code PutObject} responses.
 */
final class SeaweedFsNativeS3Operations extends NativeS3ObjectOperations {

    private final S3Client client;
    private final String bucket;

    SeaweedFsNativeS3Operations(S3Client client, String bucket) {
        super(client, bucket);
        this.client = client;
        this.bucket = bucket;
    }

    List<String> listKeys(String prefix) {
        return client.listObjectsV2(b -> b.bucket(bucket).prefix(prefix)).contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    byte[] readObject(String key) {
        return client.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asByteArray();
    }

    void writeObject(String key, byte[] data) {
        client.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromBytes(data));
    }

    void removeObject(String key) {
        client.deleteObject(b -> b.bucket(bucket).key(key));
    }

    boolean objectExists(String key) {
        try {
            client.headObject(b -> b.bucket(bucket).key(key));
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }
}
