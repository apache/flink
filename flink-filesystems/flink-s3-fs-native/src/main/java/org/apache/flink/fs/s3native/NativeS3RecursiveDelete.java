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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A single recursive deletion of every object under one key prefix. Instantiated per {@code
 * delete(path, true)} call; walks the prefix one listing page at a time so memory use stays bounded
 * by the page size rather than the size of the whole subtree, since each page maps directly onto
 * one {@code DeleteObjects} request.
 */
final class NativeS3RecursiveDelete {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3RecursiveDelete.class);

    /** Also used as the listing page size (the S3 {@code DeleteObjects} limit). */
    private static final int PAGE_SIZE = 1000;

    private final S3Client s3Client;
    private final String bucketName;
    private final String prefix;
    private final boolean batchEnabled;

    NativeS3RecursiveDelete(
            S3Client s3Client, String bucketName, String key, boolean batchEnabled) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.prefix = key.endsWith("/") ? key : key + "/";
        this.batchEnabled = batchEnabled;
    }

    /** Deletes every object under the prefix using the configured delete strategy. */
    void execute() throws IOException {
        String continuationToken = null;

        do {
            final ListObjectsV2Response response = listPage(continuationToken);
            final List<String> pageKeys = extractKeys(response);

            if (!pageKeys.isEmpty()) {
                deletePage(pageKeys);
            }

            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
    }

    private ListObjectsV2Response listPage(String continuationToken) {
        final ListObjectsV2Request.Builder requestBuilder =
                ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).maxKeys(PAGE_SIZE);
        if (continuationToken != null) {
            requestBuilder.continuationToken(continuationToken);
        }
        return s3Client.listObjectsV2(requestBuilder.build());
    }

    private List<String> extractKeys(ListObjectsV2Response response) {
        final List<String> keys = new ArrayList<>(response.contents().size());
        for (S3Object s3Object : response.contents()) {
            keys.add(s3Object.key());
        }
        return keys;
    }

    private void deletePage(List<String> keys) throws IOException {
        if (batchEnabled) {
            LOG.debug(
                    "Deleting {} object(s) under prefix {} using batched DeleteObjects",
                    keys.size(),
                    prefix);
            deleteBatch(keys);
        } else {
            LOG.debug(
                    "Deleting {} object(s) under prefix {} using individual DeleteObject calls "
                            + "(delete batching disabled)",
                    keys.size(),
                    prefix);
            deleteIndividually(keys);
        }
    }

    private void deleteIndividually(List<String> keys) {
        for (String key : keys) {
            final DeleteObjectRequest request =
                    DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
            s3Client.deleteObject(request);
        }
    }

    private void deleteBatch(List<String> keys) throws IOException {
        final List<ObjectIdentifier> objectIdentifiers = new ArrayList<>(keys.size());
        for (String key : keys) {
            objectIdentifiers.add(ObjectIdentifier.builder().key(key).build());
        }

        final DeleteObjectsRequest request =
                DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(Delete.builder().objects(objectIdentifiers).build())
                        .build();

        LOG.debug("Issuing batch DeleteObjects request for {} key(s)", keys.size());
        final DeleteObjectsResponse response = s3Client.deleteObjects(request);
        if (response.hasErrors() && !response.errors().isEmpty()) {
            final StringBuilder errorMessage = new StringBuilder("Failed to delete objects: ");
            for (S3Error error : response.errors()) {
                errorMessage
                        .append(error.key())
                        .append(" (")
                        .append(error.code())
                        .append(": ")
                        .append(error.message())
                        .append("); ");
            }
            throw new IOException(errorMessage.toString());
        }
    }
}
