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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3RecursiveDelete}. */
class NativeS3RecursiveDeleteTest {

    private static final String BUCKET = "test-bucket";

    @Test
    void batchingEnabledIssuesOneDeleteObjectsCallAndNoDeleteObjectCalls() throws Exception {
        try (RecordingS3Client client = new RecordingS3Client(keys(5), 1000)) {
            new NativeS3RecursiveDelete(client, BUCKET, "dir", true).execute();

            assertThat(client.deleteObjectsRequests).hasSize(1);
            assertThat(client.deleteObjectsRequests.get(0).delete().objects())
                    .extracting(ObjectIdentifier::key)
                    .containsExactlyElementsOf(keys(5));
            assertThat(client.deleteObjectRequests).isEmpty();
        }
    }

    @Test
    void batchingDisabledIssuesOneDeleteObjectCallPerKeyAndNoDeleteObjectsCalls() throws Exception {
        try (RecordingS3Client client = new RecordingS3Client(keys(5), 1000)) {
            new NativeS3RecursiveDelete(client, BUCKET, "dir", false).execute();

            assertThat(client.deleteObjectRequests)
                    .extracting(DeleteObjectRequest::key)
                    .containsExactlyElementsOf(keys(5));
            assertThat(client.deleteObjectsRequests).isEmpty();
        }
    }

    @Test
    void moreThanPageSizeKeysAreSplitAcrossMultipleDeleteObjectsCalls() throws Exception {
        // Small page size to make pagination and per-page batching observable without
        // constructing 1000+ keys.
        try (RecordingS3Client client = new RecordingS3Client(keys(7), 3)) {
            new NativeS3RecursiveDelete(client, BUCKET, "dir", true).execute();

            // 7 keys at 3 per listing page -> 3 DeleteObjects calls (3, 3, 1 keys), each within
            // the page size, and never holding more than one page of keys at a time.
            assertThat(client.deleteObjectsRequests).hasSize(3);
            assertThat(
                            client.deleteObjectsRequests.stream()
                                    .flatMap(r -> r.delete().objects().stream())
                                    .map(ObjectIdentifier::key)
                                    .collect(Collectors.toList()))
                    .containsExactlyElementsOf(keys(7));
            assertThat(client.deleteObjectsRequests)
                    .allSatisfy(r -> assertThat(r.delete().objects()).hasSizeLessThanOrEqualTo(3));
        }
    }

    @Test
    void partialBatchDeleteFailureThrowsIOException() throws Exception {
        try (RecordingS3Client client = new RecordingS3Client(keys(3), 1000)) {
            client.failKeyWith(
                    keys(3).get(1), S3Error.builder().code("AccessDenied").message("nope").build());

            assertThatThrownBy(
                            () ->
                                    new NativeS3RecursiveDelete(client, BUCKET, "dir", true)
                                            .execute())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(keys(3).get(1))
                    .hasMessageContaining("AccessDenied");
        }
    }

    private static List<String> keys(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> "dir/file-" + i)
                .collect(Collectors.toList());
    }

    /** Records every {@code list}/{@code delete} request issued against a fixed key set. */
    private static final class RecordingS3Client implements S3Client {
        private final List<String> allKeys;
        private final int pageSize;
        private final Map<String, S3Error> failuresByKey = new HashMap<>();

        final List<DeleteObjectsRequest> deleteObjectsRequests = new ArrayList<>();
        final List<DeleteObjectRequest> deleteObjectRequests = new ArrayList<>();

        RecordingS3Client(List<String> allKeys, int pageSize) {
            this.allKeys = allKeys;
            this.pageSize = pageSize;
        }

        void failKeyWith(String key, S3Error error) {
            failuresByKey.put(key, error);
        }

        @Override
        public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
            int start =
                    request.continuationToken() == null
                            ? 0
                            : Integer.parseInt(request.continuationToken());
            int end = Math.min(start + pageSize, allKeys.size());

            List<S3Object> page =
                    allKeys.subList(start, end).stream()
                            .map(k -> S3Object.builder().key(k).build())
                            .collect(Collectors.toList());

            ListObjectsV2Response.Builder builder = ListObjectsV2Response.builder().contents(page);
            if (end < allKeys.size()) {
                builder.nextContinuationToken(String.valueOf(end)).isTruncated(true);
            }
            return builder.build();
        }

        @Override
        public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request) {
            deleteObjectsRequests.add(request);
            List<S3Error> errors = new ArrayList<>();
            for (ObjectIdentifier id : request.delete().objects()) {
                S3Error error = failuresByKey.get(id.key());
                if (error != null) {
                    errors.add(error.toBuilder().key(id.key()).build());
                }
            }
            return DeleteObjectsResponse.builder().errors(errors).build();
        }

        @Override
        public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
            deleteObjectRequests.add(request);
            return DeleteObjectResponse.builder().build();
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {}
    }
}
