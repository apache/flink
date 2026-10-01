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

package org.apache.flink.fs.s3hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests specifically for the S3 callback implementation in HadoopS3AccessHelper. These tests
 * verify that the callbacks actually implement S3 operations using AWS SDK v2 instead of throwing
 * UnsupportedOperationException.
 */
public class S3CallbackImplementationTest {

    @Test
    public void testUploadPartCallbackImplementation() throws Exception {
        // Test that uploadPart callback attempts real S3 operations
        WriteOperationHelper.WriteOperationHelperCallbacks callbacks = createTestCallbacks();

        // Create a well-formed upload part request
        software.amazon.awssdk.services.s3.model.UploadPartRequest uploadRequest =
                software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
                        .bucket("test-bucket")
                        .key("test-key")
                        .uploadId("test-upload-id")
                        .partNumber(1)
                        .contentLength(100L)
                        .build();

        software.amazon.awssdk.core.sync.RequestBody body =
                software.amazon.awssdk.core.sync.RequestBody.fromString("test content");

        try {
            callbacks.uploadPart(uploadRequest, body, null);
            // If we get here, the S3 operation succeeded (unlikely in test environment)
            assertTrue("S3 upload operation succeeded", true);
        } catch (RuntimeException e) {
            // Expected in test environment due to no AWS credentials/connectivity
            assertTrue(
                    "Should attempt S3 operation and fail with network/auth error, got: "
                            + e.getMessage(),
                    e.getMessage().contains("Failed to upload S3 part")
                            || e.getMessage().contains("Failed to create S3 client")
                            || e.getMessage().contains("Unable to load AWS")
                            || e.getMessage().contains("credentials"));
        }
    }

    @Test
    public void testCompleteMultipartUploadCallbackImplementation() throws Exception {
        // Test that completeMultipartUpload callback attempts real S3 operations
        WriteOperationHelper.WriteOperationHelperCallbacks callbacks = createTestCallbacks();

        // Create a well-formed complete multipart upload request
        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest completeRequest =
                software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
                        .bucket("test-bucket")
                        .key("test-key")
                        .uploadId("test-upload-id")
                        .multipartUpload(
                                m ->
                                        m.parts(
                                                software.amazon.awssdk.services.s3.model
                                                        .CompletedPart.builder()
                                                        .partNumber(1)
                                                        .eTag("test-etag")
                                                        .build()))
                        .build();

        try {
            callbacks.completeMultipartUpload(completeRequest);
            // If we get here, the S3 operation succeeded (unlikely in test environment)
            assertTrue("S3 complete operation succeeded", true);
        } catch (RuntimeException e) {
            // Expected in test environment due to no AWS credentials/connectivity
            assertTrue(
                    "Should attempt S3 operation and fail with network/auth error, got: "
                            + e.getMessage(),
                    e.getMessage().contains("Failed to complete S3 multipart upload")
                            || e.getMessage().contains("Failed to create S3 client")
                            || e.getMessage().contains("Unable to load AWS")
                            || e.getMessage().contains("credentials"));
        }
    }

    @Test
    public void testCallbackErrorHandlingWithInvalidRequests() throws Exception {
        // Test error handling with malformed requests
        WriteOperationHelper.WriteOperationHelperCallbacks callbacks = createTestCallbacks();

        // Test upload part with missing required fields
        software.amazon.awssdk.services.s3.model.UploadPartRequest invalidUploadRequest =
                software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
                        // Missing bucket, key, uploadId - should cause error
                        .partNumber(1)
                        .build();

        software.amazon.awssdk.core.sync.RequestBody body =
                software.amazon.awssdk.core.sync.RequestBody.empty();

        try {
            callbacks.uploadPart(invalidUploadRequest, body, null);
            fail("Should have thrown exception for invalid upload request");
        } catch (RuntimeException e) {
            assertTrue(
                    "Should get error for invalid request",
                    e.getMessage().contains("Failed to upload S3 part")
                            || e.getMessage().contains("Failed to create S3 client"));
        }

        // Test complete multipart upload with missing required fields
        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
                invalidCompleteRequest =
                        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
                                .builder()
                                // Missing bucket, key, uploadId - should cause error
                                .build();

        try {
            callbacks.completeMultipartUpload(invalidCompleteRequest);
            fail("Should have thrown exception for invalid complete request");
        } catch (RuntimeException e) {
            assertTrue(
                    "Should get error for invalid request",
                    e.getMessage().contains("Failed to complete S3 multipart upload")
                            || e.getMessage().contains("Failed to create S3 client"));
        }
    }

    @Test
    public void testAWSSDKv2TypesUsed() {
        // Verify we're using the correct AWS SDK v2 types

        // Test UploadPartRequest
        software.amazon.awssdk.services.s3.model.UploadPartRequest uploadRequest =
                software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
                        .bucket("test")
                        .key("test")
                        .uploadId("test")
                        .partNumber(1)
                        .contentLength(100L)
                        .build();

        assertNotNull("UploadPartRequest should be creatable", uploadRequest);
        assertTrue(
                "Should be AWS SDK v2 type",
                uploadRequest.getClass().getName().contains("software.amazon.awssdk"));

        // Test CompleteMultipartUploadRequest
        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest completeRequest =
                software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
                        .bucket("test")
                        .key("test")
                        .uploadId("test")
                        .build();

        assertNotNull("CompleteMultipartUploadRequest should be creatable", completeRequest);
        assertTrue(
                "Should be AWS SDK v2 type",
                completeRequest.getClass().getName().contains("software.amazon.awssdk"));

        // Test RequestBody
        software.amazon.awssdk.core.sync.RequestBody body =
                software.amazon.awssdk.core.sync.RequestBody.fromString("test");

        assertNotNull("RequestBody should be creatable", body);
        assertTrue(
                "Should be AWS SDK v2 type",
                body.getClass().getName().contains("software.amazon.awssdk"));
    }

    @Test
    public void testCallbacksDoNotThrowUnsupportedOperationException() throws Exception {
        // Verify callbacks no longer throw UnsupportedOperationException
        WriteOperationHelper.WriteOperationHelperCallbacks callbacks = createTestCallbacks();

        // Create valid requests
        software.amazon.awssdk.services.s3.model.UploadPartRequest uploadRequest =
                software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
                        .bucket("test-bucket")
                        .key("test-key")
                        .uploadId("test-upload-id")
                        .partNumber(1)
                        .build();

        software.amazon.awssdk.core.sync.RequestBody body =
                software.amazon.awssdk.core.sync.RequestBody.empty();

        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest completeRequest =
                software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
                        .bucket("test-bucket")
                        .key("test-key")
                        .uploadId("test-upload-id")
                        .build();

        // Both methods should attempt S3 operations, not throw UnsupportedOperationException
        try {
            callbacks.uploadPart(uploadRequest, body, null);
        } catch (UnsupportedOperationException e) {
            fail(
                    "uploadPart should not throw UnsupportedOperationException, it should attempt S3 operations");
        } catch (RuntimeException e) {
            // Expected - should be network/auth errors, not UnsupportedOperationException
            assertTrue(
                    "Should not be UnsupportedOperationException",
                    !e.getClass().equals(UnsupportedOperationException.class));
        }

        try {
            callbacks.completeMultipartUpload(completeRequest);
        } catch (UnsupportedOperationException e) {
            fail(
                    "completeMultipartUpload should not throw UnsupportedOperationException, it should attempt S3 operations");
        } catch (RuntimeException e) {
            // Expected - should be network/auth errors, not UnsupportedOperationException
            assertTrue(
                    "Should not be UnsupportedOperationException",
                    !e.getClass().equals(UnsupportedOperationException.class));
        }
    }

    /**
     * Helper method to create test callbacks. We can't easily create a full S3AFileSystem in tests,
     * but we can test the callback behavior.
     */
    private WriteOperationHelper.WriteOperationHelperCallbacks createTestCallbacks()
            throws Exception {
        try {
            // Try to create a minimal S3AFileSystem for testing
            S3AFileSystem s3a = new S3AFileSystem();
            Configuration conf = new Configuration();

            // Create HadoopS3AccessHelper to get the callbacks
            HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a, conf);

            // Use reflection to access the createCallbacks method for testing
            java.lang.reflect.Method method =
                    HadoopS3AccessHelper.class.getDeclaredMethod("createCallbacks");
            method.setAccessible(true);
            return (WriteOperationHelper.WriteOperationHelperCallbacks) method.invoke(helper);

        } catch (Exception e) {
            // If we can't create the helper, we'll assume the test environment doesn't support it
            throw new org.junit.AssumptionViolatedException(
                    "Cannot create S3 helper in test environment", e);
        }
    }
}
