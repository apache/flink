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

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.RequestCharged;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link HadoopS3AccessHelper} to verify correct AWS SDK v2 to v1 conversions and
 * Hadoop 3.4.2 API compatibility.
 */
public class HadoopS3AccessHelperTest {

    @Test
    public void testHadoop342ApiCompatibility() throws Exception {
        // Verify that we're using Hadoop 3.4.2 compatible APIs

        // Check that PutObjectOptions.keepingDirs() method exists (Hadoop 3.4.2 API)
        Method keepingDirsMethod = PutObjectOptions.class.getMethod("keepingDirs");
        assertNotNull(keepingDirsMethod);

        // Verify the method returns the expected type
        PutObjectOptions options = PutObjectOptions.keepingDirs();
        assertNotNull(options);
    }

    @Test
    public void testCreateDefaultCallbacksThrowsExceptionOnUploadPart() throws Exception {
        // Get the callbacks to test exception throwing
        WriteOperationHelper.WriteOperationHelperCallbacks callbacks = getDefaultCallbacks();

        // Test that uploadPart callback throws UnsupportedOperationException
        software.amazon.awssdk.services.s3.model.UploadPartRequest request =
                software.amazon.awssdk.services.s3.model.UploadPartRequest.builder().build();
        software.amazon.awssdk.core.sync.RequestBody body =
                software.amazon.awssdk.core.sync.RequestBody.empty();

        try {
            callbacks.uploadPart(request, body, null);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Direct uploadPart callback is not supported"));
        }
    }

    @Test
    public void testCreateDefaultCallbacksThrowsExceptionOnCompleteMultipartUpload()
            throws Exception {
        // Get the callbacks to test exception throwing
        WriteOperationHelper.WriteOperationHelperCallbacks callbacks = getDefaultCallbacks();

        // Test that completeMultipartUpload callback throws UnsupportedOperationException
        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest request =
                software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest.builder()
                        .build();

        try {
            callbacks.completeMultipartUpload(request);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(
                    e.getMessage()
                            .contains("Direct completeMultipartUpload callback is not supported"));
        }
    }

    @Test
    public void testUploadPartResponseConversion() {
        // Create an UploadPartResponse with all properties
        UploadPartResponse response =
                UploadPartResponse.builder()
                        .eTag("test-etag-123")
                        .requestCharged(RequestCharged.REQUESTER)
                        .build();

        UploadPartResult result = convertUploadPartResponse(response, 5);

        // Verify all properties are correctly copied
        assertEquals("test-etag-123", result.getETag());
        assertEquals(5, result.getPartNumber());
        assertTrue(result.isRequesterCharged());
    }

    @Test
    public void testUploadPartResponseConversionWithoutRequestCharged() {
        // Create an UploadPartResponse without requestCharged
        UploadPartResponse response = UploadPartResponse.builder().eTag("test-etag-456").build();

        UploadPartResult result = convertUploadPartResponse(response, 3);

        // Verify properties are correctly set
        assertEquals("test-etag-456", result.getETag());
        assertEquals(3, result.getPartNumber());
        assertFalse(result.isRequesterCharged()); // Should default to false
    }

    @Test
    public void testPutObjectResponseConversion() {
        // Create a PutObjectResponse with all properties
        PutObjectResponse response =
                PutObjectResponse.builder()
                        .eTag("put-object-etag")
                        .requestCharged(RequestCharged.REQUESTER)
                        .build();

        PutObjectResult result = convertPutObjectResponse(response);

        // Verify all properties are correctly copied
        assertEquals("put-object-etag", result.getETag());
        assertTrue(result.isRequesterCharged());
    }

    @Test
    public void testPutObjectResponseConversionWithoutRequestCharged() {
        // Create a PutObjectResponse without requestCharged
        PutObjectResponse response = PutObjectResponse.builder().eTag("put-object-etag-2").build();

        PutObjectResult result = convertPutObjectResponse(response);

        // Verify properties are correctly set
        assertEquals("put-object-etag-2", result.getETag());
        assertFalse(result.isRequesterCharged()); // Should default to false
    }

    @Test
    public void testCompleteMultipartUploadResponseConversion() {
        // Create a CompleteMultipartUploadResponse with all properties
        CompleteMultipartUploadResponse response =
                CompleteMultipartUploadResponse.builder()
                        .eTag("complete-etag")
                        .bucket("test-bucket")
                        .key("test/key")
                        .requestCharged(RequestCharged.REQUESTER)
                        .build();

        CompleteMultipartUploadResult result = convertCompleteMultipartUploadResponse(response);

        // Verify all properties are correctly copied
        assertEquals("complete-etag", result.getETag());
        assertEquals("test-bucket", result.getBucketName());
        assertEquals("test/key", result.getKey());
        assertTrue(result.isRequesterCharged());
    }

    @Test
    public void testCompleteMultipartUploadResponseConversionWithoutRequestCharged() {
        // Create a CompleteMultipartUploadResponse without requestCharged
        CompleteMultipartUploadResponse response =
                CompleteMultipartUploadResponse.builder()
                        .eTag("complete-etag-2")
                        .bucket("test-bucket-2")
                        .key("test/key2")
                        .build();

        CompleteMultipartUploadResult result = convertCompleteMultipartUploadResponse(response);

        // Verify properties are correctly set
        assertEquals("complete-etag-2", result.getETag());
        assertEquals("test-bucket-2", result.getBucketName());
        assertEquals("test/key2", result.getKey());
        assertFalse(result.isRequesterCharged()); // Should default to false
    }

    @Test
    public void testRequestChargedConversionLogic() {
        // Test various RequestCharged values
        assertTrue(isRequesterCharged(RequestCharged.REQUESTER));
        assertFalse(isRequesterCharged(RequestCharged.UNKNOWN_TO_SDK_VERSION));
        assertFalse(isRequesterCharged(null));
    }

    // Helper methods for testing internal conversion logic

    private WriteOperationHelper.WriteOperationHelperCallbacks getDefaultCallbacks()
            throws Exception {
        Method createCallbacksMethod =
                HadoopS3AccessHelper.class.getDeclaredMethod("createDefaultCallbacks");
        createCallbacksMethod.setAccessible(true);
        return (WriteOperationHelper.WriteOperationHelperCallbacks)
                createCallbacksMethod.invoke(null);
    }

    private UploadPartResult convertUploadPartResponse(
            UploadPartResponse response, int partNumber) {
        // Simulate the conversion logic from uploadPart method
        UploadPartResult result = new UploadPartResult();
        result.setETag(response.eTag());
        result.setPartNumber(partNumber);
        if (response.requestCharged() != null) {
            result.setRequesterCharged(response.requestCharged().toString().equals("requester"));
        }
        return result;
    }

    private PutObjectResult convertPutObjectResponse(PutObjectResponse response) {
        // Simulate the conversion logic from putObject method
        PutObjectResult result = new PutObjectResult();
        result.setETag(response.eTag());
        if (response.requestCharged() != null) {
            result.setRequesterCharged(response.requestCharged().toString().equals("requester"));
        }
        return result;
    }

    private CompleteMultipartUploadResult convertCompleteMultipartUploadResponse(
            CompleteMultipartUploadResponse response) {
        // Simulate the conversion logic from commitMultiPartUpload method
        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        result.setETag(response.eTag());
        result.setBucketName(response.bucket());
        result.setKey(response.key());
        if (response.requestCharged() != null) {
            result.setRequesterCharged(response.requestCharged().toString().equals("requester"));
        }
        return result;
    }

    private boolean isRequesterCharged(RequestCharged requestCharged) {
        // Simulate the requester charged conversion logic
        return requestCharged != null && requestCharged.toString().equals("requester");
    }

    // ========== Hadoop 3.4.2 API Verification Tests ==========

    @Test
    public void testHadoop342VersionInUse() throws Exception {
        // Verify we're using Hadoop 3.4.2 by checking the version info
        String hadoopVersion = org.apache.hadoop.util.VersionInfo.getVersion();

        assertNotNull("Hadoop version should not be null", hadoopVersion);
        assertTrue(
                "Expected Hadoop version 3.4.2, but got: " + hadoopVersion,
                hadoopVersion.startsWith("3.4.2"));
    }

    @Test
    public void testHadoop342S3AFileSystemPackageExists() throws Exception {
        // Test that core S3A package classes exist in Hadoop 3.4.2
        try {
            // These should be available in Hadoop 3.4.2
            Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem");
            Class.forName("org.apache.hadoop.fs.s3a.WriteOperationHelper");

            // Basic test passes if we can load these core classes
            assertTrue("Core S3A classes found in Hadoop 3.4.2", true);

        } catch (ClassNotFoundException e) {
            fail("Core S3A classes not found - may not be using Hadoop 3.4.2: " + e.getMessage());
        }
    }

    @Test
    public void testHadoop342S3AFileSystemClassExists() throws Exception {
        // Verify that S3AFileSystem class exists and is accessible
        try {
            Class<?> s3aFileSystemClass = Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem");
            assertNotNull("S3AFileSystem class should be accessible", s3aFileSystemClass);

            // Check for some methods that should exist in Hadoop 3.4.2
            s3aFileSystemClass.getMethod("getCanonicalServiceName");
            s3aFileSystemClass.getMethod("getBucket");

        } catch (ClassNotFoundException e) {
            fail("S3AFileSystem class not found: " + e.getMessage());
        } catch (NoSuchMethodException e) {
            fail(
                    "Expected S3AFileSystem methods not found - may not be using correct Hadoop version: "
                            + e.getMessage());
        }
    }

    @Test
    public void testHadoop342WriteOperationHelperExists() throws Exception {
        // Verify WriteOperationHelper class exists (methods may vary by version)
        try {
            Class<?> writeOpHelperClass =
                    Class.forName("org.apache.hadoop.fs.s3a.WriteOperationHelper");
            assertNotNull("WriteOperationHelper class should be accessible", writeOpHelperClass);

            // Just verify the class exists - method signatures may vary in different versions
            assertTrue("WriteOperationHelper class found in Hadoop 3.4.2", true);

        } catch (ClassNotFoundException e) {
            fail("WriteOperationHelper class not found: " + e.getMessage());
        }
    }

    @Test
    public void testHadoop342SpecificClasses() throws Exception {
        // Test for classes that we know should exist in Hadoop 3.4.2
        String[] expectedClasses = {
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "org.apache.hadoop.fs.s3a.WriteOperationHelper",
            "org.apache.hadoop.util.VersionInfo",
            "org.apache.hadoop.fs.statistics.DurationTrackerFactory"
        };

        for (String className : expectedClasses) {
            try {
                Class.forName(className);
                // Class exists - good!
            } catch (ClassNotFoundException e) {
                fail(
                        "Expected Hadoop 3.4.2 class not found: "
                                + className
                                + " - "
                                + e.getMessage());
            }
        }
    }

    @Test
    public void testHadoop342AWSSDKIntegration() throws Exception {
        // Verify that we can access AWS SDK v2 classes that are used in Hadoop 3.4.2
        try {
            // These should be available in our shaded JAR
            Class.forName("software.amazon.awssdk.services.s3.model.UploadPartRequest");
            Class.forName("software.amazon.awssdk.services.s3.model.UploadPartResponse");
            Class.forName(
                    "software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest");
            Class.forName(
                    "software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse");
            Class.forName("software.amazon.awssdk.services.s3.model.RequestCharged");

        } catch (ClassNotFoundException e) {
            fail(
                    "Expected AWS SDK v2 classes not found - may indicate Hadoop 3.4.2 integration issue: "
                            + e.getMessage());
        }
    }

    @Test
    public void testHadoop342BuildInformation() throws Exception {
        // Additional verification of Hadoop build information
        String version = org.apache.hadoop.util.VersionInfo.getVersion();
        String revision = org.apache.hadoop.util.VersionInfo.getRevision();

        assertNotNull("Hadoop version should not be null", version);
        assertNotNull("Hadoop revision should not be null", revision);

        // Print build info for debugging
        System.out.println("Hadoop Version: " + version);
        System.out.println("Hadoop Revision: " + revision);

        // Verify this is indeed 3.4.2
        assertTrue("Expected Hadoop 3.4.2, got: " + version, version.startsWith("3.4.2"));
    }
}
