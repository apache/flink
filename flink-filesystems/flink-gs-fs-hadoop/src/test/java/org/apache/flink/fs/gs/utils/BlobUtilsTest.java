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

package org.apache.flink.fs.gs.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test {@link BlobUtils}. */
class BlobUtilsTest {

    @Test
    void shouldParseValidUri() {
        GSBlobIdentifier blobIdentifier = BlobUtils.parseUri(URI.create("gs://bucket/foo/bar"));
        assertEquals("bucket", blobIdentifier.bucketName);
        assertEquals("foo/bar", blobIdentifier.objectName);
    }

    @Test
    void shouldFailToParseUriBadScheme() {
        assertThatThrownBy(() -> BlobUtils.parseUri(URI.create("s3://bucket/foo/bar")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailToParseUriMissingBucketName() {
        assertThatThrownBy(() -> BlobUtils.parseUri(URI.create("gs:///foo/bar")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailToParseUriMissingObjectName() {
        assertThatThrownBy(() -> BlobUtils.parseUri(URI.create("gs://bucket/")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldUseTemporaryBucketNameIfSpecified() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, "temp");
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");

        String bucketName = BlobUtils.getTemporaryBucketName(identifier, options);
        assertEquals("temp", bucketName);
    }

    @Test
    void shouldUseIdentifierBucketNameNameIfTemporaryBucketNotSpecified() {
        Configuration flinkConfig = new Configuration();
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");

        String bucketName = BlobUtils.getTemporaryBucketName(identifier, options);
        assertEquals("foo", bucketName);
    }

    @Test
    void shouldProperlyConstructTemporaryObjectPartialName() {
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");

        String partialName = BlobUtils.getTemporaryObjectPartialName(identifier);
        assertEquals(".inprogress/foo/bar/", partialName);
    }

    @Test
    void shouldProperlyConstructTemporaryObjectName() {
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        String partialName = BlobUtils.getTemporaryObjectName(identifier, temporaryObjectId);
        assertEquals(".inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47", partialName);
    }

    @Test
    void shouldProperlyConstructTemporaryObjectNameWithEntropy() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(GSFileSystemOptions.ENABLE_FILESINK_ENTROPY, Boolean.TRUE);

        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        String partialName =
                BlobUtils.getTemporaryObjectNameWithEntropy(identifier, temporaryObjectId);
        assertEquals(
                "f09c43e5-ea49-4537-a406-0586f8f09d47.inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47",
                partialName);
    }

    @Test
    void shouldProperlyConstructTemporaryBlobIdentifierWithDefaultBucket() {

        Configuration flinkConfig = new Configuration();
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        GSBlobIdentifier temporaryBlobIdentifier =
                BlobUtils.getTemporaryBlobIdentifier(identifier, temporaryObjectId, options);
        assertEquals("foo", temporaryBlobIdentifier.bucketName);
        assertEquals(
                ".inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47",
                temporaryBlobIdentifier.objectName);
    }

    @Test
    void shouldProperlyConstructTemporaryBlobIdentifierWithTemporaryBucket() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(GSFileSystemOptions.WRITER_TEMPORARY_BUCKET_NAME, "temp");
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        GSBlobIdentifier temporaryBlobIdentifier =
                BlobUtils.getTemporaryBlobIdentifier(identifier, temporaryObjectId, options);
        assertEquals("temp", temporaryBlobIdentifier.bucketName);
        assertEquals(
                ".inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47",
                temporaryBlobIdentifier.objectName);
    }
}
