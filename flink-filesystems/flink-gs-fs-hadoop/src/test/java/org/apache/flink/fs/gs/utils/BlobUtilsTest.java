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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link BlobUtils}. */
class BlobUtilsTest {

    @Test
    void shouldParseValidUri() {
        GSBlobIdentifier blobIdentifier = BlobUtils.parseUri(URI.create("gs://bucket/foo/bar"));
        assertThat(blobIdentifier.bucketName).isEqualTo("bucket");
        assertThat(blobIdentifier.objectName).isEqualTo("foo/bar");
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
        assertThat(bucketName).isEqualTo("temp");
    }

    @Test
    void shouldUseIdentifierBucketNameNameIfTemporaryBucketNotSpecified() {
        Configuration flinkConfig = new Configuration();
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");

        String bucketName = BlobUtils.getTemporaryBucketName(identifier, options);
        assertThat(bucketName).isEqualTo("foo");
    }

    @Test
    void shouldProperlyConstructTemporaryObjectPartialName() {
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");

        String partialName = BlobUtils.getTemporaryObjectPartialName(identifier);
        assertThat(partialName).isEqualTo(".inprogress/foo/bar/");
    }

    @Test
    void shouldProperlyConstructTemporaryObjectName() {
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        String partialName = BlobUtils.getTemporaryObjectName(identifier, temporaryObjectId);
        assertThat(partialName)
                .isEqualTo(".inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47");
    }

    @Test
    void shouldProperlyConstructTemporaryObjectNameWithEntropy() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.set(GSFileSystemOptions.ENABLE_FILESINK_ENTROPY, Boolean.TRUE);

        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        String partialName =
                BlobUtils.getTemporaryObjectNameWithEntropy(identifier, temporaryObjectId);
        assertThat(
                        "f09c43e5-ea49-4537-a406-0586f8f09d47.inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47")
                .isEqualTo(partialName);
    }

    @Test
    void shouldProperlyConstructTemporaryBlobIdentifierWithDefaultBucket() {

        Configuration flinkConfig = new Configuration();
        GSFileSystemOptions options = new GSFileSystemOptions(flinkConfig);
        GSBlobIdentifier identifier = new GSBlobIdentifier("foo", "bar");
        UUID temporaryObjectId = UUID.fromString("f09c43e5-ea49-4537-a406-0586f8f09d47");

        GSBlobIdentifier temporaryBlobIdentifier =
                BlobUtils.getTemporaryBlobIdentifier(identifier, temporaryObjectId, options);
        assertThat(temporaryBlobIdentifier.bucketName).isEqualTo("foo");
        assertThat(temporaryBlobIdentifier.objectName)
                .isEqualTo(".inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47");
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
        assertThat(temporaryBlobIdentifier.bucketName).isEqualTo("temp");
        assertThat(temporaryBlobIdentifier.objectName)
                .isEqualTo(".inprogress/foo/bar/f09c43e5-ea49-4537-a406-0586f8f09d47");
    }
}
