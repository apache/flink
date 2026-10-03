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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.Bucket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Basic tests for {@link SeaweedFsNativeS3TestContainer}. */
class SeaweedFsNativeS3TestContainerTest {

    private static final String DEFAULT_BUCKET_NAME = "test-bucket";

    @RegisterExtension
    private static final EachCallbackWrapper<TestContainerExtension<SeaweedFsNativeS3TestContainer>>
            SEAWEEDFS_EXTENSION =
                    new EachCallbackWrapper<>(
                            new TestContainerExtension<>(
                                    () -> new SeaweedFsNativeS3TestContainer(DEFAULT_BUCKET_NAME)));

    private static SeaweedFsNativeS3TestContainer getTestContainer() {
        return SEAWEEDFS_EXTENSION.getCustomExtension().getTestContainer();
    }

    @Test
    void testBucketCreation() {
        final String bucketName = "other-bucket";
        getTestContainer().getClient().createBucket(b -> b.bucket(bucketName));

        assertThat(getTestContainer().getClient().listBuckets().buckets())
                .map(Bucket::name)
                .containsExactlyInAnyOrder(getTestContainer().getDefaultBucketName(), bucketName);
    }

    @Test
    void testPutObject() {
        final String key = "test-object";
        final String content = "test content";
        getTestContainer()
                .getClient()
                .putObject(
                        b -> b.bucket(getTestContainer().getDefaultBucketName()).key(key),
                        RequestBody.fromString(content));

        assertThat(getTestContainer().getObjectAsString(key)).isEqualTo(content);
    }

    @Test
    void testSetS3ConfigOptions() {
        final Configuration config = new Configuration();
        getTestContainer().setS3ConfigOptions(config);

        assertThat(config.containsKey(NativeS3FileSystemFactory.ENDPOINT.key())).isTrue();
        assertThat(config.containsKey(NativeS3FileSystemFactory.REGION.key())).isTrue();
        assertThat(config.containsKey(NativeS3FileSystemFactory.ACCESS_KEY.key())).isTrue();
        assertThat(config.containsKey(NativeS3FileSystemFactory.SECRET_KEY.key())).isTrue();
        assertThat(config.containsKey(NativeS3FileSystemFactory.PATH_STYLE_ACCESS.key())).isTrue();
        assertThat(config.containsKey(NativeS3FileSystemFactory.CHUNKED_ENCODING_ENABLED.key()))
                .isTrue();
        assertThat(config.containsKey(NativeS3FileSystemFactory.CHECKSUM_VALIDATION_ENABLED.key()))
                .isTrue();
    }

    @Test
    void testGetDefaultBucketName() {
        assertThat(getTestContainer().getDefaultBucketName()).isEqualTo(DEFAULT_BUCKET_NAME);
    }

    @Test
    void testDefaultBucketCreation() {
        assertThat(getTestContainer().getClient().listBuckets().buckets())
                .singleElement()
                .extracting(Bucket::name)
                .isEqualTo(getTestContainer().getDefaultBucketName());
    }

    @Test
    void testEndpointRequiredBeforeInitializingFileSystem() {
        assertThatThrownBy(() -> getTestContainer().initializeFileSystem(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
