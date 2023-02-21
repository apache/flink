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

package org.apache.flink.fs.s3.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.core.testutils.TestContainerExtension;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code MinioTestContainerTest} tests some basic functionality provided by {@link
 * MinioTestContainer}.
 */
public class MinioTestContainerTest {

    private static final String DEFAULT_BUCKET_NAME = "test-bucket";

    @RegisterExtension
    private static final EachCallbackWrapper<TestContainerExtension<MinioTestContainer>>
            MINIO_EXTENSION =
                    new EachCallbackWrapper<>(
                            new TestContainerExtension<>(
                                    () -> new MinioTestContainer(DEFAULT_BUCKET_NAME)));

    private static MinioTestContainer getTestContainer() {
        return MINIO_EXTENSION.getCustomExtension().getTestContainer();
    }

    private static AmazonS3 getClient() {
        return getTestContainer().getClient();
    }

    @Test
    public void testBucketCreation() {
        final String bucketName = "other-bucket";
        final Bucket otherBucket = getClient().createBucket(bucketName);

        assertThat(otherBucket).isNotNull();
        assertThat(otherBucket).extracting(Bucket::getName).isEqualTo(bucketName);

        assertThat(getClient().listBuckets())
                .map(Bucket::getName)
                .containsExactlyInAnyOrder(getTestContainer().getDefaultBucketName(), bucketName);
    }

    @Test
    public void testPutObject() throws IOException {
        final String bucketName = "other-bucket";

        getClient().createBucket(bucketName);
        final String objectId = "test-object";
        final String content = "test content";
        getClient().putObject(bucketName, objectId, content);

        final BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                getClient().getObject(bucketName, objectId).getObjectContent()));
        assertThat(reader.readLine()).isEqualTo(content);
    }

    @Test
    public void testSetS3ConfigOptions() {
        final Configuration config = new Configuration();
        getTestContainer().setS3ConfigOptions(config);

        assertThat(config.containsKey("s3.endpoint")).isTrue();
        assertThat(config.containsKey("s3.path.style.access")).isTrue();
        assertThat(config.containsKey("s3.access.key")).isTrue();
        assertThat(config.containsKey("s3.secret.key")).isTrue();
    }

    @Test
    public void testGetDefaultBucketName() {
        assertThat(getTestContainer().getDefaultBucketName()).isEqualTo(DEFAULT_BUCKET_NAME);
    }

    @Test
    public void testDefaultBucketCreation() {
        assertThat(getClient().listBuckets())
                .singleElement()
                .extracting(Bucket::getName)
                .isEqualTo(getTestContainer().getDefaultBucketName());
    }

    @Test
    public void testS3EndpointNeedsToBeSpecifiedBeforeInitializingFileSyste() {
        assertThatThrownBy(() -> getTestContainer().initializeFileSystem(new Configuration()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
