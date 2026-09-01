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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.Preconditions;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;

/** Provides a SeaweedFS S3-compatible test instance for the native S3 filesystem. */
public class SeaweedFsNativeS3TestContainer
        extends GenericContainer<SeaweedFsNativeS3TestContainer> {

    private static final int DEFAULT_PORT = 8333;
    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/healthz";
    private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";

    private final String accessKey;
    private final String secretKey;
    private final String defaultBucketName;

    private S3Client client;

    public SeaweedFsNativeS3TestContainer() {
        this(randomString("bucket", 6));
    }

    public SeaweedFsNativeS3TestContainer(String defaultBucketName) {
        super(DockerImageVersions.SEAWEEDFS);

        this.accessKey = randomString("accessKey", 10);
        // secrets must have at least 8 characters
        this.secretKey = randomString("secretKey", 10);
        this.defaultBucketName = Preconditions.checkNotNull(defaultBucketName);

        withNetworkAliases(randomString("seaweedfs", 6));
        addExposedPort(DEFAULT_PORT);
        withEnv(AWS_ACCESS_KEY_ID, accessKey);
        withEnv(AWS_SECRET_ACCESS_KEY, secretKey);
        withCommand(
                "server", "-s3", "-s3.port=" + DEFAULT_PORT, "-dir=" + DEFAULT_STORAGE_DIRECTORY);
        setWaitStrategy(
                new HttpWaitStrategy()
                        .forPort(DEFAULT_PORT)
                        .forPath(HEALTH_ENDPOINT)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        // A transient 503 during startup can slip past the SDK's default retry strategy.
        withStartupAttempts(3);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        getClient().createBucket(b -> b.bucket(defaultBucketName));
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
            client = null;
        }
        super.stop();
    }

    /** Returns a vanilla SDK-v2 client for verification, independent of the code under test. */
    public S3Client getClient() {
        if (client == null) {
            client =
                    S3Client.builder()
                            .endpointOverride(URI.create(getHttpEndpoint()))
                            .region(Region.US_EAST_1)
                            .credentialsProvider(
                                    StaticCredentialsProvider.create(
                                            AwsBasicCredentials.create(accessKey, secretKey)))
                            .forcePathStyle(true)
                            .build();
        }
        return client;
    }

    /**
     * Sets the config required to reach this instance from the native S3 filesystem. SeaweedFS
     * supports neither AWS chunked encoding nor trailing checksums, so both are disabled.
     */
    public void setS3ConfigOptions(Configuration config) {
        config.set(NativeS3FileSystemFactory.ENDPOINT, getHttpEndpoint());
        config.set(NativeS3FileSystemFactory.REGION, Region.US_EAST_1.id());
        config.set(NativeS3FileSystemFactory.ACCESS_KEY, accessKey);
        config.set(NativeS3FileSystemFactory.SECRET_KEY, secretKey);
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, true);
        config.set(NativeS3FileSystemFactory.CHUNKED_ENCODING_ENABLED, false);
        config.set(NativeS3FileSystemFactory.CHECKSUM_VALIDATION_ENABLED, false);
    }

    public void initializeFileSystem(Configuration config) {
        Preconditions.checkArgument(
                config.containsKey(NativeS3FileSystemFactory.ENDPOINT.key()),
                NativeS3FileSystemFactory.ENDPOINT.key()
                        + " needs to be specified before initializing the FileSystems.");
        FileSystem.initialize(config, null);
    }

    /** Returns the internally used default bucket. */
    public String getDefaultBucketName() {
        return defaultBucketName;
    }

    public String getS3UriForDefaultBucket() {
        return "s3://" + defaultBucketName;
    }

    public List<S3Object> listObjects(String prefix) {
        return getClient()
                .listObjectsV2(b -> b.bucket(defaultBucketName).prefix(prefix))
                .contents();
    }

    public String getObjectAsString(String key) {
        return getClient()
                .getObjectAsBytes(b -> b.bucket(defaultBucketName).key(key))
                .asUtf8String();
    }

    private String getHttpEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_PORT));
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }
}
