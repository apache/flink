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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.Preconditions;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.Base58;

import java.time.Duration;
import java.util.Locale;

/** {@code MinioTestContainer} provides a {@code Minio} test instance. */
public class MinioTestContainer extends GenericContainer<MinioTestContainer> {

    private static final String FLINK_CONFIG_S3_ENDPOINT = "s3.endpoint";

    private static final int DEFAULT_PORT = 9000;

    private static final String MINIO_ACCESS_KEY = "MINIO_ROOT_USER";
    private static final String MINIO_SECRET_KEY = "MINIO_ROOT_PASSWORD";

    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";

    private final String accessKey;
    private final String secretKey;
    private final String defaultBucketName;

    public MinioTestContainer() {
        this(randomString("bucket", 6));
    }

    public MinioTestContainer(String defaultBucketName) {
        super(DockerImageVersions.MINIO);

        this.accessKey = randomString("accessKey", 10);
        // secrets must have at least 8 characters
        this.secretKey = randomString("secret", 10);
        this.defaultBucketName = Preconditions.checkNotNull(defaultBucketName);

        withNetworkAliases(randomString("minio", 6));
        addExposedPort(DEFAULT_PORT);
        withEnv(MINIO_ACCESS_KEY, this.accessKey);
        withEnv(MINIO_SECRET_KEY, this.secretKey);
        withCommand("server", DEFAULT_STORAGE_DIRECTORY);
        setWaitStrategy(
                new HttpWaitStrategy()
                        .forPort(DEFAULT_PORT)
                        .forPath(HEALTH_ENDPOINT)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        // Very rarely, a 503 status will be returned continuously while the container is
        // starting up, slipping past the AmazonS3 client's default retry strategy.
        withStartupAttempts(3);
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        createDefaultBucket();
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }

    /** Creates {@link AmazonS3} client for accessing the {@code Minio} instance. */
    public AmazonS3 getClient() {
        return AmazonS3Client.builder()
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(accessKey, secretKey)))
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                getHttpEndpoint(), "unused-region"))
                .build();
    }

    private String getHttpEndpoint() {
        return String.format("http://%s:%s", getHost(), getMappedPort(DEFAULT_PORT));
    }

    /**
     * Initializes the Minio instance (i.e. creating the default bucket and initializing Flink's
     * FileSystems). Additionally, the passed Flink {@link Configuration} is extended by all
     * relevant parameter to access the {@code Minio} instance.
     */
    public void setS3ConfigOptions(Configuration config) {
        config.setString(FLINK_CONFIG_S3_ENDPOINT, getHttpEndpoint());
        config.setString("s3.path.style.access", "true");
        config.setString("s3.access.key", accessKey);
        config.setString("s3.secret.key", secretKey);
    }

    public void initializeFileSystem(Configuration config) {
        Preconditions.checkArgument(
                config.containsKey(FLINK_CONFIG_S3_ENDPOINT),
                FLINK_CONFIG_S3_ENDPOINT
                        + " needs to be specified before initializing the FileSystems.");
        FileSystem.initialize(config, null);
    }

    private void createDefaultBucket() {
        getClient().createBucket(defaultBucketName);
    }

    /** Returns the internally used default bucket. */
    public String getDefaultBucketName() {
        return defaultBucketName;
    }

    /**
     * Returns the S3 URI for the default bucket. This can be used to create the HA storage
     * directory path.
     */
    public String getS3UriForDefaultBucket() {
        return "s3://" + getDefaultBucketName();
    }
}
