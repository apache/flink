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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.Base58;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;

/** {@code SeaweedFsTestContainer} provides a {@code SeaweedFs} test instance. */
class SeaweedFsTestContainer extends GenericContainer<SeaweedFsTestContainer> {

    private static final int DEFAULT_PORT = 8333;

    private static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";

    private static final String DEFAULT_STORAGE_DIRECTORY = "/data";

    private final String accessKey;
    private final String secretKey;
    private final String defaultBucketName;

    public SeaweedFsTestContainer() {
        this(randomString("bucket", 6));
    }

    public SeaweedFsTestContainer(String defaultBucketName) {
        super(DockerImageVersions.SEAWEEDFS);

        this.accessKey = randomString("accessKey", 10);
        // secrets must have at least 8 characters
        this.secretKey = randomString("secret", 10);
        this.defaultBucketName = Preconditions.checkNotNull(defaultBucketName);

        withNetworkAliases(randomString("seaweedfs", 6));
        addExposedPort(DEFAULT_PORT);
        withEnv(AWS_ACCESS_KEY_ID, this.accessKey);
        withEnv(AWS_SECRET_ACCESS_KEY, this.secretKey);
        withCommand(
                "mini",
                "-s3.port=" + DEFAULT_PORT,
                "-dir=" + DEFAULT_STORAGE_DIRECTORY,
                "-bucket=" + defaultBucketName);
        // mini pre-creates the bucket and only reports readiness once every component,
        // including that bucket, is available.
        setWaitStrategy(
                new LogMessageWaitStrategy()
                        .withRegEx("(?s).*All enabled components are running and ready to use.*")
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    private static String randomString(String prefix, int length) {
        return String.format("%s-%s", prefix, Base58.randomString(length).toLowerCase(Locale.ROOT));
    }

    /** Creates {@link AmazonS3} client for accessing the {@code SeaweedFs} instance. */
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
     * Initializes the SeaweedFs instance (i.e. creating the default bucket and initializing Flink's
     * FileSystems). Additionally, the passed Flink {@link Configuration} is extended by all
     * relevant parameter to access the {@code SeaweedFs} instance.
     */
    public void setS3ConfigOptions(Configuration config) {
        config.set(AbstractS3FileSystemFactory.ENDPOINT, getHttpEndpoint());
        config.setString("s3.path.style.access", "true");
        config.set(AbstractS3FileSystemFactory.ACCESS_KEY, accessKey);
        config.set(AbstractS3FileSystemFactory.SECRET_KEY, secretKey);
    }

    public void initializeFileSystem(Configuration config) {
        Preconditions.checkArgument(
                config.containsKey(AbstractS3FileSystemFactory.ENDPOINT.key()),
                AbstractS3FileSystemFactory.ENDPOINT.key()
                        + " needs to be specified before initializing the FileSystems.");
        FileSystem.initialize(config, null);
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

    public void writeCredentialsFile(File credentialsFile) throws IOException {
        try (FileWriter writer = new FileWriter(credentialsFile)) {
            writer.write(
                    String.format(
                            "[default]\n"
                                    + "aws_access_key_id = %s\n"
                                    + "aws_secret_access_key = %s\n",
                            accessKey, secretKey));
        }
    }
}
