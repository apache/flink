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

package org.apache.flink.fs.s3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test for S3 FileSystem using MinIO.
 *
 * <p>This test demonstrates:
 *
 * <ul>
 *   <li>Starting an embedded MinIO container
 *   <li>Configuring Flink to use S3 with MinIO endpoint
 *   <li>Writing data to S3 using FileSink
 *   <li>Reading and verifying data from S3
 * </ul>
 */
class S3FileSystemMinioTest {

    private static final int DEFAULT_PORT = 9000;
    private static final String HEALTH_ENDPOINT = "/minio/health/ready";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String MINIO_IMAGE = "minio/minio:RELEASE.2022-02-07T08-17-33Z";

    private static GenericContainer<?> minioContainer;
    private static AmazonS3 s3Client;
    private static String minioEndpoint;

    @BeforeAll
    static void setUp() {
        // Start MinIO container
        minioContainer = startMinioContainer();
        minioEndpoint = getMinioEndpoint(minioContainer);

        // Create S3 client and bucket
        s3Client = createS3Client(minioEndpoint);
        s3Client.createBucket(BUCKET_NAME);
    }

    @AfterAll
    static void tearDown() {
        if (minioContainer != null) {
            minioContainer.stop();
        }
    }

    @Test
    void testS3FileSystemWithMinio() throws Exception {
        String s3Prefix = "flink-test-data/";

        // Run Flink job to write data to S3
        runFlinkJobToS3(minioEndpoint, s3Prefix);

        // List objects in S3 to verify they were written
        List<S3ObjectSummary> objects =
                s3Client.listObjects(BUCKET_NAME, s3Prefix).getObjectSummaries();

        assertThat(objects).as("Objects should be written to S3").isNotEmpty();
        assertThat(objects).as("Expected 5 objects in S3").hasSize(5);

        // Verify content of objects
        verifyS3Objects(s3Client, s3Prefix, objects);

        // Delete objects to confirm deletion works
        for (S3ObjectSummary obj : objects) {
            s3Client.deleteObject(BUCKET_NAME, obj.getKey());
        }

        // Verify deletion
        objects = s3Client.listObjects(BUCKET_NAME, s3Prefix).getObjectSummaries();
        assertThat(objects).as("Objects should be deleted from S3").isEmpty();
    }

    private static GenericContainer<?> startMinioContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(MINIO_IMAGE)
                        .withExposedPorts(DEFAULT_PORT)
                        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                        .withCommand("server", "/data")
                        .waitingFor(
                                new HttpWaitStrategy()
                                        .forPort(DEFAULT_PORT)
                                        .forPath(HEALTH_ENDPOINT)
                                        .withStartupTimeout(Duration.ofMinutes(2)));

        container.start();
        return container;
    }

    private static String getMinioEndpoint(GenericContainer<?> container) {
        return String.format(
                "http://%s:%d", container.getHost(), container.getMappedPort(DEFAULT_PORT));
    }

    private static AmazonS3 createS3Client(String endpoint) {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .build();
    }

    private static void runFlinkJobToS3(String minioEndpoint, String s3Prefix) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // Generate test data using custom source
        DataStream<Tuple2<Integer, String>> source = env.addSource(new SimpleTestDataSource());

        // Write to S3 using custom S3 sink
        source.addSink(new S3WriterSink(minioEndpoint, s3Prefix));

        env.execute("S3 FileSystem MinIO Test");
    }

    private static void verifyS3Objects(
            AmazonS3 s3Client, String s3Prefix, List<S3ObjectSummary> objects) throws Exception {
        // Read and verify content
        int totalRecords = 0;
        for (S3ObjectSummary objSummary : objects) {
            S3Object s3Object = s3Client.getObject(BUCKET_NAME, objSummary.getKey());
            try (BufferedReader reader =
                    new BufferedReader(
                            new java.io.InputStreamReader(
                                    s3Object.getObjectContent(), StandardCharsets.UTF_8))) {
                String content = reader.lines().collect(Collectors.joining("\n"));
                if (!content.isEmpty()) {
                    totalRecords++;
                }
            }
        }

        assertThat(totalRecords).as("Expected 5 records in S3").isEqualTo(5);
    }

    /** Simple test data source that emits a fixed set of records. */
    private static class SimpleTestDataSource implements SourceFunction<Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            String[] messages = {
                "Hello S3", "Testing MinIO", "Flink rocks", "End-to-end test", "Success!"
            };

            for (int i = 0; i < messages.length && running; i++) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(Tuple2.of(i + 1, messages[i]));
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /** Custom sink that writes directly to S3 using AWS SDK. */
    private static class S3WriterSink
            implements org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction<
                    Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;
        private final String minioEndpoint;
        private final String s3Prefix;
        private transient AmazonS3 s3Client;

        public S3WriterSink(String minioEndpoint, String s3Prefix) {
            this.minioEndpoint = minioEndpoint;
            this.s3Prefix = s3Prefix;
        }

        @Override
        public void invoke(Tuple2<Integer, String> value) throws Exception {
            if (s3Client == null) {
                s3Client = createS3Client(minioEndpoint);
            }

            String key = s3Prefix + "record-" + value.f0 + ".txt";
            String content = value.f0 + "," + value.f1;

            s3Client.putObject(BUCKET_NAME, key, content);
        }
    }
}
