/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util.s3;

import org.apache.flink.configuration.Configuration;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;
import java.util.Optional;

/**
 * {@link S3Resource} that sets up docker-based Minio.
 */
public class MinioS3Resource implements S3Resource {
	private static final String ACCESS_KEY = "access_key";
	private static final String SECRET_KEY = "secret_key";
	private static final int MINIO_PORT = 9000;
	private static final Logger LOG = LoggerFactory.getLogger(MinioS3Resource.class);

	private GenericContainer minioServer = new GenericContainer("minio/minio")
		.withEnv("MINIO_ACCESS_KEY", ACCESS_KEY)
		.withEnv("MINIO_SECRET_KEY", SECRET_KEY)
		.withEnv("MINIO_DOMAIN", "localhost")
		.withCommand("server /data")
		.withExposedPorts(MINIO_PORT)
		.waitingFor(new HttpWaitStrategy()
			.forPath("/minio/health/ready")
			.forPort(MINIO_PORT)
			.withStartupTimeout(Duration.ofSeconds(10)));
	private AmazonS3 client;

	@Override
	public AmazonS3 getClient() {
		return client;
	}

	@Override
	public Configuration getFlinkConfiguration() {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString("s3.access-key", ACCESS_KEY);
		flinkConfig.setString("s3.secret-key", SECRET_KEY);
		flinkConfig.setString("s3.endpoint", getServiceEndpoint());
		flinkConfig.setString("s3.path.style.access", "true");
		flinkConfig.setString("s3.path-style-access", "true");
		return flinkConfig;
	}

	/**
	 * Returns the service endpoint for the s3 server. Communication with the server through {@link #getClient()} is
	 * preferred.
	 */
	public String getServiceEndpoint() {
		return "http://localhost:" + minioServer.getFirstMappedPort();
	}

	@Override
	public void before() {
		minioServer.start();
		final String serviceEndpoint = "http://localhost:" + minioServer.getFirstMappedPort();
		LOG.info("Started s3 @ " + serviceEndpoint);

		client = AmazonS3ClientBuilder
			.standard()
			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
				serviceEndpoint,
				"us-east-1"))
			.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
			.withPathStyleAccessEnabled(true)
			.build();
		getClient().createBucket(getTestBucket());
	}

	@Override
	public void afterTestSuccess() {
		minioServer.stop();
	}

	/**
	 * The factory for MinioS3Resource.
	 */
	public static final class Factory implements S3Resource.Factory {
		private static final Logger LOG = LoggerFactory.getLogger(Factory.class);

		@Override
		public Optional<S3Resource> create() {
			LOG.info("Created {}.", MinioS3Resource.class.getSimpleName());
			return Optional.of(new MinioS3Resource());
		}
	}
}
