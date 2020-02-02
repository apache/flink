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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link S3Resource} that accesses AWS S3. This resource is stateless but should still be used as a JUnit resource.
 */
public class AWSS3Resource implements S3Resource {
	private static final String ACCESS_KEY = System.getenv("IT_CASE_S3_ACCESS_KEY");
	private static final String SECRET_KEY = System.getenv("IT_CASE_S3_SECRET_KEY");
	private static final String BUCKET = System.getenv("IT_CASE_S3_BUCKET");

	@Override
	public AmazonS3 getClient() {
		return AmazonS3ClientBuilder
			.standard()
			.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
			.withPathStyleAccessEnabled(true)
			.build();
	}

	@Override
	public Configuration getFlinkConfiguration() {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString("s3.access-key", ACCESS_KEY);
		flinkConfig.setString("s3.secret-key", SECRET_KEY);
		return flinkConfig;
	}

	@Override
	public String getTestBucket() {
		return BUCKET;
	}

	@Override
	public String getTestPrefix() {
		return "temp/";
	}

	@Override
	public void before() {
		// not needing any resources
	}

	@Override
	public void afterTestSuccess() {
		// not needing any resources
	}

	/**
	 * The factory for AWSS3Resource, which only succeeds if the environment variables IT_CASE_S3_ACCESS_KEY,
	 * IT_CASE_S3_SECRET_KEY, and IT_CASE_S3_BUCKET are present.
	 */
	public static final class Factory implements S3Resource.Factory {
		private static final Logger LOG = LoggerFactory.getLogger(Factory.class);

		@Override
		public Optional<S3Resource> create() {
			if (ACCESS_KEY == null || SECRET_KEY == null || BUCKET == null) {
				String[] missingEnvs = {
					ACCESS_KEY == null ? "IT_CASE_S3_ACCESS_KEY" : null,
					SECRET_KEY == null ? "IT_CASE_S3_SECRET_KEY" : null,
					BUCKET == null ? "IT_CASE_S3_BUCKET" : null};
				LOG.info("Missing the following environment variables to initialize AWS S3 {}",
					Arrays.stream(missingEnvs).filter(Objects::nonNull).collect(Collectors.joining(", ")));
				return Optional.empty();
			}
			LOG.info("Created {}.", AWSS3Resource.class.getSimpleName());
			return Optional.of(new AWSS3Resource());
		}
	}
}
