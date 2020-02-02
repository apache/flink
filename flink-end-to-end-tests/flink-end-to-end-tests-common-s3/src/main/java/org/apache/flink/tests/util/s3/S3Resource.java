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
import org.apache.flink.tests.util.util.FactoryUtils;
import org.apache.flink.util.ExternalResource;

import com.amazonaws.services.s3.AmazonS3;

import java.net.URI;
import java.util.Optional;

/**
 * Generic interface for interacting with S3.
 */
public interface S3Resource extends ExternalResource {
	/**
	 * Returns an S3 client that can be used to communicate with this resource.
	 */
	AmazonS3 getClient();

	/**
	 * The configurations option to add to the Flink application, such that this resource can be used through Flink.
	 */
	Configuration getFlinkConfiguration();

	/**
	 * The bucket that should be used by default. Depending on the underlying implementation this may be the only
	 * bucket with write access.
	 */
	default String getTestBucket() {
		return "test-bucket";
	}

	/**
	 * The prefix that should be used by default. Depending on the underlying implementation this may be the only
	 * prefix with write access.
	 */
	default String getTestPrefix() {
		return "";
	}

	/**
	 * Returns a full URI using {@link #getTestBucket()} and {@link #getTestPrefix()}, such that it is guaranteed
	 * to be writable.
	 */
	default URI getFullUri(String path) {
		return URI.create("s3://" + getTestBucket() + "/" + getFullKey(path));
	}

	/**
	 * Returns the full key using {@link #getTestPrefix()}, such that it is guaranteed to be writable under
	 * {@link #getTestBucket()}.
	 */
	default String getFullKey(String path) {
		return getTestPrefix() + path;
	}

	/**
	 * Returns the configured S3Resource implementation, or a {@link MinioS3Resource} if none is configured.
	 */
	static S3Resource get() {
		return FactoryUtils.loadAndInvokeFactory(
			Factory.class,
			Factory::create,
			MinioS3Resource.Factory::new);
	}

	/**
	 * A factory for {@link S3Resource} implementations.
	 */
	@FunctionalInterface
	interface Factory {

		/**
		 * Returns a {@link S3Resource} instance. If the instance could not be instantiated (for example, because a
		 * mandatory parameter was missing), then an empty {@link Optional} should be returned.
		 *
		 * @return S3Resource instance, or an empty Optional if the instance could not be instantiated
		 */
		Optional<S3Resource> create();
	}
}
