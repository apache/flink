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

package org.apache.flink.testutils.oss;

import org.junit.Assume;

import javax.annotation.Nullable;

/**
 * Access to credentials to access OSS buckets during integration tests.
 */
public class OSSTestCredentials {
	@Nullable
	private static final String ENDPOINT = System.getenv("ARTIFACTS_OSS_ENDPOINT");

	@Nullable
	private static final String BUCKET = System.getenv("ARTIFACTS_OSS_BUCKET");

	@Nullable
	private static final String ACCESS_KEY = System.getenv("ARTIFACTS_OSS_ACCESS_KEY");

	@Nullable
	private static final String SECRET_KEY = System.getenv("ARTIFACTS_OSS_SECRET_KEY");

	// ------------------------------------------------------------------------

	public static boolean credentialsAvailable() {
		return ENDPOINT != null && BUCKET != null && ACCESS_KEY != null && SECRET_KEY != null;
	}

	public static void assumeCredentialsAvailable() {
		Assume.assumeTrue("No OSS credentials available in this test's environment", credentialsAvailable());
	}

	/**
	 * Get OSS endpoint used to connect.
	 * @return OSS endpoint
	 */
	public static String getOSSEndpoint() {
		if (ENDPOINT != null) {
			return ENDPOINT;
		} else {
			throw new IllegalStateException("OSS endpoint is not available");
		}
	}

	/**
	 * Get OSS access key.
	 * @return OSS access key
	 */
	public static String getOSSAccessKey() {
		if (ACCESS_KEY != null) {
			return ACCESS_KEY;
		} else {
			throw new IllegalStateException("OSS access key is not available");
		}
	}

	/**
	 * Get OSS secret key.
	 * @return OSS secret key
	 */
	public static String getOSSSecretKey() {
		if (SECRET_KEY != null) {
			return SECRET_KEY;
		} else {
			throw new IllegalStateException("OSS secret key is not available");
		}
	}

	public static String getTestBucketUri() {
		return getTestBucketUriWithScheme("oss");
	}

	public static String getTestBucketUriWithScheme(String scheme) {
		if (BUCKET != null) {
			return scheme + "://" + BUCKET + "/";
		}
		else {
			throw new IllegalStateException("OSS test bucket is not available");
		}
	}
}
