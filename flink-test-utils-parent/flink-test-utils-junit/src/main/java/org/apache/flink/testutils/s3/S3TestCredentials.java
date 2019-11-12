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

package org.apache.flink.testutils.s3;

import org.junit.Assume;
import org.junit.AssumptionViolatedException;

import javax.annotation.Nullable;

/**
 * Access to credentials to access S3 buckets during integration tests.
 */
public class S3TestCredentials {

	@Nullable
	private static final String S3_TEST_BUCKET = System.getenv("IT_CASE_S3_BUCKET");

	@Nullable
	private static final String S3_TEST_ACCESS_KEY = System.getenv("IT_CASE_S3_ACCESS_KEY");

	@Nullable
	private static final String S3_TEST_SECRET_KEY = System.getenv("IT_CASE_S3_SECRET_KEY");

	// ------------------------------------------------------------------------

	/**
	 * Checks whether S3 test credentials are available in the environment variables
	 * of this JVM.
	 */
	public static boolean credentialsAvailable() {
		return S3_TEST_BUCKET != null && S3_TEST_ACCESS_KEY != null && S3_TEST_SECRET_KEY != null;
	}

	/**
	 * Checks whether credentials are available in the environment variables of this JVM.
	 * If not, throws an {@link AssumptionViolatedException} which causes JUnit tests to be
	 * skipped.
	 */
	public static void assumeCredentialsAvailable() {
		Assume.assumeTrue("No S3 credentials available in this test's environment", credentialsAvailable());
	}

	/**
	 * Gets the S3 Access Key.
	 *
	 * <p>This method throws an exception if the key is not available. Tests should
	 * use {@link #assumeCredentialsAvailable()} to skip tests when credentials are not
	 * available.
	 */
	public static String getS3AccessKey() {
		if (S3_TEST_ACCESS_KEY != null) {
			return S3_TEST_ACCESS_KEY;
		}
		else {
			throw new IllegalStateException("S3 test access key not available");
		}
	}

	/**
	 * Gets the S3 Secret Key.
	 *
	 * <p>This method throws an exception if the key is not available. Tests should
	 * use {@link #assumeCredentialsAvailable()} to skip tests when credentials are not
	 * available.
	 */
	public static String getS3SecretKey() {
		if (S3_TEST_SECRET_KEY != null) {
			return S3_TEST_SECRET_KEY;
		}
		else {
			throw new IllegalStateException("S3 test secret key not available");
		}
	}

	/**
	 * Gets the URI for the path under which all tests should put their data.
	 *
	 * <p>This method throws an exception if the bucket was not configured. Tests should
	 * use {@link #assumeCredentialsAvailable()} to skip tests when credentials are not
	 * available.
	 */
	public static String getTestBucketUri() {
		return getTestBucketUriWithScheme("s3");
	}

	/**
	 * Gets the URI for the path under which all tests should put their data.
	 *
	 * <p>This method throws an exception if the bucket was not configured. Tests should
	 * use {@link #assumeCredentialsAvailable()} to skip tests when credentials are not
	 * available.
	 */
	public static String getTestBucketUriWithScheme(String scheme) {
		if (S3_TEST_BUCKET != null) {
			return scheme + "://" + S3_TEST_BUCKET + "/temp/";
		}
		else {
			throw new IllegalStateException("S3 test bucket not available");
		}
	}
}
