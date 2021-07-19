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

package org.apache.flink.testutils.cos;

import org.junit.Assume;
import org.junit.AssumptionViolatedException;

import javax.annotation.Nullable;

/** Access to credentials to access COS buckets during integration tests. */
public class COSTestCredentials {
    @Nullable
    private static final String COS_TEST_SECRET_ID = System.getenv("IT_COS_TEST_SECRET_ID");

    @Nullable
    private static final String COS_TEST_SECRET_KEY = System.getenv("IT_COS_TEST_SECRET_KEY");

    @Nullable private static final String COS_TEST_REGION = System.getenv("IT_COS_TEST_REGION");

    @Nullable private static final String COS_TEST_BUCKET = System.getenv("IT_COS_TEST_BUCKET");

    @Nullable
    private static final String COS_TEST_COSN_IMPL = System.getenv("IT_COS_TEST_COSN_IMPL");

    @Nullable
    private static final String COS_TEST_AFS_COSN_IMPL = System.getenv("IT_COS_TEST_AFS_COSN_IMPL");

    // ------------------------------------------------------------------------

    /**
     * Checks whether COS test credentials are available in the environment variables of this JVM.
     */
    private static boolean credentialsAvailable() {
        return isNotEmpty(COS_TEST_SECRET_ID)
                && isNotEmpty(COS_TEST_SECRET_KEY)
                && isNotEmpty(COS_TEST_REGION)
                && isNotEmpty(COS_TEST_COSN_IMPL)
                && isNotEmpty(COS_TEST_AFS_COSN_IMPL)
                && isNotEmpty(COS_TEST_BUCKET);
    }

    /** Checks if a String is not null and not empty. */
    private static boolean isNotEmpty(@Nullable String str) {
        return str != null && !str.isEmpty();
    }

    /**
     * Checks whether credentials are available in the environment variables of this JVM. If not,
     * throws an {@link AssumptionViolatedException} which causes JUnit tests to be skipped.
     */
    public static void assumeCredentialsAvailable() {
        Assume.assumeTrue(
                "No COS credentials available in this test's environment", credentialsAvailable());
    }

    @Nullable
    public static String getCosTestSecretId() {
        return COS_TEST_SECRET_ID;
    }

    @Nullable
    public static String getCosTestSecretKey() {
        return COS_TEST_SECRET_KEY;
    }

    @Nullable
    public static String getCosTestRegion() {
        return COS_TEST_REGION;
    }

    @Nullable
    public static String getCosTestBucket() {
        return COS_TEST_BUCKET;
    }

    @Nullable
    public static String getCosTestCosnImpl() {
        return COS_TEST_COSN_IMPL;
    }

    @Nullable
    public static String getCosTestAfsCosnImpl() {
        return COS_TEST_AFS_COSN_IMPL;
    }

    /**
     * Gets the URI for the path under which all tests should put their data.
     *
     * <p>This method throws an exception if the bucket was not configured. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getTestBucketUri() {
        return getTestBucketUriWithScheme("cosn");
    }

    /**
     * Gets the URI for the path under which all tests should put their data.
     *
     * <p>This method throws an exception if the bucket was not configured. Tests should use {@link
     * #assumeCredentialsAvailable()} to skip tests when credentials are not available.
     */
    public static String getTestBucketUriWithScheme(String scheme) {
        if (COS_TEST_BUCKET != null) {
            return scheme + "://" + COS_TEST_BUCKET + "/";
        } else {
            throw new IllegalStateException("COS test bucket not available");
        }
    }
}
