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

package org.apache.flink.fs.gs;

import javax.annotation.Nullable;

import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Access to the GCS bucket used by the integration tests in this module.
 *
 * <p>The integration tests are skipped unless the {@code IT_CASE_GCS_BUCKET} environment variable
 * names a writable bucket. Authentication uses Application Default Credentials, so the {@code
 * GOOGLE_APPLICATION_CREDENTIALS} environment variable should point to a service-account key file
 * (the method recommended by the GCS filesystem documentation), which feeds both the gcs-connector
 * and the google-cloud-storage library used by {@code RecoverableWriter}.
 */
class GSTestCredentials {

    @Nullable private static final String GCS_TEST_BUCKET = System.getenv("IT_CASE_GCS_BUCKET");

    /** Skips the calling test unless a GCS test bucket is configured. */
    static void assumeCredentialsAvailable() {
        assumeThat(GCS_TEST_BUCKET)
                .as("No GCS test bucket configured via the IT_CASE_GCS_BUCKET environment variable")
                .isNotBlank();
    }

    /** Returns the URI of the test bucket, e.g. {@code gs://my-bucket/temp/}. */
    static String getTestBucketUri() {
        if (GCS_TEST_BUCKET == null) {
            throw new IllegalStateException(
                    "GCS test bucket not available (IT_CASE_GCS_BUCKET not set)");
        }
        return "gs://" + GCS_TEST_BUCKET + "/temp/";
    }
}
