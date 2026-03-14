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

package org.apache.flink.fs.s3native;

import org.apache.flink.annotation.Internal;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;

/** Utilities for consistent handling of {@link S3Exception} across the native S3 connector. */
@Internal
public final class S3ExceptionUtils {

    private S3ExceptionUtils() {}

    /**
     * Wraps an {@link S3Exception} into an {@link IOException} with a contextual message that
     * includes HTTP status code and AWS error details when available.
     */
    public static IOException toIOException(String context, S3Exception e) {
        return new IOException(
                String.format("%s (HTTP %d: %s)", context, e.statusCode(), errorMessage(e)), e);
    }

    public static String errorMessage(S3Exception e) {
        AwsErrorDetails details = e.awsErrorDetails();
        if (details != null && details.errorMessage() != null) {
            return details.errorMessage();
        }
        return e.getMessage() != null ? e.getMessage() : "Unknown S3 error";
    }

    public static String errorCode(S3Exception e) {
        AwsErrorDetails details = e.awsErrorDetails();
        if (details != null && details.errorCode() != null) {
            return details.errorCode();
        }
        return "Unknown";
    }
}
