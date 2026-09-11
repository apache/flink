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
import org.apache.flink.core.fs.Path;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Shared S3 URI handling utilities. */
@Internal
public final class S3UriUtils {

    private S3UriUtils() {}

    private static final String S3 = "s3://";
    private static final String S3A = "s3a://";

    /** Extracts the S3 object key from a Flink {@link Path}. */
    public static String extractKey(Path path) {
        checkSupportedS3Scheme(path);
        String pathStr = path.toUri().getPath();
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1);
        }
        return pathStr;
    }

    /** Extracts the S3 bucket name from a Flink {@link Path}. */
    public static String extractBucketName(Path path) {
        checkSupportedS3Scheme(path);
        return path.toUri().getHost();
    }

    /**
     * Extracts the bucket name from a raw S3 URI string.
     *
     * @throws IllegalArgumentException if the scheme is not s3:// or s3a://
     */
    public static String extractBucket(String s3Uri) {
        String uri = requireSupportedScheme(s3Uri);
        int bucketEnd = uri.indexOf('/', S3.length());
        return bucketEnd == -1 ? uri.substring(S3.length()) : uri.substring(S3.length(), bucketEnd);
    }

    public static String extractKey(String s3Uri) {
        String uri = requireSupportedScheme(s3Uri);
        int keyStart = uri.indexOf('/', S3.length());
        return keyStart == -1 ? "" : uri.substring(keyStart + 1);
    }

    public static boolean isSupportedS3Scheme(Path path) {
        String scheme = path.toUri().getScheme();
        return "s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme);
    }

    public static boolean isSupportedLocalScheme(Path path) {
        String scheme = path.toUri().getScheme();
        return scheme == null || "file".equalsIgnoreCase(scheme);
    }

    private static void checkSupportedS3Scheme(Path path) {
        checkArgument(
                isSupportedS3Scheme(path),
                "Unsupported S3 URI scheme (expected s3:// or s3a://): %s",
                path);
    }

    private static String requireSupportedScheme(String s3Uri) {
        checkNotNull(s3Uri, "s3Uri must not be null");
        String uri = startsWithIgnoreCase(s3Uri, S3A) ? S3 + s3Uri.substring(S3A.length()) : s3Uri;
        checkArgument(
                startsWithIgnoreCase(uri, S3),
                "Unsupported S3 URI (expected s3:// or s3a:// scheme): %s",
                s3Uri);
        return uri;
    }

    private static boolean startsWithIgnoreCase(String value, String prefix) {
        return value.regionMatches(true, 0, prefix, 0, prefix.length());
    }
}
