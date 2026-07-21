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

package org.apache.flink.fs.s3.common.token;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * AWS session credentials obtained through delegation tokens, held in an SDK-agnostic form.
 *
 * <p>This class deliberately references no AWS SDK types: it is bundled into both the {@code
 * flink-s3-fs-hadoop} jar (which ships only AWS SDK v2) and the {@code flink-s3-fs-presto} jar
 * (which ships only AWS SDK v1), so it must be loadable with either SDK absent. Each filesystem's
 * credential provider converts these values into its own SDK's credential type.
 */
@Internal
public final class S3SessionCredentials {

    private final String accessKeyId;

    private final String secretAccessKey;

    @Nullable private final String sessionToken;

    public S3SessionCredentials(
            String accessKeyId, String secretAccessKey, @Nullable String sessionToken) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    @Nullable
    public String getSessionToken() {
        return sessionToken;
    }
}
