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

import java.io.Serializable;

/**
 * AWS session credentials obtained through delegation tokens.
 *
 * <p>This class references no AWS SDK types, because it is bundled into filesystem plugins that
 * ship different AWS SDK versions. Each plugin's credentials provider converts it into its own
 * SDK's type.
 *
 * <p>It is the Java-serialized delegation token payload. The provider and the receiver always come
 * from the same plugin jar, so the payload never crosses versions and is never persisted.
 */
@Internal
public final class S3SessionCredentials implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String accessKeyId;

    private final String secretAccessKey;

    @Nullable private final String sessionToken;

    private final long expirationEpochMilli;

    public S3SessionCredentials(
            String accessKeyId,
            String secretAccessKey,
            @Nullable String sessionToken,
            long expirationEpochMilli) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.expirationEpochMilli = expirationEpochMilli;
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

    public long getExpirationEpochMilli() {
        return expirationEpochMilli;
    }
}
