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

package org.apache.flink.streaming.connectors.gcp.pubsub.emulator;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;

import java.time.Instant;
import java.util.Date;

import static java.lang.Long.MAX_VALUE;

/**
 * A placeholder for credentials to signify that requests sent to the server should not be
 * authenticated. This is typically useful when using local service emulators. NOTE: The Google
 * provided NoCredentials and NoCredentialsProvider do not behave as expected See
 * https://github.com/googleapis/gax-java/issues/1148
 */
public final class EmulatorCredentials extends OAuth2Credentials {
    private static final EmulatorCredentials INSTANCE = new EmulatorCredentials();

    private EmulatorCredentials() {}

    private Object readResolve() {
        return INSTANCE;
    }

    public static EmulatorCredentials getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public AccessToken refreshAccessToken() {
        return new AccessToken(
                "Dummy credentials for emulator", Date.from(Instant.ofEpochMilli(MAX_VALUE)));
    }
}
