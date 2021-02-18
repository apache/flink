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

package org.apache.flink.runtime.rest.auth;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;

import java.util.Base64;

/** Basic authorization scheme. */
class BasicAuth implements AuthScheme {
    private static final Base64.Encoder encoder = Base64.getEncoder();
    private final String authorization;
    private final String realm;

    protected BasicAuth(
            final String userName, final String password, final Authenticate challenge) {
        this.authorization =
                "Basic " + encoder.encodeToString((userName + ":" + password).getBytes());
        this.realm = challenge.getRealm();
    }

    /**
     * Returns the basic authorization response.
     *
     * @return basic authorization response
     */
    public String getAuthorization(final HttpMethod method, final String uri) {
        return authorization;
    }

    /**
     * Returns if current authorization implementation is valid for given authenticate challenge.
     *
     * @return if current authorization implementation is valid for given authenticate challenge
     */
    public boolean isValid(final Authenticate challenge) {
        return challenge.getScheme().equals(AuthScheme.BASIC) && challenge.getRealm().equals(realm);
    }
}
