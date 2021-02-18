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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** Rest client authorization. */
public class RestClientAuth {
    private static final Logger LOG = LoggerFactory.getLogger(RestClientAuth.class);
    private final String userName;
    private final String password;

    private Optional<AuthScheme> auth;
    private String responseHeader;

    private RestClientAuth(final String userName, final String password) {
        this.userName = userName;
        this.password = password;
        this.auth = Optional.empty();
    }

    /**
     * Returns a copy of current authorization credential.
     *
     * @return copy of current authorization credential
     */
    public RestClientAuth copy() {
        return new RestClientAuth(userName, password);
    }

    /**
     * Add authorization response header and returns the new headers.
     *
     * @return the new headers after adding authorization response header
     */
    public HttpHeaders addAuthorization(
            final HttpMethod method, final String uri, final HttpHeaders headers) {
        return auth.map(a -> headers.set(responseHeader, a.getAuthorization(method, uri)))
                .orElse(headers);
    }

    /**
     * Returns rest client authorization credential if it's configured.
     *
     * @return Optional of rest client authorization credential
     */
    public static Optional<RestClientAuth> fromConfiguration(final Configuration config) {
        final String userName = config.getString(RestOptions.CLIENT_AUTH_USERNAME);
        final String password = config.getString(RestOptions.CLIENT_AUTH_PASSWORD);
        if (userName == null || password == null) {
            return Optional.empty();
        }
        return Optional.of(new RestClientAuth(userName, password));
    }

    private AuthScheme createAuth(final Authenticate challenge) {
        if (challenge.getScheme().equals(AuthScheme.BASIC)) {
            return new BasicAuth(userName, password, challenge);
        } else {
            return new DigestAuth(userName, password, challenge);
        }
    }

    /**
     * Returns if the failed http request due to authentication can be restarted with corresponding
     * authorization scheme.
     *
     * @return if the failed http request can be restarted with corresponding authorization scheme
     */
    public boolean isRestartable(final HttpResponse response) {
        final String authenticate;
        final String header;
        if (response.getStatus().equals(HttpResponseStatus.UNAUTHORIZED)) {
            authenticate = HttpHeaders.getHeader(response, HttpHeaders.Names.WWW_AUTHENTICATE);
            header = HttpHeaders.Names.AUTHORIZATION;
        } else {
            authenticate = HttpHeaders.getHeader(response, HttpHeaders.Names.PROXY_AUTHENTICATE);
            header = HttpHeaders.Names.PROXY_AUTHORIZATION;
        }
        try {
            final Authenticate challenge = Authenticate.parse(authenticate);
            // Do not restart the request if it was responsed with the same challenge
            if (!auth.map(a -> a.isValid(challenge) && header.equals(responseHeader))
                    .orElse(true)) {
                return false;
            }
            auth = Optional.of(createAuth(challenge));
            responseHeader = header;
            return true;
        } catch (Throwable throwable) {
            LOG.error("Failed to process authenticate challenge: {}", authenticate, throwable);
            return false;
        }
    }
}
