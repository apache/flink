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

/** Authorization scheme interface. */
public interface AuthScheme {
    String BASIC = "Basic";
    String DIGEST = "Digest";

    /**
     * Returns the authorization response for given uri using specified http method.
     *
     * @return authorization response for given uri using specified http method
     */
    String getAuthorization(final HttpMethod method, final String uri);

    /**
     * Returns if current authorization implementation is valid for given authenticate challenge.
     *
     * @return if current authorization implementation is valid for given authenticate challenge
     */
    boolean isValid(final Authenticate authenticate);
}
