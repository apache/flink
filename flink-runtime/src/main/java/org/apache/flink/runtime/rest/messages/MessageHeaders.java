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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpHeader;
import org.apache.flink.runtime.rest.HttpMethodWrapper;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * This class links {@link RequestBody}s to {@link ResponseBody}s types and contains meta-data
 * required for their http headers.
 *
 * <p>Implementations must be state-less.
 *
 * @param <R> request message type
 * @param <P> response message type
 * @param <M> message parameters type
 */
public interface MessageHeaders<
                R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
        extends UntypedResponseMessageHeaders<R, M> {

    /**
     * Returns the class of the response message.
     *
     * @return class of the response message
     */
    Class<P> getResponseClass();

    /**
     * Returns the http status code for the response.
     *
     * @return http status code of the response
     */
    HttpResponseStatus getResponseStatusCode();

    /**
     * Returns the collection of type parameters for the response type.
     *
     * @return Collection of type parameters for the response type
     */
    default Collection<Class<?>> getResponseTypeParameters() {
        return Collections.emptyList();
    }

    /**
     * Returns the description for this header.
     *
     * @return description for the header
     */
    String getDescription();

    /**
     * Returns a short description for this header suitable for method code generation.
     *
     * @return short description
     */
    default String operationId() {
        final String className = getClass().getSimpleName();

        if (getHttpMethod() != HttpMethodWrapper.GET) {
            throw new UnsupportedOperationException(
                    "The default implementation is only supported for GET calls. Please override 'operationId()' in '"
                            + className
                            + "'.");
        }

        final int headersSuffixStart = className.lastIndexOf("Headers");
        if (headersSuffixStart == -1) {
            throw new IllegalStateException(
                    "Expect name of class "
                            + getClass()
                            + " to end on 'Headers'. Please rename the class or override 'operationId()'.");
        }

        return getHttpMethod().name().toLowerCase(Locale.ROOT)
                + className.substring(0, headersSuffixStart);
    }

    /**
     * Returns a collection of custom HTTP headers.
     *
     * <p>This default implementation returns an empty list. Override this method to provide custom
     * headers if needed.
     *
     * @return a collection of custom {@link HttpHeaders}, empty by default.
     */
    default Collection<HttpHeader> getCustomHeaders() {
        return Collections.emptyList();
    }
}
