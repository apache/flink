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
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Decorator class for {@link MessageHeaders} that adds the ability to include custom HTTP headers.
 */
public class CustomHeadersDecorator<
                R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
        implements MessageHeaders<R, P, M> {

    private final MessageHeaders<R, P, M> decorated;
    private Collection<HttpHeader> customHeaders;

    /**
     * Creates a new {@code CustomHeadersDecorator} for a given {@link MessageHeaders} object.
     *
     * @param decorated The MessageHeaders to decorate.
     */
    public CustomHeadersDecorator(MessageHeaders<R, P, M> decorated) {
        this.decorated = decorated;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return decorated.getHttpMethod();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return decorated.getTargetRestEndpointURL();
    }

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return decorated.getSupportedAPIVersions();
    }

    @Override
    public Class<P> getResponseClass() {
        return decorated.getResponseClass();
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return decorated.getResponseStatusCode();
    }

    @Override
    public String getDescription() {
        return decorated.getDescription();
    }

    @Override
    public Class<R> getRequestClass() {
        return decorated.getRequestClass();
    }

    @Override
    public M getUnresolvedMessageParameters() {
        return decorated.getUnresolvedMessageParameters();
    }

    /**
     * Returns the custom headers added to the message.
     *
     * @return The custom headers as a collection of {@link HttpHeader}.
     */
    @Override
    public Collection<HttpHeader> getCustomHeaders() {
        return customHeaders;
    }

    @Override
    public Collection<Class<?>> getResponseTypeParameters() {
        return decorated.getResponseTypeParameters();
    }

    /**
     * Sets the custom headers for the message.
     *
     * @param customHeaders A collection of custom headers.
     */
    public void setCustomHeaders(Collection<HttpHeader> customHeaders) {
        this.customHeaders = customHeaders;
    }

    /**
     * Adds a custom header to the message. Initializes the custom headers collection if it hasn't
     * been initialized yet.
     *
     * @param httpHeader The header to add.
     */
    public void addCustomHeader(HttpHeader httpHeader) {
        if (customHeaders == null) {
            customHeaders = new ArrayList<>();
        }
        customHeaders.add(httpHeader);
    }

    public MessageHeaders<R, P, M> getDecorated() {
        return decorated;
    }
}
