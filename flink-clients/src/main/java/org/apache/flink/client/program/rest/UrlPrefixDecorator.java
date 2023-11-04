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

package org.apache.flink.client.program.rest;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;

import static org.apache.flink.shaded.guava31.com.google.common.base.Preconditions.checkNotNull;

/**
 * This class decorates the URL of the original message headers by adding a prefix. The purpose of
 * this decorator is to provide a consistent URL prefix for all the REST API endpoints while
 * preserving the behavior of the original message headers.
 *
 * @param <R> the type of the request body
 * @param <P> the type of the response body
 * @param <M> the type of the message parameters
 */
public class UrlPrefixDecorator<
                R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
        implements MessageHeaders<R, P, M> {

    private final String prefixedUrl;
    private final MessageHeaders<R, P, M> decorated;

    /**
     * Constructs an instance of UrlPrefixDecorator.
     *
     * @param messageHeaders the original SqlGatewayMessageHeaders to be decorated
     * @param urlPrefix the URL prefix to be added to the target REST endpoint URL
     */
    public UrlPrefixDecorator(MessageHeaders<R, P, M> messageHeaders, String urlPrefix) {
        checkNotNull(messageHeaders);
        checkNotNull(urlPrefix);
        this.decorated = messageHeaders;
        this.prefixedUrl =
                constructPrefixedUrlWithVersionPlaceholder(
                        urlPrefix, messageHeaders.getTargetRestEndpointURL());
    }

    private static String constructPrefixedUrlWithVersionPlaceholder(
            String urlPrefix, String targetRestEndpointURL) {
        return urlPrefix + "/" + RestClient.VERSION_PLACEHOLDER + targetRestEndpointURL;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return decorated.getHttpMethod();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return prefixedUrl;
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

    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return decorated.getSupportedAPIVersions();
    }

    @Override
    public Collection<Class<?>> getResponseTypeParameters() {
        return decorated.getResponseTypeParameters();
    }

    public MessageHeaders<R, P, M> getDecorated() {
        return decorated;
    }
}
