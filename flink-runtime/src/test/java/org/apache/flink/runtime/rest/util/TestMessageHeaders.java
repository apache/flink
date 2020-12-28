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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Utility {@link MessageParameters} providing a builder to initialize MessageParameters without
 * introducing a new class.
 *
 * @param <REQ> The RequestBody type.
 * @param <RES> The ResponseBody type.
 * @param <M> The MessageParameters type.
 */
public class TestMessageHeaders<
                REQ extends RequestBody, RES extends ResponseBody, M extends MessageParameters>
        implements MessageHeaders<REQ, RES, M> {

    public static TestMessageHeaders.Builder<
                    EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
            emptyBuilder() {
        return new Builder<>(
                EmptyRequestBody.class,
                EmptyResponseBody.class,
                EmptyMessageParameters.getInstance());
    }

    public static <
                    MREQ extends RequestBody,
                    MRES extends ResponseBody,
                    MM extends MessageParameters>
            Builder<MREQ, MRES, MM> builder(
                    Class<MREQ> requestClass, Class<MRES> responseClass, MM messageParameters) {
        return new Builder<>(requestClass, responseClass, messageParameters);
    }

    /**
     * TestMessageHeaders.Builder is used for initializing a TestMessageHeaders instance.
     *
     * @param <BREQ> The RequestBody type used by the Builder.
     * @param <BRES> The ResponseBody type used by the Builder.
     * @param <BM> The MessageParameters type used by the Builder.
     */
    public static class Builder<
            BREQ extends RequestBody, BRES extends ResponseBody, BM extends MessageParameters> {

        private Class<BREQ> requestClass;
        private Class<BRES> responseClass;
        private HttpResponseStatus responseStatusCode = HttpResponseStatus.OK;
        private String description = "Test description";
        private BM messageParameters;
        private HttpMethodWrapper httpMethod = HttpMethodWrapper.GET;
        private String targetRestEndpointURL = "/test-endpoint";

        private Builder(Class<BREQ> requestClass, Class<BRES> responseClass, BM messageParameters) {
            this.requestClass = requestClass;
            this.responseClass = responseClass;
            this.messageParameters = messageParameters;
        }

        public Builder<BREQ, BRES, BM> setResponseStatusCode(
                HttpResponseStatus responseStatusCode) {
            this.responseStatusCode = responseStatusCode;
            return this;
        }

        public Builder<BREQ, BRES, BM> setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder<BREQ, BRES, BM> setHttpMethod(HttpMethodWrapper httpMethod) {
            this.httpMethod = httpMethod;
            return this;
        }

        public Builder<BREQ, BRES, BM> setTargetRestEndpointURL(String targetRestEndpointURL) {
            this.targetRestEndpointURL = targetRestEndpointURL;
            return this;
        }

        public TestMessageHeaders<BREQ, BRES, BM> build() {
            return new TestMessageHeaders(
                    requestClass,
                    responseClass,
                    responseStatusCode,
                    description,
                    messageParameters,
                    httpMethod,
                    targetRestEndpointURL);
        }
    }

    private final Class<REQ> requestClass;
    private final Class<RES> responseClass;
    private final HttpResponseStatus responseStatusCode;
    private final String description;
    private final M messageParameters;
    private final HttpMethodWrapper httpMethod;
    private final String targetRestEndpointURL;

    private TestMessageHeaders(
            Class<REQ> requestClass,
            Class<RES> responseClass,
            HttpResponseStatus responseStatusCode,
            String description,
            M messageParameters,
            HttpMethodWrapper httpMethod,
            String targetRestEndpointURL) {
        this.requestClass = requestClass;
        this.responseClass = responseClass;
        this.responseStatusCode = responseStatusCode;
        this.description = description;
        this.messageParameters = messageParameters;
        this.httpMethod = httpMethod;
        this.targetRestEndpointURL = targetRestEndpointURL;
    }

    @Override
    public Class<RES> getResponseClass() {
        return responseClass;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return responseStatusCode;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Class<REQ> getRequestClass() {
        return requestClass;
    }

    @Override
    public M getUnresolvedMessageParameters() {
        return messageParameters;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return httpMethod;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return targetRestEndpointURL;
    }
}
