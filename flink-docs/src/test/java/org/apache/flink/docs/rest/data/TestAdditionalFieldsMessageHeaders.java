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

package org.apache.flink.docs.rest.data;

import org.apache.flink.annotation.docs.FlinkJsonSchema;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link MessageHeaders} for testing purpose. Its request body contains additional fields,
 * response body and message parameters are empty.
 */
public class TestAdditionalFieldsMessageHeaders
        implements RuntimeMessageHeaders<
                TestAdditionalFieldsMessageHeaders.AdditionalFieldsRequestBody,
                EmptyResponseBody,
                EmptyMessageParameters> {

    /** Testing request body. */
    @FlinkJsonSchema.AdditionalFields(type = String.class)
    public static class AdditionalFieldsRequestBody implements RequestBody {}

    private static final String URL = "/test/additional-fields";
    private static final String DESCRIPTION = "This is an testing REST API with additional fields.";

    private final String url;
    private final String description;
    private final String operationId;

    public TestAdditionalFieldsMessageHeaders(String operationId) {
        this(URL, DESCRIPTION, operationId);
    }

    private TestAdditionalFieldsMessageHeaders(String url, String description, String operationId) {
        this.url = url;
        this.description = description;
        this.operationId = operationId;
    }

    @Override
    public Class<AdditionalFieldsRequestBody> getRequestClass() {
        return AdditionalFieldsRequestBody.class;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.PUT;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String operationId() {
        return operationId;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return url;
    }

    @Override
    public Collection<RuntimeRestAPIVersion> getSupportedAPIVersions() {
        return Collections.singleton(RuntimeRestAPIVersion.V0);
    }
}
