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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link org.apache.flink.runtime.rest.messages.MessageParameter} for testing purpose. This REST
 * API should not appear in the generated documentation.
 */
@Documentation.ExcludeFromDocumentation()
public class TestExcludeMessageHeaders
        implements MessageHeaders<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

    private static final String URL = "/test/excluded";
    private static final String DESCRIPTION =
            "This REST API should not appear in the generated documentation.";

    private final String url;
    private final String description;

    public TestExcludeMessageHeaders() {
        this.url = URL;
        this.description = DESCRIPTION;
    }

    public TestExcludeMessageHeaders(String url, String description) {
        this.url = URL;
        this.description = DESCRIPTION;
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
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
        return URL;
    }

    @Override
    public Collection<RestAPIVersion> getSupportedAPIVersions() {
        return Collections.singleton(RestAPIVersion.V0);
    }
}
