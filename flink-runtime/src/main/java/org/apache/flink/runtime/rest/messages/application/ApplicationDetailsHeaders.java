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

package org.apache.flink.runtime.rest.messages.application;

import org.apache.flink.runtime.messages.webmonitor.ApplicationDetailsInfo;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.application.ApplicationDetailsHandler;
import org.apache.flink.runtime.rest.messages.ApplicationIDPathParameter;
import org.apache.flink.runtime.rest.messages.ApplicationMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for the {@link ApplicationDetailsHandler}. */
public class ApplicationDetailsHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody, ApplicationDetailsInfo, ApplicationMessageParameters> {

    private static final ApplicationDetailsHeaders INSTANCE = new ApplicationDetailsHeaders();

    public static final String URL = "/applications/:" + ApplicationIDPathParameter.KEY;

    private ApplicationDetailsHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<ApplicationDetailsInfo> getResponseClass() {
        return ApplicationDetailsInfo.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public ApplicationMessageParameters getUnresolvedMessageParameters() {
        return new ApplicationMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static ApplicationDetailsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Returns details of an application.";
    }
}
