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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.legacy.messages.TopNMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for Top N metrics. */
public class TopNMetricsHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody, TopNMetricsResponseBody, TopNMetricsMessageParameters> {

    private static final TopNMetricsHeaders INSTANCE = new TopNMetricsHeaders();

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/metrics/top-n";

    private TopNMetricsHeaders() {}

    @Override
    public Class<TopNMetricsResponseBody> getResponseClass() {
        return TopNMetricsResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public TopNMetricsMessageParameters getUnresolvedMessageParameters() {
        return new TopNMetricsMessageParameters();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public String getDescription() {
        return "Returns Top N metrics for a job including CPU consumers, "
                + "backpressured operators, and GC-intensive tasks.";
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    public static TopNMetricsHeaders getInstance() {
        return INSTANCE;
    }
}
