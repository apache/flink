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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;

/** Headers for Top N metrics request. */
public class TopNMetricsHeaders
        implements MessageHeaders<TopNMetricsParameters, TopNMetricsResponseBody, JobMessageParameters> {

    private static final TopNMetricsHeaders INSTANCE = new TopNMetricsHeaders();

    public static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/metrics/top-n";

    @Override
    public Class<TopNMetricsParameters> getRequestClass() {
        return TopNMetricsParameters.class;
    }

    @Override
    public Class<TopNMetricsResponseBody> getResponseClass() {
        return TopNMetricsResponseBody.class;
    }

    @Override
    public HttpMethod getHttpMethod() {
        return HttpMethod.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public String getDescription() {
        return "Returns Top N metrics for a job including top CPU consumers, backpressure operators, and GC intensive tasks.";
    }

    @Override
    public Class<JobMessageParameters> getUnresolvedMessageClass() {
        return JobMessageParameters.class;
    }

    public static TopNMetricsHeaders getInstance() {
        return INSTANCE;
    }
}
