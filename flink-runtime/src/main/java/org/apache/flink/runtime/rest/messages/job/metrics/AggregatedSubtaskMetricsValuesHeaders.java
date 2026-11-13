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
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for batch aggregated subtask metric values. */
public class AggregatedSubtaskMetricsValuesHeaders
        implements RuntimeMessageHeaders<
                EmptyRequestBody,
                AggregatedSubtaskMetricsBatchResponseBody,
                AggregatedSubtaskMetricsValuesParameters> {

    private static final AggregatedSubtaskMetricsValuesHeaders INSTANCE =
            new AggregatedSubtaskMetricsValuesHeaders();

    private AggregatedSubtaskMetricsValuesHeaders() {}

    @Override
    public Class<EmptyRequestBody> getRequestClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public Class<AggregatedSubtaskMetricsBatchResponseBody> getResponseClass() {
        return AggregatedSubtaskMetricsBatchResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public AggregatedSubtaskMetricsValuesParameters getUnresolvedMessageParameters() {
        return new AggregatedSubtaskMetricsValuesParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.GET;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/jobs/:" + JobIDPathParameter.KEY + "/vertices/subtasks/metrics/values";
    }

    public static AggregatedSubtaskMetricsValuesHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Provides batch access to aggregated subtask metric values.";
    }
}
