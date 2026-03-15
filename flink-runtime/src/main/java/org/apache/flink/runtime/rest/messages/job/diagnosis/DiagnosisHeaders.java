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

package org.apache.flink.runtime.rest.messages.job.diagnosis;

import org.apache.flink.runtime.rest.handler.legacy.messages.DiagnosisResponseBody;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;

/** Headers for Diagnosis Advisor. */
public class DiagnosisHeaders
        implements MessageHeaders<
                EmptyRequestBody, DiagnosisResponseBody, DiagnosisMessageParameters> {

    private static final DiagnosisHeaders INSTANCE = new DiagnosisHeaders();

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/diagnosis";

    private DiagnosisHeaders() {}

    @Override
    public Class<DiagnosisResponseBody> getResponseBodyClass() {
        return DiagnosisResponseBody.class;
    }

    @Override
    public Class<EmptyRequestBody> getRequestBodyClass() {
        return EmptyRequestBody.class;
    }

    @Override
    public DiagnosisMessageParameters getUnresolvedMessageParameters() {
        return new DiagnosisMessageParameters();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public String getDescription() {
        return "Provides automated diagnostic suggestions based on job metrics "
                + "including CPU, memory, GC, and backpressure analysis.";
    }

    @Override
    public HttpMethod getHttpMethod() {
        return HttpMethod.GET;
    }

    public static DiagnosisHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getOperationId() {
        return "diagnosis";
    }
}
