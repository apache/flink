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

package org.apache.flink.runtime.rest.handler.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.ApplicationIDPathParameter;
import org.apache.flink.runtime.rest.messages.ApplicationMessageParameters;
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.application.JobManagerApplicationConfigurationHeaders;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the {@link JobManagerApplicationConfigurationHandler}. */
class JobManagerApplicationConfigurationHandlerTest {

    private static HandlerRequest<EmptyRequestBody> createRequest(ApplicationID applicationId)
            throws HandlerRequestException {
        Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(ApplicationIDPathParameter.KEY, applicationId.toString());
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new ApplicationMessageParameters(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @Test
    void testRequestConfiguration() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.ADDRESS, "address");

        final JobManagerApplicationConfigurationHandler handler =
                new JobManagerApplicationConfigurationHandler(
                        () -> null,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobManagerApplicationConfigurationHeaders.getInstance(),
                        configuration);

        final ApplicationID applicationId = ApplicationID.generate();
        final HandlerRequest<EmptyRequestBody> handlerRequest = createRequest(applicationId);

        final ConfigurationInfo configurationInfo =
                handler.handleRequest(handlerRequest, new TestingRestfulGateway.Builder().build())
                        .get();

        assertThat(configurationInfo.get(0).getKey()).isEqualTo(JobManagerOptions.ADDRESS.key());
        assertThat(configurationInfo.get(0).getValue()).isEqualTo("address");
    }
}
