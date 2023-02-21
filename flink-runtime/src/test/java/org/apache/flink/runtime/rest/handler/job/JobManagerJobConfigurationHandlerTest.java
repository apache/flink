/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.ConfigurationInfo;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobManagerJobConfigurationHeaders;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for the {@link JobManagerJobConfigurationHandler}. */
public class JobManagerJobConfigurationHandlerTest extends TestLogger {

    @Test
    public void testRequestConfiguration() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.ADDRESS, "address");

        final JobManagerJobConfigurationHandler handler =
                new JobManagerJobConfigurationHandler(
                        () -> null,
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        JobManagerJobConfigurationHeaders.getInstance(),
                        configuration);

        final ConfigurationInfo configurationInfo =
                handler.handleRequest(
                                HandlerRequest.resolveParametersAndCreate(
                                        EmptyRequestBody.getInstance(),
                                        new JobMessageParameters(),
                                        Collections.emptyMap(),
                                        Collections.emptyMap(),
                                        Collections.emptyList()),
                                new TestingRestfulGateway.Builder().build())
                        .get();

        assertEquals(JobManagerOptions.ADDRESS.key(), configurationInfo.get(0).getKey());
        assertEquals("address", configurationInfo.get(0).getValue());
    }
}
