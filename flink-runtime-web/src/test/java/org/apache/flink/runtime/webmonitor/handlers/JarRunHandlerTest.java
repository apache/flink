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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.deployment.application.ApplicationRunner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link JarRunHandler}. */
class JarRunHandlerTest {
    private static final Time TIMEOUT = Time.seconds(10);
    private static final Map<String, String> RESPONSE_HEADERS = Collections.emptyMap();
    private static final String SAVEPOINT_PATH = "dummy/path";

    @Test
    void testGetSavepointRestoreSettings() throws Exception {
        assertGetSavepointRestoreSettings(new Configuration(), null, RestoreMode.NO_CLAIM);
        assertGetSavepointRestoreSettings(
                getConfig(RestoreMode.NO_CLAIM), null, RestoreMode.NO_CLAIM);
        assertGetSavepointRestoreSettings(
                new Configuration(), RestoreMode.NO_CLAIM, RestoreMode.NO_CLAIM);
        assertGetSavepointRestoreSettings(getConfig(RestoreMode.CLAIM), null, RestoreMode.CLAIM);
        assertGetSavepointRestoreSettings(
                new Configuration(), RestoreMode.CLAIM, RestoreMode.CLAIM);
        assertGetSavepointRestoreSettings(getConfig(RestoreMode.LEGACY), null, RestoreMode.LEGACY);
        assertGetSavepointRestoreSettings(
                new Configuration(), RestoreMode.LEGACY, RestoreMode.LEGACY);
        assertGetSavepointRestoreSettings(
                getConfig(RestoreMode.LEGACY), RestoreMode.CLAIM, RestoreMode.CLAIM);
    }

    private static void assertGetSavepointRestoreSettings(
            Configuration configuration, RestoreMode request, RestoreMode expected)
            throws Exception {
        JarRunHandler jarRunHandler = getJarRunHandler(configuration);
        HandlerRequest<JarRunRequestBody> handlerRequest = getHandlerRequest(request);
        SavepointRestoreSettings savepointRestoreSettings =
                jarRunHandler.getSavepointRestoreSettings(handlerRequest);
        assertEquals(expected, savepointRestoreSettings.getRestoreMode());
    }

    private static Configuration getConfig(RestoreMode restoreMode) {
        Configuration configuration = new Configuration();
        configuration.set(SavepointConfigOptions.RESTORE_MODE, restoreMode);
        return configuration;
    }

    private static HandlerRequest<JarRunRequestBody> getHandlerRequest(RestoreMode restoreMode) {
        HandlerRequest<JarRunRequestBody> handlerRequest =
                (HandlerRequest<JarRunRequestBody>) mock(HandlerRequest.class);
        JarRunRequestBody requestBody =
                new JarRunRequestBody(
                        null,
                        null,
                        null,
                        Integer.MAX_VALUE,
                        new JobID(),
                        false,
                        SAVEPOINT_PATH,
                        restoreMode);
        when(handlerRequest.getRequestBody()).thenReturn(requestBody);
        return handlerRequest;
    }

    private static JarRunHandler getJarRunHandler(Configuration configuration) {
        return new JarRunHandler(
                (GatewayRetriever<TestingDispatcherGateway>) mock(GatewayRetriever.class),
                TIMEOUT,
                RESPONSE_HEADERS,
                JarRunHeaders.getInstance(),
                mock(Path.class),
                configuration,
                mock(ScheduledExecutorService.class),
                (Supplier<ApplicationRunner>) mock(Supplier.class));
    }
}
