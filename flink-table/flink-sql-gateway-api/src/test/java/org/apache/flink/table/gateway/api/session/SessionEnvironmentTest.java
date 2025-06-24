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

package org.apache.flink.table.gateway.api.session;

import org.apache.flink.table.gateway.api.utils.MockedEndpointVersion;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test {@link SessionEnvironment}. */
public class SessionEnvironmentTest {

    @Test
    public void testBuildSessionEnvironment() {
        String sessionName = "test";

        Map<String, String> configMap = new HashMap<>();
        configMap.put("key1", "value1");
        configMap.put("key2", "value2");

        SessionEnvironment expectedEnvironment =
                new SessionEnvironment(
                        sessionName,
                        MockedEndpointVersion.V1,
                        new HashMap<>(),
                        new HashMap<>(),
                        "default",
                        configMap);

        SessionEnvironment actualEnvironment =
                SessionEnvironment.newBuilder()
                        .setSessionName(sessionName)
                        .setSessionEndpointVersion(MockedEndpointVersion.V1)
                        .addSessionConfig(configMap)
                        .setDefaultCatalog("default")
                        .build();
        assertEquals(expectedEnvironment, actualEnvironment);
    }
}
