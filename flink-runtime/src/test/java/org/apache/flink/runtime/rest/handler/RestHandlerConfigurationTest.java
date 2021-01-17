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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link RestHandlerConfiguration}. */
public class RestHandlerConfigurationTest extends TestLogger {

    @Test
    public void testWebSubmitFeatureFlagEnabled() {
        testWebSubmitFeatureFlag(true);
    }

    @Test
    public void testWebSubmitFeatureFlagDisabled() {
        testWebSubmitFeatureFlag(false);
    }

    private static void testWebSubmitFeatureFlag(boolean webSubmitEnabled) {
        final Configuration config = new Configuration();
        config.setBoolean(WebOptions.SUBMIT_ENABLE, webSubmitEnabled);

        RestHandlerConfiguration restHandlerConfiguration =
                RestHandlerConfiguration.fromConfiguration(config);
        assertEquals(webSubmitEnabled, restHandlerConfiguration.isWebSubmitEnabled());
    }
}
