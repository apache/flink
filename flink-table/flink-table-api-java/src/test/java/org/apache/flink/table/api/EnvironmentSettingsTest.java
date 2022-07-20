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

package org.apache.flink.table.api;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test {@link EnvironmentSettings}. */
class EnvironmentSettingsTest {

    @Test
    void testFromConfiguration() {
        Configuration configuration = new Configuration();
        configuration.setString("execution.runtime-mode", "batch");
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withConfiguration(configuration).build();

        assertThat(settings.isStreamingMode()).as("Expect batch mode.").isFalse();
    }

    @Test
    void testGetConfiguration() {
        EnvironmentSettings settings = new EnvironmentSettings.Builder().inBatchMode().build();
        Configuration configuration = settings.getConfiguration();

        assertThat(configuration.get(RUNTIME_MODE)).isEqualTo(RuntimeExecutionMode.BATCH);
    }
}
