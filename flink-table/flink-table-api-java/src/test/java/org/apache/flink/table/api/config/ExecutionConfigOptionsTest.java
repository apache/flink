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

package org.apache.flink.table.api.config;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExecutionConfigOptions}. */
class ExecutionConfigOptionsTest {

    /**
     * {@link SerializerConfigImpl} reads the state schema evolution option by key string, because
     * flink-core cannot depend on this module. Nothing else ties the two together: if the key here
     * changed, the option would keep documenting and validating while the runtime read its own
     * unset key and the feature stayed permanently off. This is the only place both sides are
     * visible at once.
     */
    @Test
    void stateSchemaEvolutionOptionReachesTheSerializerConfig() {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_STATE_SCHEMA_EVOLUTION_ENABLED, true);

        assertThat(new SerializerConfigImpl(configuration).isStateSchemaEvolutionEnabled())
                .isTrue();
        assertThat(new SerializerConfigImpl(new Configuration()).isStateSchemaEvolutionEnabled())
                .isFalse();
    }
}
