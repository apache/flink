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

package org.apache.flink.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ClusterOptions}. */
class ClusterOptionsTest {

    @Test
    void testThreadDumpDefaultModeDefaultsToFull() {
        assertThat(new Configuration().get(ClusterOptions.THREAD_DUMP_DEFAULT_MODE))
                .isEqualTo(ThreadDumpMode.FULL);
    }

    @ParameterizedTest
    @ValueSource(strings = {"LITE", "lite", "Lite"})
    void testThreadDumpDefaultModeIsCaseInsensitive(String value) {
        final Configuration configuration = new Configuration();
        configuration.setString(ClusterOptions.THREAD_DUMP_DEFAULT_MODE.key(), value);

        assertThat(configuration.get(ClusterOptions.THREAD_DUMP_DEFAULT_MODE))
                .isEqualTo(ThreadDumpMode.LITE);
    }

    /**
     * An unparsable value must fail fast instead of silently falling back to {@link
     * ThreadDumpMode#FULL}, which is the mode operators configure this option to avoid.
     */
    @ParameterizedTest
    @ValueSource(strings = {"Ltie", "", " ", "none"})
    void testThreadDumpDefaultModeRejectsUnknownValue(String value) {
        final Configuration configuration = new Configuration();
        configuration.setString(ClusterOptions.THREAD_DUMP_DEFAULT_MODE.key(), value);

        assertThatThrownBy(() -> configuration.get(ClusterOptions.THREAD_DUMP_DEFAULT_MODE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ClusterOptions.THREAD_DUMP_DEFAULT_MODE.key())
                .cause()
                .hasMessageContaining(ThreadDumpMode.LITE.name())
                .hasMessageContaining(ThreadDumpMode.FULL.name());
    }
}
