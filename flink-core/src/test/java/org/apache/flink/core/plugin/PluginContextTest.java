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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.core.plugin;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PluginContext}. */
class PluginContextTest {

    @Test
    void testGetTaskManagerIdReturnsConstructedValue() {
        final PluginContext ctx = new PluginContext("tm-001");
        assertThat(ctx.getTaskManagerId()).isEqualTo("tm-001");
    }

    @Test
    void testNullTaskManagerIdIsRejected() {
        assertThatThrownBy(() -> new PluginContext(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testToStringContainsTaskManagerId() {
        final PluginContext ctx = new PluginContext("tm-42");
        assertThat(ctx.toString()).contains("tm-42");
    }
}
