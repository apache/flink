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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for async batch lookup join functionality. */
public class AsyncBatchLookupJoinTest extends BatchAbstractTestBase {

    @Test
    public void testAsyncBatchLookupJoinConfiguration() {
        // Test that the configuration options are properly recognized
        // This is a basic test to verify the configuration options exist and work

        // Test default values
        boolean defaultBatchEnabled =
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED.defaultValue();
        int defaultBatchSize =
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE.defaultValue();
        long defaultFlushInterval =
                OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS
                        .defaultValue();

        assertThat(defaultBatchEnabled).isFalse();
        assertThat(defaultBatchSize).isEqualTo(100);
        assertThat(defaultFlushInterval).isEqualTo(2000L);
    }

    @Test
    public void testConfigurationKeys() {
        // Test that configuration keys are correctly defined
        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED.key())
                .isEqualTo("table.optimizer.dim-lookup-join.batch.enabled");

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE.key())
                .isEqualTo("table.optimizer.dim-lookup-join.batch.size");

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS.key())
                .isEqualTo("table.optimizer.dim-lookup-join.batch.flush.millis");
    }

    @Test
    public void testConfigurationDescriptions() {
        // Test that configuration descriptions are not empty
        assertThat(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED
                                .description())
                .isNotEmpty()
                .contains("dim table batch lookup join");

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE.description())
                .isNotEmpty()
                .contains("batch size");

        assertThat(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS
                                .description())
                .isNotEmpty()
                .contains("flush interval");
    }

    @Test
    public void testConfigurationTypes() {
        // Test that configuration options have correct types
        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_ENABLED.getClazz())
                .isEqualTo(Boolean.class);

        assertThat(OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_SIZE.getClazz())
                .isEqualTo(Integer.class);

        assertThat(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_DIM_LOOKUP_JOIN_BATCH_FLUSH_MILLIS
                                .getClazz())
                .isEqualTo(Long.class);
    }
}
