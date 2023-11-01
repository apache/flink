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

package org.apache.flink.test.junit5;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

class MiniClusterExtensionDefaultParallelismITCase {

    private static final int TARGET_PARALLELISM = 1;

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(
                                    new Configuration()
                                            .set(
                                                    CoreOptions.DEFAULT_PARALLELISM,
                                                    TARGET_PARALLELISM))
                            .setNumberSlotsPerTaskManager(TARGET_PARALLELISM * 2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    void testDefaultParallelismSettingHonored() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final int actualParallelism =
                env.fromElements(1)
                        .map(
                                new RichMapFunction<Integer, Integer>() {
                                    @Override
                                    public Integer map(Integer value) {
                                        return getRuntimeContext().getNumberOfParallelSubtasks();
                                    }
                                })
                        .executeAndCollect(TARGET_PARALLELISM)
                        .get(0);

        assertThat(actualParallelism).isEqualTo(TARGET_PARALLELISM);
    }
}
