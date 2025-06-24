/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.graph;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.util.MockStreamConfig;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link StreamConfig}. */
class StreamConfigTest {

    @Test
    void testClearInitialConfigs() {
        int chainedTaskId = 3456;
        MockStreamConfig streamConfig =
                new MockStreamConfig(
                        new Configuration(),
                        1,
                        Collections.singletonMap(
                                chainedTaskId, new MockStreamConfig(new Configuration(), 1)));

        ClassLoader classLoader = getClass().getClassLoader();
        StreamOperatorFactory<?> streamOperatorFactory =
                streamConfig.getStreamOperatorFactory(classLoader);
        assertThat(streamOperatorFactory).isNotNull();
        assertThat(streamConfig.getStreamOperatorFactoryClass(classLoader)).isNotNull();
        assertThat(streamConfig.getTransitiveChainedTaskConfigs(classLoader))
                .hasSize(1)
                .containsKey(chainedTaskId);

        // StreamOperatorFactory and ChainedTaskConfigs should be cleared after clearInitialConfigs,
        // but the factory class shouldn't be cleared.
        streamConfig.clearInitialConfigs();
        assertThatThrownBy(() -> streamConfig.getStreamOperatorFactory(classLoader))
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("serializedUDF has been removed.");
        assertThat(streamConfig.getStreamOperatorFactoryClass(classLoader)).isNotNull();
        assertThatThrownBy(() -> streamConfig.getTransitiveChainedTaskConfigs(classLoader))
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("chainedTaskConfig_ has been removed.");
    }
}
