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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.v2.internal.InternalMapState;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForStMapState}. */
class ForStMapStateTest extends ForStStateTestBase {

    @Test
    void testTtlMapStateAsyncIteratorsReportEmptyAfterClear() throws Exception {
        MapStateDescriptor<Integer, String> descriptor =
                new MapStateDescriptor<>(
                        "testState", IntSerializer.INSTANCE, StringSerializer.INSTANCE);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofMinutes(10)).build());
        InternalMapState<String, Integer, Integer, String> mapState =
                keyedBackend.createState(1, IntSerializer.INSTANCE, descriptor);

        setCurrentContext("test", "test");
        mapState.asyncPut(1, "value")
                .thenCompose(ignored -> mapState.asyncEntries())
                .thenCompose(
                        iterator -> {
                            assertThat(iterator.isEmpty()).isFalse();
                            return mapState.asyncClear();
                        })
                .thenCompose(clear -> mapState.asyncEntries())
                .thenCompose(
                        iterator -> {
                            assertThat(iterator.isEmpty()).isTrue();
                            return mapState.asyncKeys();
                        })
                .thenCompose(
                        iterator -> {
                            assertThat(iterator.isEmpty()).isTrue();
                            return mapState.asyncValues();
                        })
                .thenAccept(iterator -> assertThat(iterator.isEmpty()).isTrue());
        drain();
    }
}
