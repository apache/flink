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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CommittableCollectorTest {

    @Test
    void testGetCheckpointCommittablesUpTo() {
        final CommittableCollector<Integer> committableCollector = new CommittableCollector<>(1, 1);
        CommittableSummary<Integer> first = new CommittableSummary<>(1, 1, 1L, 1, 0, 0);
        committableCollector.addMessage(first);
        CommittableSummary<Integer> second = new CommittableSummary<>(1, 1, 2L, 1, 0, 0);
        committableCollector.addMessage(second);
        committableCollector.addMessage(new CommittableSummary<>(1, 1, 3L, 1, 0, 0));

        assertThat(committableCollector.getCheckpointCommittablesUpTo(2)).hasSize(2);

        assertThat(committableCollector.getEndOfInputCommittable()).isNull();
    }

    @Test
    void testGetEndOfInputCommittable() {
        final CommittableCollector<Integer> committableCollector = new CommittableCollector<>(1, 1);
        CommittableSummary<Integer> first = new CommittableSummary<>(1, 1, null, 1, 0, 0);
        committableCollector.addMessage(first);

        CommittableManager<Integer> endOfInputCommittable =
                committableCollector.getEndOfInputCommittable();
        assertThat(endOfInputCommittable).isNotNull();
        SinkV2Assertions.assertThat(endOfInputCommittable.getSummary())
                .hasCheckpointId(Long.MAX_VALUE);
    }
}
