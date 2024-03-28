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

package org.apache.flink.datastream.impl.common;

import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OutputCollector}. */
class OutputCollectorTest {
    @Test
    void testCollect() {
        List<StreamElement> list = new ArrayList<>();
        CollectorOutput<Integer> collectorOutput = new CollectorOutput<>(list);
        OutputCollector<Integer> collector = new OutputCollector<>(collectorOutput);
        collector.collect(1);
        collector.collect(2);
        assertThat(list).containsExactly(new StreamRecord<>(1), new StreamRecord<>(2));
    }

    @Test
    void testCollectAndOverwriteTimestamp() {
        List<StreamElement> list = new ArrayList<>();
        CollectorOutput<Integer> collectorOutput = new CollectorOutput<>(list);
        OutputCollector<Integer> collector = new OutputCollector<>(collectorOutput);
        collector.collectAndOverwriteTimestamp(1, 10L);
        collector.collectAndOverwriteTimestamp(2, 20L);
        assertThat(list).containsExactly(new StreamRecord<>(1, 10L), new StreamRecord<>(2, 20L));
    }
}
