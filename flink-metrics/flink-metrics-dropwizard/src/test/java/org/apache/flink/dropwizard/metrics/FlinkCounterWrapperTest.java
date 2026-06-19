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

package org.apache.flink.dropwizard.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.util.TestCounter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the FlinkCounterWrapper. */
class FlinkCounterWrapperTest {

    @Test
    void testWrapperIncDec() {
        Counter counter = new TestCounter();
        counter.inc();

        FlinkCounterWrapper wrapper = new FlinkCounterWrapper(counter);
        assertThat(wrapper.getCount()).isEqualTo(1L);
        wrapper.dec();
        assertThat(wrapper.getCount()).isEqualTo(0L);
        wrapper.inc(2);
        assertThat(wrapper.getCount()).isEqualTo(2L);
        wrapper.dec(2);
        assertThat(wrapper.getCount()).isEqualTo(0L);
    }
}
