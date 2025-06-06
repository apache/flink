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

package org.apache.flink.connector.base.sink.writer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit Test for Batch, majorly testing constructor, setter and getters. */
public class BatchTest {

    @Test
    public void testConstructorAndGetters() {
        Batch<String> result = new Batch<>(Arrays.asList("event1", "event2", "event3"), 18L);

        assertThat(result.getRecordCount()).isEqualTo(3);
        assertThat(result.getSizeInBytes()).isEqualTo(18L);
        assertThat(result.getBatchEntries()).isEqualTo(Arrays.asList("event1", "event2", "event3"));
    }

    @Test
    public void testEmptyBatch() {
        Batch<String> result = new Batch<>(Collections.emptyList(), 0L);

        assertThat(result.getBatchEntries()).isEmpty();
        assertThat(result.getSizeInBytes()).isEqualTo(0L);
        assertThat(result.getRecordCount()).isEqualTo(0);
    }
}
