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

package org.apache.flink.runtime.executiongraph;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultSubtaskAttemptNumberStore}. */
class DefaultSubtaskAttemptNumberStoreTest {

    @Test
    void testGetAttemptCount() {
        final List<Integer> initialAttemptCounts = Arrays.asList(1, 2, 3);
        final DefaultSubtaskAttemptNumberStore subtaskAttemptNumberStore =
                new DefaultSubtaskAttemptNumberStore(initialAttemptCounts);

        assertThat(subtaskAttemptNumberStore.getAttemptCount(1))
                .isEqualTo(initialAttemptCounts.get(1));
    }

    @Test
    void testOutOfBoundsSubtaskIndexReturnsZero() {
        final List<Integer> initialAttemptCounts = Arrays.asList(1, 2, 3);
        final DefaultSubtaskAttemptNumberStore subtaskAttemptNumberStore =
                new DefaultSubtaskAttemptNumberStore(initialAttemptCounts);

        assertThat(subtaskAttemptNumberStore.getAttemptCount(initialAttemptCounts.size() * 2))
                .isZero();
    }

    @Test
    void testNegativeSubtaskIndexRejected() {
        final DefaultSubtaskAttemptNumberStore subtaskAttemptNumberStore =
                new DefaultSubtaskAttemptNumberStore(Collections.emptyList());

        assertThatThrownBy(() -> subtaskAttemptNumberStore.getAttemptCount(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
