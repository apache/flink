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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JoinConditionWithNullFilters}. */
class JoinConditionWithNullFiltersTest {

    private final TestKeyContext keyContext = new TestKeyContext();

    /** The first key field is compared with {@code =}, the second with IS NOT DISTINCT FROM. */
    @Test
    void testMixedKeyCannotMatchOnlyWithNullInFilteredField() {
        final JoinConditionWithNullFilters condition =
                new JoinConditionWithNullFilters(null, new boolean[] {true, false}, keyContext);

        keyContext.setCurrentKey(key(null, 1));
        assertThat(condition.currentKeyCannotMatch()).isTrue();
        keyContext.setCurrentKey(key(1, null));
        assertThat(condition.currentKeyCannotMatch()).isFalse();
        keyContext.setCurrentKey(key(1, 1));
        assertThat(condition.currentKeyCannotMatch()).isFalse();
    }

    @Test
    void testNullSafeKeyCanAlwaysMatch() {
        final JoinConditionWithNullFilters condition =
                new JoinConditionWithNullFilters(null, new boolean[] {false, false}, keyContext);

        keyContext.setCurrentKey(key(null, null));
        assertThat(condition.currentKeyCannotMatch()).isFalse();
    }

    private static BinaryRowData key(Integer first, Integer second) {
        final BinaryRowData key = new BinaryRowData(2);
        final BinaryRowWriter writer = new BinaryRowWriter(key);
        final Integer[] fields = {first, second};
        for (int i = 0; i < fields.length; i++) {
            if (fields[i] == null) {
                writer.setNullAt(i);
            } else {
                writer.writeInt(i, fields[i]);
            }
        }
        writer.complete();
        return key;
    }

    private static class TestKeyContext implements KeyContext {

        private Object currentKey;

        @Override
        public void setCurrentKey(Object key) {
            currentKey = key;
        }

        @Override
        public Object getCurrentKey() {
            return currentKey;
        }
    }
}
