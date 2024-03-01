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

package org.apache.flink.util;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link OutputTag}. */
class OutputTagTest {

    @Test
    void testNullRejected() {
        assertThatThrownBy(() -> new OutputTag<Integer>(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testNullRejectedWithTypeInfo() {
        assertThatThrownBy(() -> new OutputTag<>(null, BasicTypeInfo.INT_TYPE_INFO))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testEmptyStringRejected() {
        assertThatThrownBy(() -> new OutputTag<Integer>(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEmptyStringRejectedWithTypeInfo() {
        assertThatThrownBy(() -> new OutputTag<>("", BasicTypeInfo.INT_TYPE_INFO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
