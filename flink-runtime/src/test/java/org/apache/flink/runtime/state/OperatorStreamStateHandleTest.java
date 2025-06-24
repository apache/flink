/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OperatorStreamStateHandleTest {

    @Test
    void testFixedEnumOrder() {

        // Ensure the order / ordinal of all values of enum 'mode' are fixed, as this is used for
        // serialization
        assertThat(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE.ordinal()).isZero();
        assertThat(OperatorStateHandle.Mode.UNION.ordinal()).isOne();
        assertThat(OperatorStateHandle.Mode.BROADCAST.ordinal()).isEqualTo(2);

        // Ensure all enum values are registered and fixed forever by this test
        assertThat(OperatorStateHandle.Mode.values()).hasSize(3);

        // Byte is used to encode enum value on serialization
        assertThat(OperatorStateHandle.Mode.values()).hasSizeLessThanOrEqualTo(Byte.MAX_VALUE);
    }
}
