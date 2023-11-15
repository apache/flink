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
package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.KeyGroupRange;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InternalKeyContextImpl}. */
class InternalKeyContextImplTest {

    @Test
    void testSetKeyGroupIndexWithinRange() {
        InternalKeyContextImpl<Integer> integerInternalKeyContext =
                new InternalKeyContextImpl<>(KeyGroupRange.of(0, 128), 4096);
        // There will be no exception thrown since the key group index is within the range.
        integerInternalKeyContext.setCurrentKeyGroupIndex(64);
    }

    @Test
    void testSetKeyGroupIndexOutOfRange() {
        InternalKeyContextImpl<Integer> integerInternalKeyContext =
                new InternalKeyContextImpl<>(KeyGroupRange.of(0, 128), 4096);
        // There will be an IllegalArgumentException thrown since the key group index is out of the
        // range.
        assertThatThrownBy(() -> integerInternalKeyContext.setCurrentKeyGroupIndex(2048))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
