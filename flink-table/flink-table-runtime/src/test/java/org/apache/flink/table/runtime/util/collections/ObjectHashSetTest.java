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

package org.apache.flink.table.runtime.util.collections;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectHashSetTest {
    @Test
    void testBinaryContentMembership() {
        ObjectHashSet<byte[]> set = new ObjectHashSet<>();
        assertThat(set.add(new byte[] {(byte) 0x80, (byte) 0xff, 0})).isTrue();
        assertThat(set.contains(new byte[] {(byte) 0x80, (byte) 0xff, 0})).isTrue();
        assertThat(set.add(new byte[] {(byte) 0x80, (byte) 0xff, 0})).isFalse();
        assertThat(set.contains(new byte[] {(byte) 0x80, (byte) 0xff})).isFalse();
        assertThat(set.contains(new byte[] {(byte) 0x80, (byte) 0xfe, 0})).isFalse();
        assertThat(set.add(new byte[0])).isTrue();
        assertThat(set.contains(new byte[0])).isTrue();
        set.optimize();
        assertThat(set.contains(new byte[] {(byte) 0x80, (byte) 0xff, 0})).isTrue();
    }

    @Test
    void testStoredBinaryKeyDoesNotAliasInput() {
        ObjectHashSet<byte[]> set = new ObjectHashSet<>();
        byte[] key = {1, 2};
        set.add(key);
        key[0] = 3;
        assertThat(set.contains(new byte[] {1, 2})).isTrue();
        assertThat(set.contains(key)).isFalse();
    }

    @Test
    void testOtherObjectKeysAndNullFlag() {
        ObjectHashSet<String> set = new ObjectHashSet<>();
        assertThat(set.add("hello")).isTrue();
        assertThat(set.contains(new String("hello"))).isTrue();
        assertThat(set.add("hello")).isFalse();
        assertThat(set.contains("world")).isFalse();
        assertThat(set.containsNull()).isFalse();
        set.addNull();
        assertThat(set.containsNull()).isTrue();
        assertThat(set.contains("world")).isFalse();
    }
}
