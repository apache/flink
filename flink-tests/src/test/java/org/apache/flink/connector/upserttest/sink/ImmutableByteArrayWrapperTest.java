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

package org.apache.flink.connector.upserttest.sink;

import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ImmutableByteArrayWrapper}. */
@ExtendWith(TestLoggerExtension.class)
class ImmutableByteArrayWrapperTest {

    @Test
    void testConstructorCopy() {
        byte[] array = "immutability of constructor".getBytes();
        byte[] clonedArray = new ImmutableByteArrayWrapper(array).bytes;
        assertCopyIsReferenceFree(array, clonedArray);
    }

    @Test
    void testGetterCopy() {
        byte[] array = "immutability of getter".getBytes();
        byte[] clonedArray = new ImmutableByteArrayWrapper(array).array();
        assertCopyIsReferenceFree(array, clonedArray);
    }

    private static void assertCopyIsReferenceFree(byte[] original, byte[] clone) {
        assertThat(clone).isNotSameAs(original);
        assertThat(clone).isEqualTo(original);
        Arrays.fill(original, (byte) 0);
        assertThat(clone).isNotEqualTo(original);
    }
}
