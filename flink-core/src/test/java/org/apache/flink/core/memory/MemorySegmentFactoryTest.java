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

package org.apache.flink.core.memory;

import org.junit.jupiter.api.Test;

import static java.lang.System.arraycopy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/** {@link MemorySegmentFactory} test. */
class MemorySegmentFactoryTest {

    @Test
    void testWrapCopyChangingData() {
        byte[] data = {1, 2, 3, 4, 5};
        byte[] changingData = new byte[data.length];
        arraycopy(data, 0, changingData, 0, data.length);
        MemorySegment segment = MemorySegmentFactory.wrapCopy(changingData, 0, changingData.length);
        changingData[0]++;
        assertThat(segment.getHeapMemory()).containsExactly(data);
    }

    @Test
    void testWrapPartialCopy() {
        byte[] data = {1, 2, 3, 5, 6};
        MemorySegment segment = MemorySegmentFactory.wrapCopy(data, 0, data.length / 2);
        byte[] exp = new byte[segment.size()];
        arraycopy(data, 0, exp, 0, exp.length);
        assertThat(segment.getHeapMemory()).containsExactly(exp);
    }

    @Test
    void testWrapCopyEmpty() {
        MemorySegmentFactory.wrapCopy(new byte[0], 0, 0);
    }

    @Test
    void testWrapCopyWrongStart() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> MemorySegmentFactory.wrapCopy(new byte[] {1, 2, 3}, 10, 3));
    }

    @Test
    void testWrapCopyWrongEnd() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> MemorySegmentFactory.wrapCopy(new byte[] {1, 2, 3}, 0, 10));
    }
}
