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

package org.apache.flink.core.memory;

import org.junit.Test;

import java.nio.ByteBuffer;

/** Tests for the sanity checks of the memory segments. */
public class MemorySegmentChecksTest {

    @Test(expected = NullPointerException.class)
    public void testHeapNullBuffer2() {
        new MemorySegment((byte[]) null, new Object());
    }

    @Test(expected = NullPointerException.class)
    public void testOffHeapNullBuffer2() {
        new MemorySegment((ByteBuffer) null, new Object());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonDirectBuffer() {
        new MemorySegment(ByteBuffer.allocate(1024), new Object());
    }
}
