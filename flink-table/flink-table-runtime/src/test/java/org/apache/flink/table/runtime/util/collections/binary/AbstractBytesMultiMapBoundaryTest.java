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

package org.apache.flink.table.runtime.util.collections.binary;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for integer overflow protection in {@link AbstractBytesMultiMap#writePointer} and {@link
 * AbstractBytesMultiMap#skipPointer}.
 *
 * <p>Without the overflow fix, offsets exceeding Integer.MAX_VALUE are silently truncated via
 * {@code (int) longValue}, leading to corrupt pointers. These tests call the actual production
 * methods via reflection and use a mock output view to simulate large offsets without allocating
 * 2GB of memory.
 */
class AbstractBytesMultiMapBoundaryTest {

    private static final int SEGMENT_SIZE = 32 * 1024;

    @Test
    void testWritePointerThrowsOnOverflow() throws Exception {
        BytesMultiMap map = createMinimalMap();
        try {
            long overflowOffset = (long) Integer.MAX_VALUE + 1;
            SimpleCollectingOutputView outputView = createOutputViewWithOffset(overflowOffset);

            Method writePointer =
                    AbstractBytesMultiMap.class.getDeclaredMethod(
                            "writePointer", SimpleCollectingOutputView.class, int.class);
            writePointer.setAccessible(true);

            assertThatThrownBy(() -> invokeReflective(writePointer, map, outputView, -1))
                    .isInstanceOf(EOFException.class);
        } finally {
            map.free();
        }
    }

    @Test
    void testSkipPointerThrowsOnOverflow() throws Exception {
        BytesMultiMap map = createMinimalMap();
        try {
            long overflowOffset = (long) Integer.MAX_VALUE + 1;
            SimpleCollectingOutputView outputView = createOutputViewWithOffset(overflowOffset);

            Method skipPointer =
                    AbstractBytesMultiMap.class.getDeclaredMethod(
                            "skipPointer", SimpleCollectingOutputView.class);
            skipPointer.setAccessible(true);

            assertThatThrownBy(() -> invokeReflective(skipPointer, map, outputView))
                    .isInstanceOf(EOFException.class);
        } finally {
            map.free();
        }
    }

    @Test
    void testWritePointerSucceedsAtExactMax() throws Exception {
        BytesMultiMap map = createMinimalMap();
        try {
            SimpleCollectingOutputView outputView = createOutputViewWithOffset(Integer.MAX_VALUE);

            Method writePointer =
                    AbstractBytesMultiMap.class.getDeclaredMethod(
                            "writePointer", SimpleCollectingOutputView.class, int.class);
            writePointer.setAccessible(true);

            long result = (long) invokeReflective(writePointer, map, outputView, -1);
            assertThat(result).isEqualTo(Integer.MAX_VALUE);
        } finally {
            map.free();
        }
    }

    @Test
    void testWritePointerThrowsAtDoubleMax() throws Exception {
        BytesMultiMap map = createMinimalMap();
        try {
            long doubleMax = 2L * Integer.MAX_VALUE;
            SimpleCollectingOutputView outputView = createOutputViewWithOffset(doubleMax);

            Method writePointer =
                    AbstractBytesMultiMap.class.getDeclaredMethod(
                            "writePointer", SimpleCollectingOutputView.class, int.class);
            writePointer.setAccessible(true);

            assertThatThrownBy(() -> invokeReflective(writePointer, map, outputView, -1))
                    .isInstanceOf(EOFException.class);
        } finally {
            map.free();
        }
    }

    private Object invokeReflective(Method method, Object target, Object... args) throws Exception {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private BytesMultiMap createMinimalMap() {
        LogicalType[] keyTypes = new LogicalType[] {new IntType()};
        LogicalType[] valueTypes = new LogicalType[] {new IntType()};
        int memorySize = 1024 * 1024 + 64 * 1024;
        MemoryManager memoryManager =
                MemoryManagerBuilder.newBuilder().setMemorySize(memorySize).build();
        return new BytesMultiMap(this, memoryManager, memorySize, keyTypes, valueTypes);
    }

    private SimpleCollectingOutputView createOutputViewWithOffset(long simulatedOffset) {
        return new SimpleCollectingOutputView(
                new ArrayList<>(), new TestMemorySegmentSource(), SEGMENT_SIZE) {
            @Override
            public long getCurrentOffset() {
                return simulatedOffset;
            }
        };
    }

    private static class TestMemorySegmentSource implements MemorySegmentSource {
        @Override
        public MemorySegment nextSegment() {
            return MemorySegmentFactory.allocateUnpooledSegment(SEGMENT_SIZE);
        }
    }
}
