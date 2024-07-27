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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;

import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link AbstractPagedInputView} and {@link AbstractPagedOutputView}. */
class PagedViewsTest {

    @Test
    void testSequenceOfIntegersWithAlignedBuffers() throws Exception {
        final int numInts = 1000000;
        testSequenceOfTypes(Util.randomRecords(numInts, SerializationTestTypeFactory.INT), 2048);
    }

    @Test
    void testSequenceOfIntegersWithUnalignedBuffers() throws Exception {
        final int numInts = 1000000;
        testSequenceOfTypes(Util.randomRecords(numInts, SerializationTestTypeFactory.INT), 2047);
    }

    @Test
    void testRandomTypes() throws Exception {
        final int numTypes = 100000;

        // test with an odd buffer size to force many unaligned cases
        testSequenceOfTypes(Util.randomRecords(numTypes), 57);
    }

    @Test
    void testReadFully() throws IOException {
        int bufferSize = 100;
        byte[] expected = new byte[bufferSize];
        new Random().nextBytes(expected);

        TestOutputView outputView = new TestOutputView(bufferSize);
        outputView.write(expected);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[bufferSize];

        inputView.readFully(buffer);

        assertThat(inputView.getCurrentPositionInSegment()).isEqualTo(bufferSize);
        assertThat(buffer).isEqualTo(expected);
    }

    @Test
    void testReadFullyAcrossSegments() throws Exception {
        int bufferSize = 100;
        int segmentSize = 30;
        byte[] expected = new byte[bufferSize];
        new Random().nextBytes(expected);

        TestOutputView outputView = new TestOutputView(segmentSize);
        outputView.write(expected);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[bufferSize];
        inputView.readFully(buffer);

        assertThat(inputView.getCurrentPositionInSegment()).isEqualTo(bufferSize % segmentSize);
        assertThat(buffer).isEqualTo(expected);
    }

    @Test
    void testReadAcrossSegments() throws Exception {
        int bufferSize = 100;
        int bytes2Write = 75;
        int segmentSize = 30;
        byte[] expected = new byte[bytes2Write];
        new Random().nextBytes(expected);

        TestOutputView outputView = new TestOutputView(segmentSize);
        outputView.write(expected);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[bufferSize];
        int bytesRead = inputView.read(buffer);

        assertThat(bytesRead).isEqualTo(bytes2Write);
        assertThat(inputView.getCurrentPositionInSegment()).isEqualTo(bytes2Write % segmentSize);

        byte[] tempBuffer = new byte[bytesRead];
        System.arraycopy(buffer, 0, tempBuffer, 0, bytesRead);
        assertThat(tempBuffer).isEqualTo(expected);
    }

    @Test
    void testEmptyingInputView() throws Exception {
        int bufferSize = 100;
        int bytes2Write = 75;
        int segmentSize = 30;
        byte[] expected = new byte[bytes2Write];
        new Random().nextBytes(expected);

        TestOutputView outputView = new TestOutputView(segmentSize);
        outputView.write(expected);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[bufferSize];
        int bytesRead = inputView.read(buffer);
        assertThat(bytesRead).isEqualTo(bytes2Write);

        byte[] tempBuffer = new byte[bytesRead];
        System.arraycopy(buffer, 0, tempBuffer, 0, bytesRead);
        assertThat(tempBuffer).isEqualTo(expected);

        bytesRead = inputView.read(buffer);
        assertThat(bytesRead).isEqualTo(-1);
        assertThat(inputView.getCurrentPositionInSegment()).isEqualTo(bytes2Write % segmentSize);
    }

    @Test
    void testReadFullyWithNotEnoughData() throws Exception {
        int bufferSize = 100;
        int bytes2Write = 99;
        int segmentSize = 30;
        byte[] expected = new byte[bytes2Write];
        new Random().nextBytes(expected);

        TestOutputView outputView = new TestOutputView(segmentSize);
        outputView.write(expected);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[bufferSize];

        assertThatThrownBy(() -> inputView.readFully(buffer)).isInstanceOf(EOFException.class);

        int bytesRead = inputView.read(buffer);
        assertThat(bytesRead).isEqualTo(-1);
    }

    @Test
    void testReadFullyWithOffset() throws Exception {
        int bufferSize = 100;
        int segmentSize = 30;
        byte[] expected = new byte[bufferSize];
        new Random().nextBytes(expected);

        TestOutputView outputView = new TestOutputView(segmentSize);
        outputView.write(expected);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[2 * bufferSize];
        inputView.readFully(buffer, bufferSize, bufferSize);
        assertThat(inputView.getCurrentPositionInSegment()).isEqualTo(bufferSize % segmentSize);

        byte[] tempBuffer = new byte[bufferSize];
        System.arraycopy(buffer, bufferSize, tempBuffer, 0, bufferSize);
        assertThat(tempBuffer).isEqualTo(expected);
    }

    @Test
    void testReadFullyEmptyView() {
        int segmentSize = 30;
        TestOutputView outputView = new TestOutputView(segmentSize);
        outputView.close();

        TestInputView inputView = new TestInputView(outputView.segments);
        byte[] buffer = new byte[segmentSize];

        assertThatThrownBy(() -> inputView.readFully(buffer)).isInstanceOf(EOFException.class);
    }

    private static void testSequenceOfTypes(
            Iterable<SerializationTestType> sequence, int segmentSize) throws Exception {

        List<SerializationTestType> elements = new ArrayList<>(512);
        TestOutputView outView = new TestOutputView(segmentSize);

        // write
        for (SerializationTestType type : sequence) {
            // serialize the record
            type.write(outView);
            elements.add(type);
        }
        outView.close();

        // check the records
        TestInputView inView = new TestInputView(outView.segments);

        for (SerializationTestType reference : elements) {
            SerializationTestType result = reference.getClass().newInstance();
            result.read(inView);
            assertThat(result).isEqualTo(reference);
        }
    }

    // ============================================================================================

    private static final class SegmentWithPosition {

        private final MemorySegment segment;
        private final int position;

        SegmentWithPosition(MemorySegment segment, int position) {
            this.segment = segment;
            this.position = position;
        }
    }

    private static final class TestOutputView extends AbstractPagedOutputView {

        private final List<SegmentWithPosition> segments = new ArrayList<>();

        private final int segmentSize;

        private TestOutputView(int segmentSize) {
            super(MemorySegmentFactory.allocateUnpooledSegment(segmentSize), segmentSize, 0);

            this.segmentSize = segmentSize;
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
                throws IOException {
            segments.add(new SegmentWithPosition(current, positionInCurrent));
            return MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
        }

        public void close() {
            segments.add(
                    new SegmentWithPosition(getCurrentSegment(), getCurrentPositionInSegment()));
        }
    }

    private static final class TestInputView extends AbstractPagedInputView {

        private final List<SegmentWithPosition> segments;

        private int num;

        private TestInputView(List<SegmentWithPosition> segments) {
            super(segments.get(0).segment, segments.get(0).position, 0);

            this.segments = segments;
            this.num = 0;
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current) throws IOException {
            num++;
            if (num < segments.size()) {
                return segments.get(num).segment;
            } else {
                throw new EOFException();
            }
        }

        @Override
        protected int getLimitForSegment(MemorySegment segment) {
            return segments.get(num).position;
        }
    }
}
