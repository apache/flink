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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UnionResultSubpartitionView}. */
class UnionResultSubpartitionViewTest {

    private UnionResultSubpartitionView view;

    private List<Buffer> buffers0;

    private ResultSubpartitionView view0;

    private List<Buffer> buffers1;

    private ResultSubpartitionView view1;

    @BeforeEach
    void before() {
        view = new UnionResultSubpartitionView((ResultSubpartitionView x) -> {}, 2);

        buffers0 =
                Arrays.asList(
                        TestBufferFactory.createBuffer(10),
                        TestBufferFactory.createBuffer(10),
                        TestBufferFactory.createBuffer(10, Buffer.DataType.EVENT_BUFFER));
        view0 = new TestingResultSubpartitionView(view, buffers0);
        view.notifyViewCreated(0, view0);

        buffers1 =
                Arrays.asList(
                        TestBufferFactory.createBuffer(10),
                        TestBufferFactory.createBuffer(10),
                        TestBufferFactory.createBuffer(10, Buffer.DataType.EVENT_BUFFER));
        view1 = new TestingResultSubpartitionView(view, buffers1);
        view.notifyViewCreated(1, view1);
    }

    @Test
    void testGetNextBuffer() throws IOException {
        assertThat(view.peekNextBufferSubpartitionId()).isEqualTo(-1);
        assertThat(view.getNextBuffer()).isNull();
        view0.notifyDataAvailable();
        assertThat(view.peekNextBufferSubpartitionId()).isZero();
        ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
        assertThat(bufferAndBacklog.buffer()).isEqualTo(buffers0.get(0));
        assertThat(bufferAndBacklog.buffersInBacklog()).isEqualTo(buffers0.size() - 1);

        view1.notifyDataAvailable();
        assertThat(view.peekNextBufferSubpartitionId()).isZero();
        assertThat(view.getNextBuffer().buffer()).isEqualTo(buffers0.get(1));

        List<Buffer> buffers = new ArrayList<>();
        while (view.getAvailabilityAndBacklog(true).isAvailable()) {
            buffers.add(view.getNextBuffer().buffer());
        }
        assertThat(buffers)
                .hasSize(buffers0.size() + buffers1.size() - 2)
                .containsSubsequence(buffers0.subList(2, buffers0.size()))
                .containsSubsequence(buffers1);
    }

    @Test
    void testGetAvailabilityAndBacklog() throws IOException {
        view0.notifyDataAvailable();
        view1.notifyDataAvailable();

        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog1 =
                view.getAvailabilityAndBacklog(false);
        assertThat(availabilityAndBacklog1.getBacklog()).isPositive();
        assertThat(availabilityAndBacklog1.isAvailable()).isFalse();
        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog2 =
                view.getAvailabilityAndBacklog(true);
        assertThat(availabilityAndBacklog2.getBacklog()).isPositive();
        assertThat(availabilityAndBacklog2.isAvailable()).isTrue();

        for (int i = 1; i < buffers0.size() + buffers1.size(); i++) {
            view.getNextBuffer();
        }

        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog3 =
                view.getAvailabilityAndBacklog(false);
        assertThat(availabilityAndBacklog3.getBacklog()).isZero();
        assertThat(availabilityAndBacklog3.isAvailable()).isTrue();
        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog4 =
                view.getAvailabilityAndBacklog(true);
        assertThat(availabilityAndBacklog4.getBacklog()).isZero();
        assertThat(availabilityAndBacklog4.isAvailable()).isTrue();
    }

    @Test
    void testReleaseAllResources() throws IOException {
        assertThat(view.isReleased()).isFalse();
        assertThat(view0.isReleased()).isFalse();
        assertThat(view1.isReleased()).isFalse();
        assertThat(buffers0).allMatch(x -> !x.isRecycled());
        assertThat(buffers1).allMatch(x -> !x.isRecycled());

        // Verifies that cached buffers are also recycled.
        view0.notifyDataAvailable();

        view.releaseAllResources();

        assertThat(view.isReleased()).isTrue();
        assertThat(view0.isReleased()).isTrue();
        assertThat(view1.isReleased()).isTrue();
        assertThat(buffers0).allMatch(Buffer::isRecycled);
        assertThat(buffers1).allMatch(Buffer::isRecycled);
    }

    @Test
    public void testDataAvailableBeforeRegistration() {
        TestAvailabilityListener listener = new TestAvailabilityListener();
        view = new UnionResultSubpartitionView(listener, 2);
        view0 = new TestingResultSubpartitionView(view, buffers0);
        view1 = new TestingResultSubpartitionView(view, buffers1);

        view0.notifyDataAvailable();
        assertThat(listener.isDataAvailable()).isFalse();

        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog1 =
                view.getAvailabilityAndBacklog(true);
        assertThat(availabilityAndBacklog1.getBacklog()).isZero();
        assertThat(availabilityAndBacklog1.isAvailable()).isFalse();

        view.notifyViewCreated(0, view0);
        assertThat(listener.isDataAvailable()).isFalse();

        view.notifyViewCreated(1, view1);
        assertThat(listener.isDataAvailable()).isTrue();

        ResultSubpartitionView.AvailabilityWithBacklog availabilityAndBacklog2 =
                view.getAvailabilityAndBacklog(true);
        assertThat(availabilityAndBacklog2.getBacklog()).isPositive();
        assertThat(availabilityAndBacklog2.isAvailable()).isTrue();
    }

    private static class TestingResultSubpartitionView extends NoOpResultSubpartitionView {
        private final BufferAvailabilityListener listener;
        private final List<Buffer> buffers;
        private int sequenceNumber = 0;
        private boolean isReleased = false;

        private TestingResultSubpartitionView(
                BufferAvailabilityListener listener, List<Buffer> buffers) {
            this.listener = listener;
            this.buffers = new ArrayList<>(buffers);
        }

        @Nullable
        @Override
        public ResultSubpartition.BufferAndBacklog getNextBuffer() {
            if (buffers.isEmpty()) {
                return null;
            }
            Buffer buffer = buffers.remove(0);
            return new ResultSubpartition.BufferAndBacklog(
                    buffer,
                    buffers.size(),
                    buffers.isEmpty() ? Buffer.DataType.NONE : buffers.get(0).getDataType(),
                    sequenceNumber++);
        }

        @Override
        public AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
            if (buffers.isEmpty()) {
                return new AvailabilityWithBacklog(false, 0);
            }

            return new AvailabilityWithBacklog(
                    isCreditAvailable || buffers.get(0).getDataType().isEvent(), buffers.size());
        }

        @Override
        public void notifyDataAvailable() {
            listener.notifyDataAvailable(this);
        }

        @Override
        public void releaseAllResources() {
            buffers.forEach(Buffer::recycleBuffer);
            buffers.clear();
            isReleased = true;
        }

        @Override
        public boolean isReleased() {
            return isReleased;
        }
    }

    private static class TestAvailabilityListener implements BufferAvailabilityListener {
        private boolean isDataAvailable = false;

        @Override
        public void notifyDataAvailable(ResultSubpartitionView view) {
            isDataAvailable = true;
        }

        boolean isDataAvailable() {
            return isDataAvailable;
        }
    }
}
