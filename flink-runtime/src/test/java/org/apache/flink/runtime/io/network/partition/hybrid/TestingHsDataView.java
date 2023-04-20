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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/** Mock {@link HsDataView} for testing. */
public class TestingHsDataView implements HsDataView {
    public static final TestingHsDataView NO_OP = TestingHsDataView.builder().build();

    private final FunctionWithException<
                    Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
            consumeBufferFunction;

    private final Function<Integer, Buffer.DataType> peekNextToConsumeDataTypeFunction;

    private final Supplier<Integer> getBacklogSupplier;

    private final Runnable releaseDataViewRunnable;

    private TestingHsDataView(
            FunctionWithException<Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
                    consumeBufferFunction,
            Function<Integer, Buffer.DataType> peekNextToConsumeDataTypeFunction,
            Supplier<Integer> getBacklogSupplier,
            Runnable releaseDataViewRunnable) {
        this.consumeBufferFunction = consumeBufferFunction;
        this.peekNextToConsumeDataTypeFunction = peekNextToConsumeDataTypeFunction;
        this.getBacklogSupplier = getBacklogSupplier;
        this.releaseDataViewRunnable = releaseDataViewRunnable;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(
            int nextBufferToConsume, Collection<Buffer> buffersToRecycle) throws Throwable {
        return consumeBufferFunction.apply(nextBufferToConsume);
    }

    @Override
    public Buffer.DataType peekNextToConsumeDataType(
            int nextBufferToConsume, Collection<Buffer> buffersToRecycle) {
        return peekNextToConsumeDataTypeFunction.apply(nextBufferToConsume);
    }

    @Override
    public int getBacklog() {
        return getBacklogSupplier.get();
    }

    @Override
    public void releaseDataView() {
        releaseDataViewRunnable.run();
    }

    /** Builder for {@link TestingHsDataView}. */
    public static class Builder {
        private FunctionWithException<
                        Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
                consumeBufferFunction = (ignore) -> Optional.empty();

        private Function<Integer, Buffer.DataType> peekNextToConsumeDataTypeFunction =
                (ignore) -> Buffer.DataType.NONE;

        private Supplier<Integer> getBacklogSupplier = () -> 0;

        private Runnable releaseDataViewRunnable = () -> {};

        private Builder() {}

        public Builder setConsumeBufferFunction(
                FunctionWithException<
                                Integer, Optional<ResultSubpartition.BufferAndBacklog>, Throwable>
                        consumeBufferFunction) {
            this.consumeBufferFunction = consumeBufferFunction;
            return this;
        }

        public Builder setPeekNextToConsumeDataTypeFunction(
                Function<Integer, Buffer.DataType> peekNextToConsumeDataTypeFunction) {
            this.peekNextToConsumeDataTypeFunction = peekNextToConsumeDataTypeFunction;
            return this;
        }

        public Builder setGetBacklogSupplier(Supplier<Integer> getBacklogSupplier) {
            this.getBacklogSupplier = getBacklogSupplier;
            return this;
        }

        public Builder setReleaseDataViewRunnable(Runnable releaseDataViewRunnable) {
            this.releaseDataViewRunnable = releaseDataViewRunnable;
            return this;
        }

        public TestingHsDataView build() {
            return new TestingHsDataView(
                    consumeBufferFunction,
                    peekNextToConsumeDataTypeFunction,
                    getBacklogSupplier,
                    releaseDataViewRunnable);
        }
    }
}
