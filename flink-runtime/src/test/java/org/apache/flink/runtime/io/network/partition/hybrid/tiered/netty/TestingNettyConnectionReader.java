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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/** Test implementation for {@link NettyConnectionReader}. */
public class TestingNettyConnectionReader implements NettyConnectionReader {

    private final Function<Integer, Buffer> readBufferFunction;

    private final Supplier<Integer> peekNextBufferSubpartitionIdSupplier;

    private TestingNettyConnectionReader(
            Function<Integer, Buffer> readBufferFunction,
            Supplier<Integer> peekNextBufferSubpartitionIdSupplier) {
        this.readBufferFunction = readBufferFunction;
        this.peekNextBufferSubpartitionIdSupplier = peekNextBufferSubpartitionIdSupplier;
    }

    @Override
    public int peekNextBufferSubpartitionId() throws IOException {
        return peekNextBufferSubpartitionIdSupplier.get();
    }

    @Override
    public Optional<Buffer> readBuffer(int subpartitionId, int segmentId) {
        return Optional.of(readBufferFunction.apply(segmentId));
    }

    /** Builder for {@link TestingNettyConnectionReader}. */
    public static class Builder {

        private Function<Integer, Buffer> readBufferFunction = segmentId -> null;

        private Supplier<Integer> peekNextBufferSubpartitionIdSupplier = () -> -1;

        public Builder() {}

        public Builder setReadBufferFunction(Function<Integer, Buffer> readBufferFunction) {
            this.readBufferFunction = readBufferFunction;
            return this;
        }

        public Builder setPeekNextBufferSubpartitionIdSupplier(
                Supplier<Integer> peekNextBufferSubpartitionIdSupplier) {
            this.peekNextBufferSubpartitionIdSupplier = peekNextBufferSubpartitionIdSupplier;
            return this;
        }

        public TestingNettyConnectionReader build() {
            return new TestingNettyConnectionReader(
                    readBufferFunction, peekNextBufferSubpartitionIdSupplier);
        }
    }
}
