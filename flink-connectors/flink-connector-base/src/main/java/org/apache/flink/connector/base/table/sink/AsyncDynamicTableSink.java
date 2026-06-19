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

package org.apache.flink.connector.base.table.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract wrapper class for {@link DynamicTableSink} with attributes of {@link
 * org.apache.flink.connector.base.sink.AsyncSinkBase}. Concrete Async Sink implementations should
 * extend this class to add Table API support.
 *
 * @param <RequestEntryT> Request entry type for {@link AsyncSinkBase}.
 */
@PublicEvolving
public abstract class AsyncDynamicTableSink<RequestEntryT extends Serializable>
        implements DynamicTableSink {
    protected final Integer maxBatchSize;
    protected final Integer maxInFlightRequests;
    protected final Integer maxBufferedRequests;
    protected final Long maxBufferSizeInBytes;
    protected final Long maxTimeInBufferMS;

    protected AsyncDynamicTableSink(
            @Nullable Integer maxBatchSize,
            @Nullable Integer maxInFlightRequests,
            @Nullable Integer maxBufferedRequests,
            @Nullable Long maxBufferSizeInBytes,
            @Nullable Long maxTimeInBufferMS) {
        this.maxBatchSize = maxBatchSize;
        this.maxInFlightRequests = maxInFlightRequests;
        this.maxBufferedRequests = maxBufferedRequests;
        this.maxBufferSizeInBytes = maxBufferSizeInBytes;
        this.maxTimeInBufferMS = maxTimeInBufferMS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        AsyncDynamicTableSink<RequestEntryT> that;
        try {
            that = (AsyncDynamicTableSink<RequestEntryT>) o;
        } catch (ClassCastException e) {
            return false;
        }

        return Objects.equals(maxBatchSize, that.maxBatchSize)
                && Objects.equals(maxBufferedRequests, that.maxBufferedRequests)
                && Objects.equals(maxInFlightRequests, that.maxInFlightRequests)
                && Objects.equals(maxBufferSizeInBytes, that.maxBufferSizeInBytes)
                && Objects.equals(maxTimeInBufferMS, that.maxTimeInBufferMS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                maxBatchSize,
                maxBufferedRequests,
                maxInFlightRequests,
                maxBufferSizeInBytes,
                maxTimeInBufferMS);
    }

    protected AsyncSinkBaseBuilder<?, RequestEntryT, ?> addAsyncOptionsToSinkBuilder(
            AsyncSinkBaseBuilder<?, RequestEntryT, ?> builder) {
        Optional.ofNullable(maxBatchSize).ifPresent(builder::setMaxBatchSize);
        Optional.ofNullable(maxBufferSizeInBytes).ifPresent(builder::setMaxBatchSizeInBytes);
        Optional.ofNullable(maxInFlightRequests).ifPresent(builder::setMaxInFlightRequests);
        Optional.ofNullable(maxBufferedRequests).ifPresent(builder::setMaxBufferedRequests);
        Optional.ofNullable(maxTimeInBufferMS).ifPresent(builder::setMaxTimeInBufferMS);
        return builder;
    }
}
