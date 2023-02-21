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

import java.io.Serializable;

/**
 * Builder class for {@link AsyncDynamicTableSink}.
 *
 * @param <RequestEntryT> Request entry type for {@link AsyncDynamicTableSink}.
 * @param <ConcreteBuilderT> Builder Implementation type.
 */
@PublicEvolving
public abstract class AsyncDynamicTableSinkBuilder<
        RequestEntryT extends Serializable,
        ConcreteBuilderT extends AsyncDynamicTableSinkBuilder<?, ?>> {

    private Integer maxBatchSize;
    private Integer maxInFlightRequests;
    private Integer maxBufferedRequests;
    private Long maxBufferSizeInBytes;
    private Long maxTimeInBufferMS;

    /**
     * @param maxBatchSize maximum number of elements that may be passed in a list to be written
     *     downstream.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        return (ConcreteBuilderT) this;
    }

    /**
     * @param maxInFlightRequests maximum number of uncompleted calls to submitRequestEntries that
     *     the SinkWriter will allow at any given point. Once this point has reached, writes and
     *     callbacks to add elements to the buffer may block until one or more requests to
     *     submitRequestEntries completes.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxInFlightRequests(int maxInFlightRequests) {
        this.maxInFlightRequests = maxInFlightRequests;
        return (ConcreteBuilderT) this;
    }

    /**
     * @param maxBufferedRequests the maximum buffer length. Callbacks to add elements to the buffer
     *     and calls to write will block if this length has been reached and will only unblock if
     *     elements from the buffer have been removed for flushing.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxBufferedRequests(int maxBufferedRequests) {
        this.maxBufferedRequests = maxBufferedRequests;
        return (ConcreteBuilderT) this;
    }

    /**
     * @param maxBufferSizeInBytes a flush will be attempted if the most recent call to write
     *     introduces an element to the buffer such that the total size of the buffer is greater
     *     than or equal to this threshold value.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxBufferSizeInBytes(long maxBufferSizeInBytes) {
        this.maxBufferSizeInBytes = maxBufferSizeInBytes;
        return (ConcreteBuilderT) this;
    }

    /**
     * @param maxTimeInBufferMS the maximum amount of time an element may remain in the buffer. In
     *     most cases elements are flushed as a result of the batch size (in bytes or number) being
     *     reached or during a snapshot. However, there are scenarios where an element may remain in
     *     the buffer forever or a long period of time. To mitigate this, a timer is constantly
     *     active in the buffer such that: while the buffer is not empty, it will flush every
     *     maxTimeInBufferMS milliseconds.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxTimeInBufferMS(long maxTimeInBufferMS) {
        this.maxTimeInBufferMS = maxTimeInBufferMS;
        return (ConcreteBuilderT) this;
    }

    public abstract AsyncDynamicTableSink<RequestEntryT> build();

    protected Integer getMaxBatchSize() {
        return maxBatchSize;
    }

    protected Integer getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    protected Integer getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    protected Long getMaxBufferSizeInBytes() {
        return maxBufferSizeInBytes;
    }

    protected Long getMaxTimeInBufferMS() {
        return maxTimeInBufferMS;
    }
}
