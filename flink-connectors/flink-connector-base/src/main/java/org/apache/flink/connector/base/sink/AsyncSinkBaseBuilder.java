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

package org.apache.flink.connector.base.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.writer.ElementConverter;

import java.io.Serializable;

/**
 * Abstract builder for constructing a concrete implementation of {@link AsyncSinkBase}.
 *
 * @param <InputT> type of elements that should be persisted in the destination
 * @param <RequestEntryT> type of payload that contains the element and additional metadata that is
 *     required to submit a single element to the destination
 * @param <ConcreteBuilderT> type of concrete implementation of this builder class
 */
@PublicEvolving
public abstract class AsyncSinkBaseBuilder<
        InputT,
        RequestEntryT extends Serializable,
        ConcreteBuilderT extends AsyncSinkBaseBuilder<?, ?, ?>> {

    private ElementConverter<InputT, RequestEntryT> elementConverter;
    private Integer maxBatchSize;
    private Integer maxInFlightRequests;
    private Integer maxBufferedRequests;
    private Long maxBatchSizeInBytes;
    private Long maxTimeInBufferMS;
    private Long maxRecordSizeInBytes;

    /**
     * @param elementConverter the {@link ElementConverter} to be used for the sink
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setElementConverter(
            ElementConverter<InputT, RequestEntryT> elementConverter) {
        this.elementConverter = elementConverter;
        return (ConcreteBuilderT) this;
    }

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
     * @param maxBatchSizeInBytes a flush will be attempted if the most recent call to write
     *     introduces an element to the buffer such that the total size of the buffer is greater
     *     than or equal to this threshold value. If this happens, the maximum number of elements
     *     from the head of the buffer will be selected, that is smaller than {@code
     *     maxBatchSizeInBytes} in size will be flushed.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxBatchSizeInBytes(long maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
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

    /**
     * @param maxRecordSizeInBytes the maximum size of each records in bytes. If a record larger
     *     than this is passed to the sink, it will throw an {@code IllegalArgumentException}.
     * @return {@link ConcreteBuilderT} itself
     */
    public ConcreteBuilderT setMaxRecordSizeInBytes(long maxRecordSizeInBytes) {
        this.maxRecordSizeInBytes = maxRecordSizeInBytes;
        return (ConcreteBuilderT) this;
    }

    /** Builds the Sink with the settings applied to this builder. */
    public abstract AsyncSinkBase<InputT, RequestEntryT> build();

    protected ElementConverter<InputT, RequestEntryT> getElementConverter() {
        return elementConverter;
    }

    protected Integer getMaxBatchSize() {
        return maxBatchSize;
    }

    protected Integer getMaxInFlightRequests() {
        return maxInFlightRequests;
    }

    protected Integer getMaxBufferedRequests() {
        return maxBufferedRequests;
    }

    protected Long getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    protected Long getMaxTimeInBufferMS() {
        return maxTimeInBufferMS;
    }

    protected Long getMaxRecordSizeInBytes() {
        return maxRecordSizeInBytes;
    }
}
