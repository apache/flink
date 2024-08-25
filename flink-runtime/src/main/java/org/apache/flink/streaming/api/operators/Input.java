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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.watermark.GeneralizedWatermark;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.GeneralizedWatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * {@link Input} interface used in {@link MultipleInputStreamOperator}. Most likely you don't want
 * to implement this interface on your own. Instead you can use {@link AbstractInput} and {@link
 * AbstractStreamOperatorV2} to implement {@link MultipleInputStreamOperator}, or just {@link
 * AbstractStreamOperatorV2} to implement {@link OneInputStreamOperator}.
 */
@PublicEvolving
public interface Input<IN> {
    /**
     * Processes one element that arrived on this input of the {@link MultipleInputStreamOperator}.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     */
    void processElement(StreamRecord<IN> element) throws Exception;

    /**
     * Processes a {@link Watermark} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.api.watermark.Watermark
     */
    void processWatermark(Watermark mark) throws Exception;

    /**
     * Processes a {@link WatermarkStatus} that arrived on this input of the {@link
     * MultipleInputStreamOperator}. This method is guaranteed to not be called concurrently with
     * other methods of the operator.
     *
     * @see WatermarkStatus
     */
    void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception;

    /**
     * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
    void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;

    /**
     * Set the correct key context before processing the {@code record}. Used for example to extract
     * key from the {@code record} and pass that key to the state backends. This method is
     * guaranteed to not be called concurrently with other methods of the operator.
     */
    void setKeyContextElement(StreamRecord<IN> record) throws Exception;

    /**
     * Processes a {@link RecordAttributes} that arrived at this input. This method is guaranteed to
     * not be called concurrently with other methods of the operator.
     */
    @Experimental
    default void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {}

    /**
     * Processes a {@link GeneralizedWatermark} that arrived at this input. This method is
     * guaranteed to not be called concurrently with other methods of the operator.
     */
    @Experimental
    default void processGeneralizedWatermark(GeneralizedWatermarkEvent watermark)
            throws Exception {}
}
