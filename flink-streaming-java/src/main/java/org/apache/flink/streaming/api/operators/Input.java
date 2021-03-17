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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** {@link Input} interface used in {@link MultipleInputStreamOperator}. */
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
     * Processes a {@link LatencyMarker} that arrived on the first input of this two-input operator.
     * This method is guaranteed to not be called concurrently with other methods of the operator.
     *
     * @see org.apache.flink.streaming.runtime.streamrecord.LatencyMarker
     */
    void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;

    void setKeyContextElement(StreamRecord<IN> record) throws Exception;
}
