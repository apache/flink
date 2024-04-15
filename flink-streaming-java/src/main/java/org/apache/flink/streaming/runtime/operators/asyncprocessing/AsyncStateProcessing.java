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

package org.apache.flink.streaming.runtime.operators.asyncprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.ThrowingConsumer;

/** This class defines the basic interfaces to process a state in operator/input layer. */
@Internal
public interface AsyncStateProcessing {

    /**
     * Get if the async state processing is enabled for this input/operator.
     *
     * @return ture if async state processing is enabled.
     */
    boolean isAsyncStateProcessingEnabled();

    /**
     * Get the record processor that could process record from input, which is the only entry for
     * async processing.
     *
     * @param inputId the input identifier, start from 1. Borrow the design from {@code
     *     org.apache.flink.streaming.api.operators.AbstractInput#inputId}. This is only relevant if
     *     there is multiple inputs for the instance.
     */
    <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(int inputId);

    /**
     * Static method helper to make a record processor with given infos.
     *
     * @param asyncOperator the operator that can process state asynchronously.
     * @param keySelector the key selector.
     * @param processor the record processing logic.
     * @return the built record processor that can returned by {@link #getRecordProcessor(int)}.
     */
    static <T> ThrowingConsumer<StreamRecord<T>, Exception> makeRecordProcessor(
            AsyncStateProcessingOperator asyncOperator,
            KeySelector<T, ?> keySelector,
            ThrowingConsumer<StreamRecord<T>, Exception> processor) {
        switch (asyncOperator.getElementOrder()) {
            case RECORD_ORDER:
                return (record) -> {
                    asyncOperator.setAsyncKeyedContextElement(record, keySelector);
                    asyncOperator.preserveRecordOrderAndProcess(() -> processor.accept(record));
                    asyncOperator.postProcessElement();
                };
            case FIRST_STATE_ORDER:
                return (record) -> {
                    asyncOperator.setAsyncKeyedContextElement(record, keySelector);
                    processor.accept(record);
                    asyncOperator.postProcessElement();
                };
            default:
                throw new UnsupportedOperationException(
                        "Unknown element order for async processing:"
                                + asyncOperator.getElementOrder());
        }
    }
}
