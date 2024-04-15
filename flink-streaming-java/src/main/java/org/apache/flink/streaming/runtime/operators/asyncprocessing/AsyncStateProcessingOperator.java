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
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * A more detailed interface based on {@link AsyncStateProcessing}, which gives the essential
 * methods for an operator to perform async state processing.
 */
@Internal
public interface AsyncStateProcessingOperator extends AsyncStateProcessing {

    /** Get the {@link ElementOrder} of this operator. */
    ElementOrder getElementOrder();

    /**
     * Set key context for async state processing.
     *
     * @param record the record.
     * @param keySelector the key selector to select a key from record.
     * @param <T> the type of the record.
     */
    <T> void setAsyncKeyedContextElement(StreamRecord<T> record, KeySelector<T, ?> keySelector)
            throws Exception;

    /** A callback that will be triggered after an element finishes {@code processElement}. */
    void postProcessElement();

    /**
     * Check the order of same-key record, and then process the record. Mainly used when the {@link
     * #getElementOrder()} returns {@link ElementOrder#RECORD_ORDER}.
     *
     * @param processing the record processing logic.
     */
    void preserveRecordOrderAndProcess(ThrowingRunnable<Exception> processing);
}
