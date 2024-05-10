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

package org.apache.flink.streaming.runtime.operators.asyncprocessing.declare;

import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.ThrowingConsumer;

/**
 * Operator with two inputs that can declare the processing of each input by a predefined function.
 */
public interface DeclarativeProcessingTwoInputOperator<IN1, IN2, OUT>
        extends TwoInputStreamOperator<IN1, IN2, OUT> {

    /**
     * A hook for declaring the process in {@code processElement1}. If subclass wants to define its
     * {@code processElement1} in declarative way, it should implement this class. If so, the {@code
     * processElement1} will not take effect and instead, the return value of this method will
     * become the processing logic. This method will be called after {@code open()} of operator.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @return the whole processing logic just like {@code processElement}.
     */
    ThrowingConsumer<StreamRecord<IN1>, Exception> declareProcess1(DeclarationContext context)
            throws DeclarationException;

    /**
     * A hook for declaring the process in {@code processElement2}. If subclass wants to define its
     * {@code processElement2} in declarative way, it should implement this class. If so, the {@code
     * processElement2} will not take effect and instead, the return value of this method will
     * become the processing logic. This method will be called after {@code open()} of operator.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @return the whole processing logic just like {@code processElement}.
     */
    ThrowingConsumer<StreamRecord<IN2>, Exception> declareProcess2(DeclarationContext context)
            throws DeclarationException;
}
