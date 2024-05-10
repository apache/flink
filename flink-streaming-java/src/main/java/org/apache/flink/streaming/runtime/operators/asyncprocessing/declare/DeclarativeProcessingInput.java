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

import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.function.ThrowingConsumer;

/** Input that can declare the processing by a predefined function. */
public interface DeclarativeProcessingInput<IN> extends Input<IN> {

    /**
     * A hook for declaring the process in {@code processElement}. If subclass wants to define its
     * {@code processElement} in declarative way, it should implement this class. If so, the {@code
     * processElement} will not take effect and instead, the return value of this method will become
     * the processing logic. This method will be called after {@code open()} of operator.
     *
     * @param context the context that provides useful methods to define named callbacks.
     * @return the whole processing logic just like {@code processElement}.
     */
    ThrowingConsumer<StreamRecord<IN>, Exception> declareProcess(DeclarationContext context)
            throws DeclarationException;
}
