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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;

import java.util.concurrent.CompletableFuture;

/**
 * A special source input implementation that immediately emit END_OF_INPUT. It is used for sources
 * that finished on restore.
 */
public class StreamTaskFinishedOnRestoreSourceInput<T> extends StreamTaskSourceInput<T> {

    private boolean emittedEndOfData = false;

    public StreamTaskFinishedOnRestoreSourceInput(
            SourceOperator<T, ?> operator, int inputGateIndex, int inputIndex) {
        super(operator, inputGateIndex, inputIndex);
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        if (emittedEndOfData) {
            return DataInputStatus.END_OF_INPUT;
        } else {
            emittedEndOfData = true;
            return DataInputStatus.END_OF_DATA;
        }
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }
}
