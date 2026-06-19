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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

/**
 * Special version of {@link BroadcastingOutputCollector} that performs a shallow copy of the {@link
 * StreamRecord} to ensure that multi-chaining works correctly.
 */
final class CopyingBroadcastingOutputCollector<T> extends BroadcastingOutputCollector<T> {

    public CopyingBroadcastingOutputCollector(
            OutputWithChainingCheck<StreamRecord<T>>[] allOutputs, Counter numRecordsOutForTask) {
        super(allOutputs, numRecordsOutForTask);
    }

    @Override
    public void collect(StreamRecord<T> record) {
        boolean emitted = false;
        int length = outputs.length;

        for (int i = 0; i < length - 1; i++) {
            OutputWithChainingCheck<StreamRecord<T>> output = outputs[i];
            StreamRecord<T> shallowCopy = record.copy(record.getValue());
            emitted |= output.collectAndCheckIfChained(shallowCopy);
        }

        if (length > 0) {
            emitted |= outputs[length - 1].collectAndCheckIfChained(record);
        }

        if (emitted) {
            numRecordsOutForTask.inc();
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        boolean emitted = false;
        int length = outputs.length;

        for (int i = 0; i < length - 1; i++) {
            OutputWithChainingCheck<StreamRecord<T>> output = outputs[i];
            StreamRecord<X> shallowCopy = record.copy(record.getValue());
            emitted |= output.collectAndCheckIfChained(outputTag, shallowCopy);
        }

        if (length > 0) {
            emitted |= outputs[length - 1].collectAndCheckIfChained(outputTag, record);
        }

        if (emitted) {
            numRecordsOutForTask.inc();
        }
    }
}
