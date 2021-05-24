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

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusProvider;
import org.apache.flink.util.OutputTag;

/**
 * Special version of {@link BroadcastingOutputCollector} that performs a shallow copy of the {@link
 * StreamRecord} to ensure that multi-chaining works correctly.
 */
final class CopyingBroadcastingOutputCollector<T> extends BroadcastingOutputCollector<T> {

    public CopyingBroadcastingOutputCollector(
            Output<StreamRecord<T>>[] outputs, StreamStatusProvider streamStatusProvider) {
        super(outputs, streamStatusProvider);
    }

    @Override
    public void collect(StreamRecord<T> record) {

        for (int i = 0; i < outputs.length - 1; i++) {
            Output<StreamRecord<T>> output = outputs[i];
            StreamRecord<T> shallowCopy = record.copy(record.getValue());
            output.collect(shallowCopy);
        }

        if (outputs.length > 0) {
            // don't copy for the last output
            outputs[outputs.length - 1].collect(record);
        }
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        for (int i = 0; i < outputs.length - 1; i++) {
            Output<StreamRecord<T>> output = outputs[i];

            StreamRecord<X> shallowCopy = record.copy(record.getValue());
            output.collect(outputTag, shallowCopy);
        }

        if (outputs.length > 0) {
            // don't copy for the last output
            outputs[outputs.length - 1].collect(outputTag, record);
        }
    }
}
