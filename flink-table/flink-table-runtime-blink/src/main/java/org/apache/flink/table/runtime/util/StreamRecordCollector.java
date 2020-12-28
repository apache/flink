/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Wrapper around an {@link Output} for wrap {@code T} to {@link StreamRecord}.
 *
 * @param <T> The type of the elements that can be emitted.
 */
@Internal
public class StreamRecordCollector<T> implements Collector<T> {

    private final StreamRecord<T> element = new StreamRecord<>(null);

    private final Output<StreamRecord<T>> underlyingOutput;

    public StreamRecordCollector(Output<StreamRecord<T>> output) {
        this.underlyingOutput = output;
    }

    @Override
    public void collect(T record) {
        underlyingOutput.collect(element.replace(record));
    }

    @Override
    public void close() {
        underlyingOutput.close();
    }
}
