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

package org.apache.flink.datastream.impl.common;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.ExceptionUtils;

import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This output checks whether the current key of the output record and the key extracted with a
 * specific key selector are exactly the same.
 */
public class KeyCheckedOutputCollector<KEY, OUT> extends TimestampCollector<OUT> {
    private final TimestampCollector<OUT> collector;

    private final KeySelector<OUT, KEY> outKeySelector;

    private final Supplier<KEY> currentKeyGetter;

    public KeyCheckedOutputCollector(
            TimestampCollector<OUT> collector,
            KeySelector<OUT, KEY> outKeySelector,
            Supplier<KEY> currentKeyGetter) {
        this.collector = collector;
        this.outKeySelector = outKeySelector;
        this.currentKeyGetter = currentKeyGetter;
    }

    @Override
    public void collect(OUT outputRecord) {
        checkOutputKey(outputRecord);
        collector.collect(outputRecord);
    }

    @Override
    public void collectAndOverwriteTimestamp(OUT outputRecord, long timestamp) {
        checkOutputKey(outputRecord);
        collector.collectAndOverwriteTimestamp(outputRecord, timestamp);
    }

    private void checkOutputKey(OUT outputRecord) {
        try {
            KEY currentKey = currentKeyGetter.get();
            KEY outputKey = checkNotNull(outKeySelector).getKey(outputRecord);
            if (!outputKey.equals(currentKey)) {
                throw new IllegalStateException(
                        "Output key must equals to input key if the output key selector is not null.");
            }
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }
    }
}
