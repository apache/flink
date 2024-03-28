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

package org.apache.flink.datastream.impl.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.common.OutputCollector;
import org.apache.flink.datastream.impl.common.TimestampCollector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/** */
public class KeyedTwoOutputProcessOperator<KEY, IN, OUT_MAIN, OUT_SIDE>
        extends TwoOutputProcessOperator<IN, OUT_MAIN, OUT_SIDE> {

    @Nullable private final KeySelector<OUT_MAIN, KEY> mainOutKeySelector;

    @Nullable private final KeySelector<OUT_SIDE, KEY> sideOutKeySelector;

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag) {
        this(userFunction, outputTag, null, null);
    }

    public KeyedTwoOutputProcessOperator(
            TwoOutputStreamProcessFunction<IN, OUT_MAIN, OUT_SIDE> userFunction,
            OutputTag<OUT_SIDE> outputTag,
            @Nullable KeySelector<OUT_MAIN, KEY> mainOutKeySelector,
            @Nullable KeySelector<OUT_SIDE, KEY> sideOutKeySelector) {
        super(userFunction, outputTag);
        this.mainOutKeySelector = mainOutKeySelector;
        this.sideOutKeySelector = sideOutKeySelector;
    }

    @Override
    protected TimestampCollector<OUT_MAIN> getMainCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(new OutputCollector<>(output), mainOutKeySelector)
                : new OutputCollector<>(output);
    }

    @Override
    public TimestampCollector<OUT_SIDE> getSideCollector() {
        return mainOutKeySelector != null && sideOutKeySelector != null
                ? new KeyCheckedOutputCollector<>(
                        new SideOutputCollector(output), sideOutKeySelector)
                : new SideOutputCollector(output);
    }

    private class KeyCheckedOutputCollector<T> extends TimestampCollector<T> {

        private final TimestampCollector<T> outputCollector;

        private final KeySelector<T, KEY> outputKeySelector;

        private KeyCheckedOutputCollector(
                TimestampCollector<T> outputCollector, KeySelector<T, KEY> outputKeySelector) {
            this.outputCollector = outputCollector;
            this.outputKeySelector = outputKeySelector;
        }

        @Override
        public void collect(T outputRecord) {
            checkOutputKey(outputRecord);
            this.outputCollector.collect(outputRecord);
        }

        @Override
        public void collectAndOverwriteTimestamp(T outputRecord, long timestamp) {
            checkOutputKey(outputRecord);
            this.outputCollector.collectAndOverwriteTimestamp(outputRecord, timestamp);
        }

        @SuppressWarnings("unchecked")
        private void checkOutputKey(T outputRecord) {
            try {
                KEY currentKey = (KEY) getCurrentKey();
                KEY outputKey = this.outputKeySelector.getKey(outputRecord);
                if (!outputKey.equals(currentKey)) {
                    throw new IllegalStateException(
                            "Output key must equals to input key if you want the produced stream "
                                    + "is keyed. ");
                }
            } catch (Exception e) {
                // TODO Change Consumer to ThrowingConsumer.
                ExceptionUtils.rethrow(e);
            }
        }
    }
}
