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

package org.apache.flink.table.runtime.operators.window.triggers;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.table.runtime.operators.window.Window;

/**
 * A {@link Trigger} that fires at some point after a specified number of input elements have
 * arrived.
 */
public class ElementTriggers {

    /** This class should never be instantiated. */
    private ElementTriggers() {}

    /** Creates a new trigger that triggers on receiving of every element. */
    public static <W extends Window> EveryElement<W> every() {
        return new EveryElement<>();
    }

    /** Creates a trigger that fires when the pane contains at lease {@code countElems} elements. */
    public static <W extends Window> CountElement<W> count(long countElems) {
        return new CountElement<>(countElems);
    }

    /**
     * A {@link Trigger} that triggers on every element.
     *
     * @param <W> type of window
     */
    public static final class EveryElement<W extends Window> extends Trigger<W> {

        private static final long serialVersionUID = 3942805366646141029L;

        @Override
        public void open(TriggerContext ctx) throws Exception {
            // do nothing
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            // trigger on every record
            return true;
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public void clear(W window) throws Exception {
            // do nothing
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            // do nothing
        }

        @Override
        public String toString() {
            return "Element.every()";
        }
    }

    /**
     * A {@link Trigger} that fires at some point after a specified number of input elements have
     * arrived.
     */
    public static final class CountElement<W extends Window> extends Trigger<W> {

        private static final long serialVersionUID = -3823782971498746808L;

        private final long countElems;
        private final ReducingStateDescriptor<Long> countStateDesc;
        private transient TriggerContext ctx;

        CountElement(long countElems) {
            this.countElems = countElems;
            this.countStateDesc =
                    new ReducingStateDescriptor<>(
                            "trigger-count-" + countElems, new Sum(), LongSerializer.INSTANCE);
        }

        @Override
        public void open(TriggerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        public boolean onElement(Object element, long timestamp, W window) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
            count.add(1L);
            if (count.get() >= countElems) {
                count.clear();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public boolean onProcessingTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public boolean onEventTime(long time, W window) throws Exception {
            return false;
        }

        @Override
        public void clear(W window) throws Exception {
            ctx.getPartitionedState(countStateDesc).clear();
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext mergeContext) throws Exception {
            mergeContext.mergePartitionedState(countStateDesc);
        }

        @Override
        public String toString() {
            return "Element.count(" + countElems + ")";
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 1L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }
}
