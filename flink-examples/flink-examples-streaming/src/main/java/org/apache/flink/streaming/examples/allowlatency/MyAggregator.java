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

package org.apache.flink.streaming.examples.allowlatency;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.PartitionableListState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * An aggregation operator with user-defined flush function.
 *
 * <p>The operator will buffer inputs in memory until a flush operation is triggered.
 */
public class MyAggregator extends AbstractStreamOperator<Tuple2<Integer, Long>>
        implements OneInputStreamOperator<Integer, Tuple2<Integer, Long>> {
    private transient Map<Integer, Long> bundle;
    private final KeySelector<Integer, Integer> keySelector;
    private ValueState<Long> store;
    private PartitionableListState<Tuple2<Integer, Long>> bundleState;
    private long visits = 0;

    public MyAggregator(KeySelector<Integer, Integer> keySelector) {
        super();
        this.keySelector = keySelector;
    }

    private Integer getKey(Integer input) throws Exception {
        return keySelector.getKey(input);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        store =
                context.getKeyedStateStore()
                        .getState(new ValueStateDescriptor<>("store", Long.class));
        this.bundle = new HashMap<>();
        bundleState =
                (PartitionableListState<Tuple2<Integer, Long>>)
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "bundle",
                                                TypeInformation.of(
                                                        new TypeHint<Tuple2<Integer, Long>>() {})));
        if (context.isRestored()) {
            for (Tuple2<Integer, Long> t : bundleState.get()) {
                bundle.put(t.f0, t.f1);
            }
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<Integer> element) throws Exception {
        Integer input = element.getValue();
        if (getExecutionConfig().getMaxFlushInterval() > 0) {
            Long bundleValue = bundle.get(input);
            // get a new value after adding this element to bundle
            Long newBundleValue = bundleValue == null ? 1 : bundleValue + 1;
            bundle.put(input, newBundleValue);
        } else {
            visits += 1;
            Long storeValue = store.value();
            Long newStoreValue = storeValue == null ? 1 : storeValue + 1;
            store.update(newStoreValue);
            output.collect(new StreamRecord<>(new Tuple2<>(input, newStoreValue)));
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        bundleState.clear();
        bundle.forEach(
                (k, v) -> {
                    bundleState.add(new Tuple2<>(k, v));
                });
        LOG.info("Operator state size: {}", bundle.size());
    }

    @Override
    public void finish() throws Exception {
        finishBundle();
        System.out.println("visits: " + visits);
    }

    public void finishBundle() {
        if (bundle != null && !bundle.isEmpty()) {
            finishBundle(bundle, output);
            bundle.clear();
        }
    }

    public void finishBundle(
            Map<Integer, Long> bundle, Output<StreamRecord<Tuple2<Integer, Long>>> output) {
        //        long keyCnt = 0L;
        for (Map.Entry<Integer, Long> entry : bundle.entrySet()) {
            int k = entry.getKey();
            long v = entry.getValue();
            try {
                visits += 1;
                setKeyContextElement1(new StreamRecord<>(k));
                Long storeValue = store.value();
                Long newStoreValue = storeValue == null ? v : storeValue + v;
                store.update(newStoreValue);
                output.collect(new StreamRecord<>(new Tuple2<>(k, newStoreValue)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        //        keyCnt /= (double) bundle.size();
        //        LOG.info("Average key count: " + keyCnt);
    }

    @Override
    public void flush() {
        finishBundle();
    }
}
