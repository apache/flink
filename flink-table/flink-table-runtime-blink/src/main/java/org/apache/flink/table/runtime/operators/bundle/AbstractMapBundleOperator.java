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

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.context.ExecutionContextImpl;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTriggerCallback;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link AbstractMapBundleOperator} simply used a java Map to store the input elements in
 * key-value form. The map key is typically the same with the state key, so we can do some
 * optimizations before accessing states, like pre aggregate values for each key. And we will only
 * need to access state every key we have, but not every element we processed.
 *
 * <p>NOTES: if all elements we processed have different keys, such operator will only increase
 * memory footprint, and will not have any performance improvement.
 *
 * @param <K> The type of the key in the bundle map
 * @param <V> The type of the value in the bundle map
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
public abstract class AbstractMapBundleOperator<K, V, IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BundleTriggerCallback {

    private static final long serialVersionUID = 5081841938324118594L;

    /** The map in heap to store elements. */
    private transient Map<K, V> bundle;

    /** The trigger that determines how many elements should be put into a bundle. */
    private final BundleTrigger<IN> bundleTrigger;

    /** The function used to process when receiving element. */
    private final MapBundleFunction<K, V, IN, OUT> function;

    /** Output for stream records. */
    private transient Collector<OUT> collector;

    private transient int numOfElements = 0;

    AbstractMapBundleOperator(
            MapBundleFunction<K, V, IN, OUT> function, BundleTrigger<IN> bundleTrigger) {
        chainingStrategy = ChainingStrategy.ALWAYS;
        this.function = checkNotNull(function, "function is null");
        this.bundleTrigger = checkNotNull(bundleTrigger, "bundleTrigger is null");
    }

    @Override
    public void open() throws Exception {
        super.open();
        function.open(new ExecutionContextImpl(this, getRuntimeContext()));

        this.numOfElements = 0;
        this.collector = new StreamRecordCollector<>(output);
        this.bundle = new HashMap<>();

        bundleTrigger.registerCallback(this);
        // reset trigger
        bundleTrigger.reset();
        LOG.info("BundleOperator's trigger info: " + bundleTrigger.explain());

        // counter metric to get the size of bundle
        getRuntimeContext()
                .getMetricGroup()
                .gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
        getRuntimeContext()
                .getMetricGroup()
                .gauge(
                        "bundleRatio",
                        (Gauge<Double>)
                                () -> {
                                    int numOfKeys = bundle.size();
                                    if (numOfKeys == 0) {
                                        return 0.0;
                                    } else {
                                        return 1.0 * numOfElements / numOfKeys;
                                    }
                                });
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // get the key and value for the map bundle
        final IN input = element.getValue();
        final K bundleKey = getKey(input);
        final V bundleValue = bundle.get(bundleKey);

        // get a new value after adding this element to bundle
        final V newBundleValue = function.addInput(bundleValue, input);

        // update to map bundle
        bundle.put(bundleKey, newBundleValue);

        numOfElements++;
        bundleTrigger.onElement(input);
    }

    /** Get the key for current processing element, which will be used as the map bundle's key. */
    protected abstract K getKey(final IN input) throws Exception;

    @Override
    public void finishBundle() throws Exception {
        if (bundle != null && !bundle.isEmpty()) {
            numOfElements = 0;
            function.finishBundle(bundle, collector);
            bundle.clear();
        }
        bundleTrigger.reset();
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        finishBundle();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        finishBundle();
    }

    @Override
    public void close() throws Exception {
        try {
            finishBundle();
        } finally {
            Exception exception = null;

            try {
                super.close();
                if (function != null) {
                    FunctionUtils.closeFunction(function);
                }
            } catch (InterruptedException interrupted) {
                exception = interrupted;

                Thread.currentThread().interrupt();
            } catch (Exception e) {
                exception = e;
            }

            if (exception != null) {
                LOG.warn("Errors occurred while closing the BundleOperator.", exception);
            }
        }
    }
}
