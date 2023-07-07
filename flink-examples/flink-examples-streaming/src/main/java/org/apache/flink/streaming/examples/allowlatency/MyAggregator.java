package org.apache.flink.streaming.examples.allowlatency;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MyAggregator extends AbstractStreamOperator<Tuple2<Integer, Long>>
        implements BundleTriggerCallback, OneInputStreamOperator<Integer, Tuple2<Integer, Long>> {
    private transient Map<Integer, Long> bundle;
    private final KeySelector<Integer, Integer> keySelector;
    private final CountBundleTrigger bundleTrigger;
    private int numOfElements;
    private MapState<Integer, Long> kvStore;
    private long latencyVisits;
    private long countVisits;

    public MyAggregator(KeySelector<Integer, Integer> keySelector) {
        super();
        this.keySelector = keySelector;
        this.bundleTrigger = new CountBundleTrigger(1000000);
        this.bundleTrigger.registerCallback(this);
    }

    private Integer getKey(Integer input) throws Exception {
        return keySelector.getKey(input);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        TypeInformation<Integer> type = IntegerTypeInfo.INT_TYPE_INFO;

        kvStore = context.getKeyedStateStore().getMapState(new MapStateDescriptor<Integer, Long>(
                "KV-Store",
                IntegerTypeInfo.INT_TYPE_INFO,
                IntegerTypeInfo.LONG_TYPE_INFO));
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.numOfElements = 0;
        this.bundle = new HashMap<>();
        bundleTrigger.registerCallback(this);
        // reset trigger
        bundleTrigger.reset();
        latencyVisits = 0;
        countVisits = 0;

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
    public void processElement(StreamRecord<Integer> element) throws Exception {
        Integer input = element.getValue();
        System.out.println(input);
        System.out.println("bundle: ");
        bundle.forEach((k, v) -> {
            System.out.println(k + " " + v);
        });
        System.out.println();
        if (input == -1) {
            finishBundle(true);
        } else {
            Integer bundleKey = getKey(input);
            Long bundleValue = bundle.get(bundleKey);
            // get a new value after adding this element to bundle
            Long newBundleValue = bundleValue == null ? 1 : bundleValue + 1;
            bundle.put(bundleKey, newBundleValue);
            bundleTrigger.onElement(input);
        }
    }

    @Override
    public void finish() throws Exception {
        System.out.println("RocksDB visits due to exceeding latency: " + latencyVisits);
        System.out.println("RocksDB visits due to exceeding bundle capacity: " + countVisits);
        System.out.println("Total rocksDB visits: " + (latencyVisits + countVisits));
    }

    public void finishBundle(boolean exceedLatency) {
        if (bundle != null && !bundle.isEmpty()) {
            numOfElements = 0;
            finishBundle(bundle, output, exceedLatency);
            bundle.clear();
        }
        bundleTrigger.reset();
    }

    public void finishBundle(
            Map<Integer, Long> bundle,
            Output<StreamRecord<Tuple2<Integer, Long>>> output,
            boolean exceedLatency) {
        bundle.forEach((k, v) -> {
            try {
                if (exceedLatency) {
                    latencyVisits += 1;
                } else {
                    countVisits += 1;
                }
                Long storeValue = kvStore.get(k);
                Long newStoreValue = storeValue == null ? v : storeValue + v;
                kvStore.put(k, newStoreValue);
//                System.out.println("visiting rocksdb");
                output.collect(new StreamRecord<>(new Tuple2<>(k, newStoreValue)));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static class CountBundleTrigger implements Serializable {
        private static final long serialVersionUID = -3640028071558094813L;
        private final long maxCount;
        private transient long count = 0;
        private transient BundleTriggerCallback callback;

        public CountBundleTrigger(long maxCount) {
            Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
            this.maxCount = maxCount;
        }

        public void onElement(Integer element) {
            count++;
            if (count >= maxCount) {
                callback.finishBundle(false);
                reset();
            }
        }

        public void registerCallback(BundleTriggerCallback callback) {
            this.callback = Preconditions.checkNotNull(callback, "callback is null");
        }

        public void reset() {
            count = 0;
        }

        public String explain() {
            return "CountBundleTrigger with size " + maxCount;
        }

    }
}

