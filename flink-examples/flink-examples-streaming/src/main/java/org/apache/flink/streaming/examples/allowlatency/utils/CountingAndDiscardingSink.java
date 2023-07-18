package org.apache.flink.streaming.examples.allowlatency.utils;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CountingAndDiscardingSink<T> extends RichSinkFunction<T> {
    public static final String COUNTER_NAME = "numElements";

    private static final long serialVersionUID = 1L;

    private final LongCounter numElementsCounter = new LongCounter();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(COUNTER_NAME, numElementsCounter);
    }

    @Override
    public void invoke(T value, Context context) {
        numElementsCounter.add(1L);
    }
}
