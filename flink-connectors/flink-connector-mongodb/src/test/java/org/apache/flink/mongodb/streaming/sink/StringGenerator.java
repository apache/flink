package org.apache.flink.mongodb.streaming.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.util.Random;

public class StringGenerator implements DataGenerator<String> {

    private int count;

    @Override
    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public String next() {
        return "key" + count++ + "," + new Random().nextInt();
    }
}
