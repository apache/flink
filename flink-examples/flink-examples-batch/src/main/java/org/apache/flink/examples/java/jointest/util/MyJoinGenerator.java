package org.apache.flink.examples.java.jointest.util;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

/** A bounded source used to generate data stream in batch mode. */
public class MyJoinGenerator implements GeneratorFunction<Long, String> {

    private Random random;
    private long numValues;
    private long initSeed;
    private int numPreGeneratedData;
    private String[] preGeneratedData;

    public MyJoinGenerator(long numValues, long initSeed) {
        this.numValues = numValues;
        this.initSeed = initSeed;
    }

    @Override
    public void open(SourceReaderContext readerContext) throws Exception {
        GeneratorFunction.super.open(readerContext);
        random = new Random(initSeed);
        numPreGeneratedData = (int) numValues;
        preGeneratedData = new String[numPreGeneratedData];
        for (int i = 0; i < numPreGeneratedData; i++) {
            preGeneratedData[i] = Integer.toString(random.nextInt(numPreGeneratedData));
        }
    }

    @Override
    public String map(Long value) throws Exception {
        return preGeneratedData[(int) (value % numPreGeneratedData)];
    }
}
