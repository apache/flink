package org.apache.flink.streaming.examples.allowlatency.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** A parallel source to generate data stream for joining operation. */
public class MyJoinSource extends RichParallelSourceFunction<Integer> {

    private final int keySize;
    private long numValues;
    private long numValuesOnThisTask;
    private Integer[] preGeneratedData;
    private long pause;

    public MyJoinSource(long numValues, long pause, int keySize) {
        this.numValues = numValues;
        this.pause = pause;
        this.keySize = keySize > 0 ? keySize : (int) Math.max(numValues / 1000, 100);
    }

    @Override
    public final void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        long div = numValues / numTasks;
        long mod = numValues % numTasks;
        numValuesOnThisTask = mod > taskIdx ? div + 1 : div;
        preGeneratedData = new Integer[keySize];
        for (int i = 0; i < keySize; i++) {
            preGeneratedData[i] = i;
        }
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        long cnt = 0;
        while (cnt < this.numValuesOnThisTask) {
            if (pause > 0 && cnt % pause == 0) {
                TimeUnit.MILLISECONDS.sleep(getRandomSleepInterval());
            }
            ctx.collect(preGeneratedData[(int) (cnt % this.keySize)]);
            cnt += 1;
        }
    }

    private long getRandomSleepInterval() {
        return ThreadLocalRandom.current().nextInt(300, 600);
    }

    @Override
    public void cancel() {}
}
