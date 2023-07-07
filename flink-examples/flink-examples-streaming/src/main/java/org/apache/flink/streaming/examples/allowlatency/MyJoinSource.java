package org.apache.flink.streaming.examples.allowlatency;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** A parallel source to generate data stream for joining operation. */
public class MyJoinSource extends RichParallelSourceFunction<Integer> {

    private long numValues;
    private long numValuesOnThisTask;
    private int numPreGeneratedData;
    private Integer[] preGeneratedData;

    public MyJoinSource(long numValues) {
        this.numValues = numValues;
    }

    @Override
    public final void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        long div = numValues / numTasks;
        long mod = numValues % numTasks;
        numValuesOnThisTask = mod > taskIdx ? div + 1 : div;
        numPreGeneratedData = (int) Math.min(Math.max(numValues / 1000, 10000), 100000000);
        preGeneratedData = new Integer[numPreGeneratedData];
        for (int i = 0; i < numPreGeneratedData; i++) {
            preGeneratedData[i] = i;
        }
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        long cnt = 0;
        while (cnt < this.numValuesOnThisTask) {
            ctx.collect(preGeneratedData[(int) (cnt % this.numPreGeneratedData)]);
            cnt += 1;
        }
    }

    @Override
    public void cancel() {}
}
