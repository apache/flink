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

package org.apache.flink.streaming.examples.allowlatency.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** A parallel source to generate data stream for aggregation operation. */
public class MyAggregationSource extends RichParallelSourceFunction<Integer> {

    private final int keySize;
    private long numValues;
    private long numValuesOnThisTask;
    private Integer[] preGeneratedData;
    private long pause;

    public MyAggregationSource(long numValues, long pause, int keySize) {
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
