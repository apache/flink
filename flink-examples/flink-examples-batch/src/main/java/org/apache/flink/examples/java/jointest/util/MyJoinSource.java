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

package org.apache.flink.examples.java.jointest.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/** A parallel source to generate data stream for joining operation. */
public class MyJoinSource extends RichParallelSourceFunction<Integer> {

    private Random random;
    private long numValues;
    private long initSeed;
    private long numValuesOnThisTask;
    private int numPreGeneratedData;
    private Integer[] preGeneratedData;

    public MyJoinSource(long numValues, long initSeed) {
        this.numValues = numValues;
        this.initSeed = initSeed;
    }

    @Override
    public final void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        random = new Random(Tuple2.of(initSeed, taskIdx).hashCode());
        long div = numValues / numTasks;
        long mod = numValues % numTasks;
        numValuesOnThisTask = mod > taskIdx ? div + 1 : div;
        numPreGeneratedData = Math.max((int) numValues / 1000, 100);

        preGeneratedData = new Integer[numPreGeneratedData];
        for (int i = 0; i < numPreGeneratedData; i++) {
            preGeneratedData[i] = random.nextInt(numPreGeneratedData);
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
