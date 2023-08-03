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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/** A parallel source to generate data stream for joining operation. */
public class MyJoinSource extends RichParallelSourceFunction<Integer> {
    private long numValues;
    private long numValuesOnThisTask;
    private int numPreGeneratedData;
    private long pause;

    public MyJoinSource(long numValues, long pause) {
        this.numValues = numValues;
        this.pause = pause;
    }

    @Override
    public final void open(Configuration parameters) throws Exception {
        super.open(parameters);
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int numTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        long div = numValues / numTasks;
        long mod = numValues % numTasks;
        numValuesOnThisTask = mod > taskIdx ? div + 1 : div;
        numPreGeneratedData = (int) Math.min(Math.max(numValues / 50, 10), 100000000);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        long cnt = 0;
        while (cnt < this.numValuesOnThisTask) {
            ctx.collect((int) (cnt % this.numPreGeneratedData));
            cnt += 1;
        }
    }

    @Override
    public void cancel() {}
}
