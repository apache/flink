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

package org.apache.flink.streaming.api.operators;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.junit.Assert;

public class AccumulatorTriggerITCase extends StreamingProgramTestBase {

    private JobExecutionResult result;
    public static final String ACC_NAME = "accName";

    @Override
    protected void testProgram() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple1<Integer>> stream = env.fromCollection(Lists.newArrayList(new Tuple1<Integer>(1),
                                                                                   new Tuple1<Integer>(2),
                                                                                   new Tuple1<Integer>(3)));

        stream.windowAll(GlobalWindows.create())
              .trigger(new AccTrigger())
              .sum(0)
              .print();

        this.result = env.execute("AccumulatorTriggerTest");
    }

    @Override
    protected void postSubmit() throws Exception {
        Assert.assertEquals(result.getAccumulatorResult("accName"), 3);
    }

    public static class AccTrigger extends Trigger<Tuple1<Integer>, GlobalWindow> {


        @Override
        public TriggerResult onElement(final Tuple1 element, final long timestamp, final GlobalWindow window,
                                       final Trigger.TriggerContext ctx) throws Exception {
            Accumulator<Integer, Integer> acc = ctx.getAccumulator(ACC_NAME, new IntCounter());
            acc.add(1);
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(final long time, final GlobalWindow window, final Trigger.TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(final long time, final GlobalWindow window, final Trigger.TriggerContext ctx) throws Exception {
            return null;
        }
    }

}
