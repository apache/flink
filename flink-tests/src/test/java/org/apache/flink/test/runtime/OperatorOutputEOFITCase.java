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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/** Test that OutPutEOF node has finished before the downstream node has been set up. */
public class OperatorOutputEOFITCase extends TestLogger {
    static CountDownLatch countDownLatch = new CountDownLatch(1);

    @Test
    public void twoInputStreamOperatorOutPutEOFTest() throws Exception {
        /* Block one input stream of a TwoInputStreamOperator */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.disableOperatorChaining();
        DataStream<Integer> source1 = env.fromElements(1);
        DataStream<Integer> source2 = env.fromElements(1);
        ProcessFunction<Integer, Integer> processFunction =
                new ProcessFunction<Integer, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void processElement(
                            Integer value,
                            ProcessFunction<Integer, Integer>.Context ctx,
                            Collector<Integer> out)
                            throws Exception {}

                    @Override
                    public void close() throws Exception {
                        super.close();
                        countDownLatch.countDown();
                    }
                };
        source1.transform(
                        "Process1",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new TestOutputEOFProcessOperator<>(env.clean(processFunction)))
                .connect(source2)
                .process(
                        new CoProcessFunction<Integer, Integer, Integer>() {
                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                // Confirm that CoProcessFunction executed after the
                                // processFunction;
                                Assertions.assertThat(countDownLatch.getCount()).isEqualTo(0L);
                            }

                            @Override
                            public void processElement1(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {}

                            @Override
                            public void processElement2(
                                    Integer value, Context ctx, Collector<Integer> out)
                                    throws Exception {}
                        })
                .addSink(new DiscardingSink<>());
        env.execute();
    }

    private static class TestOutputEOFProcessOperator<IN, OUT> extends ProcessOperator<IN, OUT> {
        public TestOutputEOFProcessOperator(ProcessFunction<IN, OUT> function) {
            super(function);
            chainingStrategy = ChainingStrategy.ALWAYS;
        }

        @Override
        public OperatorAttributes getOperatorAttributes() {
            return new OperatorAttributesBuilder().setOutputOnEOF(true).build();
        }
    }
}
