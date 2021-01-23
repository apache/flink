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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;

/** Test join with a slow source. */
public class ConsumePipelinedAndBlockingResultITCase extends JavaProgramTestBase {

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple1<Long>> pipelinedSource = env.fromElements(new Tuple1<Long>(1L));

        DataSet<Tuple1<Long>> slowBlockingSource =
                env.generateSequence(0, 10)
                        .map(
                                new MapFunction<Long, Tuple1<Long>>() {
                                    @Override
                                    public Tuple1<Long> map(Long value) throws Exception {
                                        Thread.sleep(200);

                                        return new Tuple1<Long>(value);
                                    }
                                });

        slowBlockingSource
                .join(slowBlockingSource)
                .where(0)
                .equalTo(0)
                .output(new DiscardingOutputFormat<Tuple2<Tuple1<Long>, Tuple1<Long>>>());

        // Join the slow blocking and the pipelined source. This test should verify that this works
        // w/o problems and the blocking result is not requested too early.
        pipelinedSource
                .join(slowBlockingSource)
                .where(0)
                .equalTo(0)
                .output(new DiscardingOutputFormat<Tuple2<Tuple1<Long>, Tuple1<Long>>>());

        env.execute("Consume one pipelined and one blocking result test job");
    }

    @Override
    protected boolean skipCollectionExecution() {
        // Skip collection execution as it is independent of the runtime environment functionality,
        // which is under test.
        return true;
    }
}
