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

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Random;

/**
 * Tests a self-join, which leads to a deadlock with large data sizes and PIPELINED-only execution.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-1141">FLINK-1141</a>
 */
public class SelfJoinDeadlockITCase extends JavaProgramTestBase {

    protected String resultPath;

    @Rule public Timeout globalTimeout = new Timeout(120 * 1000); // Set timeout for deadlocks

    @Override
    protected void preSubmit() throws Exception {
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Integer, String>> ds =
                env.createInput(new LargeJoinDataGeneratorInputFormat(1000000));

        ds.join(ds).where(0).equalTo(1).with(new Joiner()).writeAsText(resultPath);

        env.execute("Local Selfjoin Test Job");
    }

    @SuppressWarnings("serial")
    private static class Joiner
            implements FlatJoinFunction<
                    Tuple3<Integer, Integer, String>,
                    Tuple3<Integer, Integer, String>,
                    Tuple5<Integer, Integer, Integer, String, String>> {

        @Override
        public void join(
                Tuple3<Integer, Integer, String> in1,
                Tuple3<Integer, Integer, String> in2,
                Collector<Tuple5<Integer, Integer, Integer, String, String>> out)
                throws Exception {
            out.collect(
                    new Tuple5<Integer, Integer, Integer, String, String>(
                            in1.f0, in1.f1, in2.f1, in1.f2, in2.f2));
        }
    }

    // ------------------------------------------------------------------------

    // Use custom input format to generate the data. Other available input formats (like collection
    // input format) create data upfront and serialize it completely on the heap, which might
    // break the test JVM heap sizes.
    private static class LargeJoinDataGeneratorInputFormat
            extends GenericInputFormat<Tuple3<Integer, Integer, String>>
            implements NonParallelInput {

        private static final long serialVersionUID = 1L;

        private final Random rand = new Random(42);

        private final int toProduce;

        private int produced;

        public LargeJoinDataGeneratorInputFormat(int toProduce) {
            this.toProduce = toProduce;
        }

        @Override
        public boolean reachedEnd() throws IOException {
            return produced >= toProduce;
        }

        @Override
        public Tuple3<Integer, Integer, String> nextRecord(Tuple3<Integer, Integer, String> reuse)
                throws IOException {
            produced++;

            return new Tuple3<Integer, Integer, String>(
                    rand.nextInt(toProduce), rand.nextInt(toProduce), "aaa");
        }
    }
}
