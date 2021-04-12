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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** Integration tests for {@link DataSetUtils#sample}. */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class SampleITCase extends MultipleProgramsTestBase {

    private static final Random RNG = new Random();

    public SampleITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Before
    public void initiate() {
        ExecutionEnvironment.getExecutionEnvironment().setParallelism(5);
    }

    @Test
    public void testSamplerWithFractionWithoutReplacement() throws Exception {
        verifySamplerWithFractionWithoutReplacement(0d);
        verifySamplerWithFractionWithoutReplacement(0.2d);
        verifySamplerWithFractionWithoutReplacement(1.0d);
    }

    @Test
    public void testSamplerWithFractionWithReplacement() throws Exception {
        verifySamplerWithFractionWithReplacement(0d);
        verifySamplerWithFractionWithReplacement(0.2d);
        verifySamplerWithFractionWithReplacement(1.0d);
        verifySamplerWithFractionWithReplacement(2.0d);
    }

    @Test
    public void testSamplerWithSizeWithoutReplacement() throws Exception {
        verifySamplerWithFixedSizeWithoutReplacement(0);
        verifySamplerWithFixedSizeWithoutReplacement(2);
        verifySamplerWithFixedSizeWithoutReplacement(21);
    }

    @Test
    public void testSamplerWithSizeWithReplacement() throws Exception {
        verifySamplerWithFixedSizeWithReplacement(0);
        verifySamplerWithFixedSizeWithReplacement(2);
        verifySamplerWithFixedSizeWithReplacement(21);
    }

    private void verifySamplerWithFractionWithoutReplacement(double fraction) throws Exception {
        verifySamplerWithFractionWithoutReplacement(fraction, RNG.nextLong());
    }

    private void verifySamplerWithFractionWithoutReplacement(double fraction, long seed)
            throws Exception {
        verifySamplerWithFraction(false, fraction, seed);
    }

    private void verifySamplerWithFractionWithReplacement(double fraction) throws Exception {
        verifySamplerWithFractionWithReplacement(fraction, RNG.nextLong());
    }

    private void verifySamplerWithFractionWithReplacement(double fraction, long seed)
            throws Exception {
        verifySamplerWithFraction(true, fraction, seed);
    }

    private void verifySamplerWithFraction(boolean withReplacement, double fraction, long seed)
            throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        FlatMapOperator<Tuple3<Integer, Long, String>, String> ds = getSourceDataSet(env);
        MapPartitionOperator<String, String> sampled =
                DataSetUtils.sample(ds, withReplacement, fraction, seed);
        List<String> result = sampled.collect();
        containsResultAsText(result, getSourceStrings());
    }

    private void verifySamplerWithFixedSizeWithoutReplacement(int numSamples) throws Exception {
        verifySamplerWithFixedSizeWithoutReplacement(numSamples, RNG.nextLong());
    }

    private void verifySamplerWithFixedSizeWithoutReplacement(int numSamples, long seed)
            throws Exception {
        verifySamplerWithFixedSize(false, numSamples, seed);
    }

    private void verifySamplerWithFixedSizeWithReplacement(int numSamples) throws Exception {
        verifySamplerWithFixedSizeWithReplacement(numSamples, RNG.nextLong());
    }

    private void verifySamplerWithFixedSizeWithReplacement(int numSamples, long seed)
            throws Exception {
        verifySamplerWithFixedSize(true, numSamples, seed);
    }

    private void verifySamplerWithFixedSize(boolean withReplacement, int numSamples, long seed)
            throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        FlatMapOperator<Tuple3<Integer, Long, String>, String> ds = getSourceDataSet(env);
        DataSet<String> sampled =
                DataSetUtils.sampleWithSize(ds, withReplacement, numSamples, seed);
        List<String> result = sampled.collect();
        assertEquals(numSamples, result.size());
        containsResultAsText(result, getSourceStrings());
    }

    private FlatMapOperator<Tuple3<Integer, Long, String>, String> getSourceDataSet(
            ExecutionEnvironment env) {
        return CollectionDataSets.get3TupleDataSet(env)
                .flatMap(
                        new FlatMapFunction<Tuple3<Integer, Long, String>, String>() {
                            @Override
                            public void flatMap(
                                    Tuple3<Integer, Long, String> value, Collector<String> out)
                                    throws Exception {
                                out.collect(value.f2);
                            }
                        });
    }

    private String getSourceStrings() {
        return "Hi\n"
                + "Hello\n"
                + "Hello world\n"
                + "Hello world, how are you?\n"
                + "I am fine.\n"
                + "Luke Skywalker\n"
                + "Comment#1\n"
                + "Comment#2\n"
                + "Comment#3\n"
                + "Comment#4\n"
                + "Comment#5\n"
                + "Comment#6\n"
                + "Comment#7\n"
                + "Comment#8\n"
                + "Comment#9\n"
                + "Comment#10\n"
                + "Comment#11\n"
                + "Comment#12\n"
                + "Comment#13\n"
                + "Comment#14\n"
                + "Comment#15\n";
    }
}
