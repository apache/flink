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

package org.apache.flink.test.accumulators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

import org.junit.Assert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Test for the basic functionality of accumulators. We cannot test all different kinds of plans
 * here (iterative, etc.).
 *
 * <p>TODO Test conflict when different UDFs write to accumulator with same name but with different
 * type. The conflict will occur in JobManager while merging.
 */
@SuppressWarnings("serial")
public class AccumulatorITCase extends JavaProgramTestBase {

    private static final String INPUT = "one\n" + "two two\n" + "three three three\n";
    private static final String EXPECTED = "one 1\ntwo 2\nthree 3\n";

    private String dataPath;
    private String resultPath;

    private JobExecutionResult result;

    @Override
    protected void preSubmit() throws Exception {
        dataPath = createTempFile("datapoints.txt", INPUT);
        resultPath = getTempFilePath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(EXPECTED, resultPath);

        // Test accumulator results
        System.out.println("Accumulator results:");
        JobExecutionResult res = this.result;
        System.out.println(AccumulatorHelper.getResultsFormatted(res.getAllAccumulatorResults()));

        Assert.assertEquals(Integer.valueOf(3), res.getAccumulatorResult("num-lines"));
        Assert.assertEquals(Integer.valueOf(3), res.getIntCounterResult("num-lines"));

        Assert.assertEquals(
                Double.valueOf(getParallelism()), res.getAccumulatorResult("open-close-counter"));

        // Test histogram (words per line distribution)
        Map<Integer, Integer> dist = new HashMap<>();
        dist.put(1, 1);
        dist.put(2, 1);
        dist.put(3, 1);
        Assert.assertEquals(dist, res.getAccumulatorResult("words-per-line"));

        // Test distinct words (custom accumulator)
        Set<StringValue> distinctWords = new HashSet<>();
        distinctWords.add(new StringValue("one"));
        distinctWords.add(new StringValue("two"));
        distinctWords.add(new StringValue("three"));
        Assert.assertEquals(distinctWords, res.getAccumulatorResult("distinct-words"));
    }

    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> input = env.readTextFile(dataPath);

        input.flatMap(new TokenizeLine())
                .groupBy(0)
                .reduceGroup(new CountWords())
                .writeAsCsv(resultPath, "\n", " ");

        this.result = env.execute();
    }

    private static class TokenizeLine extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

        // Needs to be instantiated later since the runtime context is not yet
        // initialized at this place
        private IntCounter cntNumLines;
        private Histogram wordsPerLineDistribution;

        // This counter will be added without convenience functions
        private DoubleCounter openCloseCounter = new DoubleCounter();
        private SetAccumulator<StringValue> distinctWords;

        @Override
        public void open(Configuration parameters) {

            // Add counters using convenience functions
            this.cntNumLines = getRuntimeContext().getIntCounter("num-lines");
            this.wordsPerLineDistribution = getRuntimeContext().getHistogram("words-per-line");

            // Add built-in accumulator without convenience function
            getRuntimeContext().addAccumulator("open-close-counter", this.openCloseCounter);

            // Add custom counter
            this.distinctWords = new SetAccumulator<>();
            this.getRuntimeContext().addAccumulator("distinct-words", distinctWords);

            // Create counter and test increment
            IntCounter simpleCounter = getRuntimeContext().getIntCounter("simple-counter");
            simpleCounter.add(1);
            Assert.assertEquals(simpleCounter.getLocalValue().intValue(), 1);

            // Test if we get the same counter
            IntCounter simpleCounter2 = getRuntimeContext().getIntCounter("simple-counter");
            Assert.assertEquals(simpleCounter.getLocalValue(), simpleCounter2.getLocalValue());

            // Should fail if we request it with different type
            try {
                @SuppressWarnings("unused")
                DoubleCounter simpleCounter3 =
                        getRuntimeContext().getDoubleCounter("simple-counter");
                // DoubleSumAggregator longAggregator3 = (DoubleSumAggregator)
                // getRuntimeContext().getAggregator("custom",
                // DoubleSumAggregator.class);
                Assert.fail(
                        "Should not be able to obtain previously created counter with different type");
            } catch (UnsupportedOperationException ex) {
                // expected!
            }

            // Test counter used in open() and closed()
            this.openCloseCounter.add(0.5);
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            this.cntNumLines.add(1);
            int wordsPerLine = 0;

            for (String token : value.toLowerCase().split("\\W+")) {
                distinctWords.add(new StringValue(token));
                out.collect(new Tuple2<>(token, 1));
                ++wordsPerLine;
            }
            wordsPerLineDistribution.add(wordsPerLine);
        }

        @Override
        public void close() throws Exception {
            // Test counter used in open and close only
            this.openCloseCounter.add(0.5);
            Assert.assertEquals(1, this.openCloseCounter.getLocalValue().intValue());
        }
    }

    private static class CountWords
            extends RichGroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>
            implements GroupCombineFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private IntCounter reduceCalls;
        private IntCounter combineCalls;

        @Override
        public void open(Configuration parameters) {
            this.reduceCalls = getRuntimeContext().getIntCounter("reduce-calls");
            this.combineCalls = getRuntimeContext().getIntCounter("combine-calls");
        }

        @Override
        public void reduce(
                Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
            reduceCalls.add(1);
            reduceInternal(values, out);
        }

        @Override
        public void combine(
                Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
            combineCalls.add(1);
            reduceInternal(values, out);
        }

        private void reduceInternal(
                Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) {
            int sum = 0;
            String key = null;

            for (Tuple2<String, Integer> e : values) {
                key = e.f0;
                sum += e.f1;
            }
            out.collect(new Tuple2<>(key, sum));
        }
    }

    /** Custom accumulator. */
    public static class SetAccumulator<T> implements Accumulator<T, HashSet<T>> {

        private static final long serialVersionUID = 1L;

        private HashSet<T> set = new HashSet<>();

        @Override
        public void add(T value) {
            this.set.add(value);
        }

        @Override
        public HashSet<T> getLocalValue() {
            return this.set;
        }

        @Override
        public void resetLocal() {
            this.set.clear();
        }

        @Override
        public void merge(Accumulator<T, HashSet<T>> other) {
            // build union
            this.set.addAll(other.getLocalValue());
        }

        @Override
        public Accumulator<T, HashSet<T>> clone() {
            SetAccumulator<T> result = new SetAccumulator<>();
            result.set.addAll(set);
            return result;
        }
    }
}
