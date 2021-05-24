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

package org.apache.flink.streaming.test;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.iteration.util.IterateExampleData;
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;
import org.apache.flink.streaming.examples.windowing.util.SessionWindowingData;
import org.apache.flink.streaming.test.examples.join.WindowJoinData;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;

/** Integration test for streaming programs in Java examples. */
public class StreamingExamplesITCase extends AbstractTestBase {

    @Test
    public void testIterateExample() throws Exception {
        final String inputPath =
                createTempFile("fibonacciInput.txt", IterateExampleData.INPUT_PAIRS);
        final String resultPath = getTempDirPath("result");

        // the example is inherently non-deterministic. The iteration timeout of 5000 ms
        // is frequently not enough to make the test run stable on CI infrastructure
        // with very small containers, so we cannot do a validation here
        org.apache.flink.streaming.examples.iteration.IterateExample.main(
                new String[] {
                    "--input", inputPath,
                    "--output", resultPath
                });
    }

    @Test
    public void testWindowJoin() throws Exception {

        final String resultPath = File.createTempFile("result-path", "dir").toURI().toString();

        final class Parser implements MapFunction<String, Tuple2<String, Integer>> {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[1], Integer.parseInt(fields[2]));
            }
        }

        try {
            final StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<Tuple2<String, Integer>> grades =
                    env.fromElements(WindowJoinData.GRADES_INPUT.split("\n"))
                            .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create())
                            .map(new Parser());

            DataStream<Tuple2<String, Integer>> salaries =
                    env.fromElements(WindowJoinData.SALARIES_INPUT.split("\n"))
                            .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create())
                            .map(new Parser());

            org.apache.flink.streaming.examples.join.WindowJoin.runWindowJoin(grades, salaries, 100)
                    .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);

            env.execute();

            // since the two sides of the join might have different speed
            // the exact output can not be checked just whether it is well-formed
            // checks that the result lines look like e.g. (bob, 2, 2015)
            checkLinesAgainstRegexp(resultPath, "^\\([a-z]+,(\\d),(\\d)+\\)");
        } finally {
            try {
                FileUtils.deleteDirectory(new File(resultPath));
            } catch (Throwable ignored) {
            }
        }
    }

    @Test
    public void testTwitterStream() throws Exception {
        final String resultPath = getTempDirPath("result");
        org.apache.flink.streaming.examples.twitter.TwitterExample.main(
                new String[] {"--output", resultPath});
        compareResultsByLinesInMemory(TwitterExampleData.STREAMING_COUNTS_AS_TUPLES, resultPath);
    }

    @Test
    public void testSessionWindowing() throws Exception {
        final String resultPath = getTempDirPath("result");
        org.apache.flink.streaming.examples.windowing.SessionWindowing.main(
                new String[] {"--output", resultPath});
        compareResultsByLinesInMemory(SessionWindowingData.EXPECTED, resultPath);
    }

    @Test
    public void testWindowWordCount() throws Exception {
        final String windowSize = "250";
        final String slideSize = "150";
        final String textPath = createTempFile("text.txt", WordCountData.TEXT);
        final String resultPath = getTempDirPath("result");

        org.apache.flink.streaming.examples.windowing.WindowWordCount.main(
                new String[] {
                    "--input", textPath,
                    "--output", resultPath,
                    "--window", windowSize,
                    "--slide", slideSize
                });

        // since the parallel tokenizers might have different speed
        // the exact output can not be checked just whether it is well-formed
        // checks that the result lines look like e.g. (faust, 2)
        checkLinesAgainstRegexp(resultPath, "^\\([a-z]+,(\\d)+\\)");
    }

    @Test
    public void testWordCount() throws Exception {
        final String textPath = createTempFile("text.txt", WordCountData.TEXT);
        final String resultPath = getTempDirPath("result");

        org.apache.flink.streaming.examples.wordcount.WordCount.main(
                new String[] {
                    "--input", textPath,
                    "--output", resultPath
                });

        compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES, resultPath);
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
