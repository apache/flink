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

package org.apache.flink.test.hadoopcompatibility.mapreduce;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Test WordCount with Hadoop input and output "mapreduce" (modern) formats. */
// This test case has been updated from dataset to a datastream.
// It is essentially a batch job, but the HadoopInputFormat is an unbounded source.
// As a result, the test case cannot be set to batch runtime mode and should not run with the
// adaptive scheduler.
@Category(FailsWithAdaptiveScheduler.class)
class WordCountMapreduceITCase extends JavaProgramTestBase {

    private String textPath;
    private String resultPath;

    @BeforeEach
    void checkOperatingSystem() {
        // FLINK-5164 - see https://wiki.apache.org/hadoop/WindowsProblems
        assumeThat(OperatingSystem.isWindows())
                .as("This test can't run successfully on Windows.")
                .isFalse();
    }

    @Override
    protected void preSubmit() throws Exception {
        textPath = createTempFile("text.txt", WordCountData.TEXT);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath, new String[] {".", "_"});
    }

    @Override
    protected JobExecutionResult testProgram() throws Exception {
        JobExecutionResult jobExecutionResult = internalRun();
        postSubmit();
        return jobExecutionResult;
    }

    private JobExecutionResult internalRun() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<LongWritable, Text>> input;
        input =
                env.createInput(
                        HadoopInputs.readHadoopFile(
                                new TextInputFormat(), LongWritable.class, Text.class, textPath));

        DataStream<String> text =
                input.map(
                        new MapFunction<Tuple2<LongWritable, Text>, String>() {
                            @Override
                            public String map(Tuple2<LongWritable, Text> value) throws Exception {
                                return value.f1.toString();
                            }
                        });

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // key by the tuple field "f0" and sum up tuple field "f1"
                        .keyBy(x -> x.f0)
                        .window(GlobalWindows.createWithEndOfStreamTrigger())
                        .sum(1);

        DataStream<Tuple2<Text, LongWritable>> words =
                counts.map(
                        new MapFunction<Tuple2<String, Integer>, Tuple2<Text, LongWritable>>() {

                            @Override
                            public Tuple2<Text, LongWritable> map(Tuple2<String, Integer> value)
                                    throws Exception {
                                return new Tuple2<Text, LongWritable>(
                                        new Text(value.f0), new LongWritable(value.f1));
                            }
                        });

        // Set up Hadoop Output Format
        Job job = Job.getInstance();
        HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
                new HadoopOutputFormat<Text, LongWritable>(
                        new TextOutputFormat<Text, LongWritable>(), job);
        job.getConfiguration().set("mapred.textoutputformat.separator", " ");
        TextOutputFormat.setOutputPath(job, new Path(resultPath));

        // Output & Execute
        words.addSink(new OutputFormatSinkFunction<>(hadoopOutputFormat));
        return env.execute("Hadoop Compat WordCount");
    }

    static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
