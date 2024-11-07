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

package org.apache.flink.test.hadoopcompatibility.mapred;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Integration tests for Hadoop IO formats. */
@ExtendWith(ParameterizedTestExtension.class)
// This test case has been updated from dataset to a datastream.
// It is essentially a batch job, but the HadoopInputFormat is an unbounded source.
// As a result, the test case cannot be set to batch runtime mode and should not run with the
// adaptive scheduler.
@Category(FailsWithAdaptiveScheduler.class)
public class HadoopIOFormatsITCase extends JavaProgramTestBase {

    private static final int NUM_PROGRAMS = 2;

    @Parameter private int curProgId;
    private String[] resultPath;
    private String[] expectedResult;
    private String sequenceFileInPath;
    private String sequenceFileInPathNull;

    @BeforeEach
    void checkOperatingSystem() {
        // FLINK-5164 - see https://wiki.apache.org/hadoop/WindowsProblems
        assumeThat(OperatingSystem.isWindows())
                .as("This test can't run successfully on Windows.")
                .isFalse();
    }

    @Override
    @TestTemplate
    public void testJobWithObjectReuse() throws Exception {
        super.testJobWithoutObjectReuse();
    }

    @Override
    @TestTemplate
    public void testJobWithoutObjectReuse() throws Exception {
        super.testJobWithoutObjectReuse();
    }

    @Override
    protected void preSubmit() throws Exception {
        resultPath = new String[] {getTempDirPath("result0"), getTempDirPath("result1")};

        File sequenceFile = createAndRegisterTempFile("seqFile");
        sequenceFileInPath = sequenceFile.toURI().toString();

        // Create a sequence file
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        FileSystem fs = FileSystem.get(URI.create(sequenceFile.getAbsolutePath()), conf);
        Path path = new Path(sequenceFile.getAbsolutePath());

        //  ------------------ Long / Text Key Value pair: ------------
        int kvCount = 4;

        LongWritable key = new LongWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
            for (int i = 0; i < kvCount; i++) {
                if (i == 1) {
                    // write key = 0 a bit more often.
                    for (int a = 0; a < 15; a++) {
                        key.set(i);
                        value.set(i + " - somestring");
                        writer.append(key, value);
                    }
                }
                key.set(i);
                value.set(i + " - somestring");
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeStream(writer);
        }

        //  ------------------ Long / Text Key Value pair: ------------

        File sequenceFileNull = createAndRegisterTempFile("seqFileNullKey");
        sequenceFileInPathNull = sequenceFileNull.toURI().toString();
        path = new Path(sequenceFileInPathNull);

        LongWritable value1 = new LongWritable();
        SequenceFile.Writer writer1 = null;
        try {
            writer1 =
                    SequenceFile.createWriter(
                            fs, conf, path, NullWritable.class, value1.getClass());
            for (int i = 0; i < kvCount; i++) {
                value1.set(i);
                writer1.append(NullWritable.get(), value1);
            }
        } finally {
            IOUtils.closeStream(writer1);
        }
    }

    @Override
    protected JobExecutionResult testProgram() throws Exception {
        Tuple2<String[], JobExecutionResult> expectedResultAndJobExecutionResult =
                HadoopIOFormatPrograms.runProgram(
                        curProgId, resultPath, sequenceFileInPath, sequenceFileInPathNull);
        expectedResult = expectedResultAndJobExecutionResult.f0;
        return expectedResultAndJobExecutionResult.f1;
    }

    @Override
    protected void postSubmit() throws Exception {
        for (int i = 0; i < resultPath.length; i++) {
            compareResultsByLinesInMemory(expectedResult[i], resultPath[i]);
        }
    }

    @Parameters(name = "curProgId = {0}")
    public static Collection<Integer> getConfigurations() {

        Collection<Integer> programIds = new ArrayList<>(NUM_PROGRAMS);

        for (int i = 1; i <= NUM_PROGRAMS; i++) {
            programIds.add(i);
        }

        return programIds;
    }

    private static class HadoopIOFormatPrograms {

        public static Tuple2<String[], JobExecutionResult> runProgram(
                int progId,
                String[] resultPath,
                String sequenceFileInPath,
                String sequenceFileInPathNull)
                throws Exception {

            switch (progId) {
                case 1:
                    {
                        /** Test sequence file, including a key access. */
                        final StreamExecutionEnvironment env =
                                StreamExecutionEnvironment.getExecutionEnvironment();

                        SequenceFileInputFormat<LongWritable, Text> sfif =
                                new SequenceFileInputFormat<LongWritable, Text>();
                        JobConf hdconf = new JobConf();
                        SequenceFileInputFormat.addInputPath(hdconf, new Path(sequenceFileInPath));
                        HadoopInputFormat<LongWritable, Text> hif =
                                new HadoopInputFormat<LongWritable, Text>(
                                        sfif, LongWritable.class, Text.class, hdconf);
                        DataStream<Tuple2<LongWritable, Text>> ds = env.createInput(hif);

                        ds.map(
                                        new MapFunction<
                                                Tuple2<LongWritable, Text>, Tuple2<Long, Text>>() {
                                            @Override
                                            public Tuple2<Long, Text> map(
                                                    Tuple2<LongWritable, Text> value)
                                                    throws Exception {
                                                return new Tuple2<Long, Text>(
                                                        value.f0.get(), value.f1);
                                            }
                                        })
                                .print();

                        DataStream<Tuple2<Long, Text>> sumed =
                                ds.map(
                                                new MapFunction<
                                                        Tuple2<LongWritable, Text>,
                                                        Tuple2<Long, Text>>() {
                                                    @Override
                                                    public Tuple2<Long, Text> map(
                                                            Tuple2<LongWritable, Text> value)
                                                            throws Exception {
                                                        return new Tuple2<Long, Text>(
                                                                value.f0.get(), value.f1);
                                                    }
                                                })
                                        .windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                                        .reduce(
                                                new ReduceFunction<Tuple2<Long, Text>>() {

                                                    @Override
                                                    public Tuple2<Long, Text> reduce(
                                                            Tuple2<Long, Text> value1,
                                                            Tuple2<Long, Text> value2) {
                                                        return Tuple2.of(
                                                                value1.f0 + value2.f0, value2.f1);
                                                    }
                                                });
                        sumed.sinkTo(
                                FileSink.forRowFormat(
                                                new org.apache.flink.core.fs.Path(resultPath[0]),
                                                new SimpleStringEncoder<Tuple2<Long, Text>>())
                                        .build());

                        DataStream<String> res =
                                ds.keyBy(x -> x.f0)
                                        .window(GlobalWindows.createWithEndOfStreamTrigger())
                                        .reduce(
                                                new ReduceFunction<Tuple2<LongWritable, Text>>() {
                                                    @Override
                                                    public Tuple2<LongWritable, Text> reduce(
                                                            Tuple2<LongWritable, Text> value1,
                                                            Tuple2<LongWritable, Text> value2)
                                                            throws Exception {
                                                        return value1;
                                                    }
                                                })
                                        .map(
                                                new MapFunction<
                                                        Tuple2<LongWritable, Text>, String>() {
                                                    @Override
                                                    public String map(
                                                            Tuple2<LongWritable, Text> value)
                                                            throws Exception {
                                                        return value.f1 + " - " + value.f0.get();
                                                    }
                                                });
                        res.sinkTo(
                                FileSink.forRowFormat(
                                                new org.apache.flink.core.fs.Path(resultPath[1]),
                                                new SimpleStringEncoder<String>())
                                        .build());
                        JobExecutionResult jobExecutionResult = env.execute();

                        // return expected result
                        return Tuple2.of(
                                new String[] {
                                    "(21,3 - somestring)",
                                    "0 - somestring - 0\n"
                                            + "1 - somestring - 1\n"
                                            + "2 - somestring - 2\n"
                                            + "3 - somestring - 3\n"
                                },
                                jobExecutionResult);
                    }
                case 2:
                    {
                        final StreamExecutionEnvironment env =
                                StreamExecutionEnvironment.getExecutionEnvironment();

                        SequenceFileInputFormat<NullWritable, LongWritable> sfif =
                                new SequenceFileInputFormat<NullWritable, LongWritable>();
                        JobConf hdconf = new JobConf();
                        SequenceFileInputFormat.addInputPath(
                                hdconf, new Path(sequenceFileInPathNull));
                        HadoopInputFormat<NullWritable, LongWritable> hif =
                                new HadoopInputFormat<NullWritable, LongWritable>(
                                        sfif, NullWritable.class, LongWritable.class, hdconf);
                        DataStream<Tuple2<NullWritable, LongWritable>> ds = env.createInput(hif);
                        DataStream<Tuple2<Void, Long>> res =
                                ds.map(
                                        new MapFunction<
                                                Tuple2<NullWritable, LongWritable>,
                                                Tuple2<Void, Long>>() {
                                            @Override
                                            public Tuple2<Void, Long> map(
                                                    Tuple2<NullWritable, LongWritable> value)
                                                    throws Exception {
                                                return new Tuple2<Void, Long>(null, value.f1.get());
                                            }
                                        });
                        DataStream<Tuple2<Void, Long>> res1 = res.keyBy(x -> x.f1).sum(1);

                        res1.sinkTo(
                                FileSink.forRowFormat(
                                                new org.apache.flink.core.fs.Path(resultPath[1]),
                                                new SimpleStringEncoder<Tuple2<Void, Long>>())
                                        .build());

                        res.sinkTo(
                                FileSink.forRowFormat(
                                                new org.apache.flink.core.fs.Path(resultPath[0]),
                                                new SimpleStringEncoder<Tuple2<Void, Long>>())
                                        .build());
                        JobExecutionResult jobExecutionResult = env.execute();

                        // return expected result
                        return Tuple2.of(
                                new String[] {
                                    "(null,2)\n" + "(null,0)\n" + "(null,1)\n" + "(null,3)",
                                    "(null,0)\n" + "(null,1)\n" + "(null,2)\n" + "(null,3)"
                                },
                                jobExecutionResult);
                    }
                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }
}
