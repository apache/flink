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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;
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
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;

/** Integration tests for Hadoop IO formats. */
@RunWith(Parameterized.class)
public class HadoopIOFormatsITCase extends JavaProgramTestBase {

    private static final int NUM_PROGRAMS = 2;

    private final int curProgId;
    private String[] resultPath;
    private String[] expectedResult;
    private String sequenceFileInPath;
    private String sequenceFileInPathNull;

    public HadoopIOFormatsITCase(int curProgId) {
        this.curProgId = curProgId;
    }

    @Before
    public void checkOperatingSystem() {
        // FLINK-5164 - see https://wiki.apache.org/hadoop/WindowsProblems
        Assume.assumeTrue(
                "This test can't run successfully on Windows.", !OperatingSystem.isWindows());
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
    protected void testProgram() throws Exception {
        expectedResult =
                HadoopIOFormatPrograms.runProgram(
                        curProgId, resultPath, sequenceFileInPath, sequenceFileInPathNull);
    }

    @Override
    protected void postSubmit() throws Exception {
        for (int i = 0; i < resultPath.length; i++) {
            compareResultsByLinesInMemory(expectedResult[i], resultPath[i]);
        }
    }

    @Parameters
    public static Collection<Object[]> getConfigurations() {

        Collection<Object[]> programIds = new ArrayList<>(NUM_PROGRAMS);

        for (int i = 1; i <= NUM_PROGRAMS; i++) {
            programIds.add(new Object[] {i});
        }

        return programIds;
    }

    private static class HadoopIOFormatPrograms {

        public static String[] runProgram(
                int progId,
                String[] resultPath,
                String sequenceFileInPath,
                String sequenceFileInPathNull)
                throws Exception {

            switch (progId) {
                case 1:
                    {
                        /** Test sequence file, including a key access. */
                        final ExecutionEnvironment env =
                                ExecutionEnvironment.getExecutionEnvironment();

                        SequenceFileInputFormat<LongWritable, Text> sfif =
                                new SequenceFileInputFormat<LongWritable, Text>();
                        JobConf hdconf = new JobConf();
                        SequenceFileInputFormat.addInputPath(hdconf, new Path(sequenceFileInPath));
                        HadoopInputFormat<LongWritable, Text> hif =
                                new HadoopInputFormat<LongWritable, Text>(
                                        sfif, LongWritable.class, Text.class, hdconf);
                        DataSet<Tuple2<LongWritable, Text>> ds = env.createInput(hif);
                        DataSet<Tuple2<Long, Text>> sumed =
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
                                        .sum(0);
                        sumed.writeAsText(resultPath[0]);
                        DataSet<String> res =
                                ds.distinct(0)
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
                        res.writeAsText(resultPath[1]);
                        env.execute();

                        // return expected result
                        return new String[] {
                            "(21,3 - somestring)",
                            "0 - somestring - 0\n"
                                    + "1 - somestring - 1\n"
                                    + "2 - somestring - 2\n"
                                    + "3 - somestring - 3\n"
                        };
                    }
                case 2:
                    {
                        final ExecutionEnvironment env =
                                ExecutionEnvironment.getExecutionEnvironment();

                        SequenceFileInputFormat<NullWritable, LongWritable> sfif =
                                new SequenceFileInputFormat<NullWritable, LongWritable>();
                        JobConf hdconf = new JobConf();
                        SequenceFileInputFormat.addInputPath(
                                hdconf, new Path(sequenceFileInPathNull));
                        HadoopInputFormat<NullWritable, LongWritable> hif =
                                new HadoopInputFormat<NullWritable, LongWritable>(
                                        sfif, NullWritable.class, LongWritable.class, hdconf);
                        DataSet<Tuple2<NullWritable, LongWritable>> ds = env.createInput(hif);
                        DataSet<Tuple2<Void, Long>> res =
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
                        DataSet<Tuple2<Void, Long>> res1 = res.groupBy(1).sum(1);
                        res1.writeAsText(resultPath[1]);
                        res.writeAsText(resultPath[0]);
                        env.execute();

                        // return expected result
                        return new String[] {
                            "(null,2)\n" + "(null,0)\n" + "(null,1)\n" + "(null,3)",
                            "(null,0)\n" + "(null,1)\n" + "(null,2)\n" + "(null,3)"
                        };
                    }
                default:
                    throw new IllegalArgumentException("Invalid program id");
            }
        }
    }
}
