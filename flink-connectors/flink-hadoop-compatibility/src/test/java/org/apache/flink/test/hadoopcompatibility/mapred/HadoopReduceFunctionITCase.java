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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.hadoopcompatibility.mapred.HadoopReducerWrappedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;

/** IT cases for the {@link HadoopReducerWrappedFunction}. */
@ExtendWith(ParameterizedTestExtension.class)
class HadoopReduceFunctionITCase extends MultipleProgramsTestBase {

    @TestTemplate
    void testStandardGrouping(@TempDir Path tempFolder) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<IntWritable, Text>> ds =
                HadoopTestData.getKVPairDataStream(env).map(new Mapper1());

        DataStream<Tuple2<IntWritable, IntWritable>> commentCnts =
                ds.keyBy(x -> x.f0)
                        .window(GlobalWindows.createWithEndOfStreamTrigger())
                        .apply(new HadoopReducerWrappedFunction<>(new CommentCntReducer()));

        String resultPath = tempFolder.toUri().toString();

        commentCnts.sinkTo(
                FileSink.forRowFormat(
                                new org.apache.flink.core.fs.Path(resultPath),
                                new SimpleStringEncoder<Tuple2<IntWritable, IntWritable>>())
                        .build());
        env.execute();

        String expected = "(0,0)\n" + "(1,3)\n" + "(2,5)\n" + "(3,5)\n" + "(4,2)\n";

        compareResultsByLinesInMemory(expected, resultPath);
    }

    @TestTemplate
    void testUngroupedHadoopReducer(@TempDir Path tempFolder) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Tuple2<IntWritable, Text>> ds = HadoopTestData.getKVPairDataStream(env);

        SingleOutputStreamOperator<Tuple2<IntWritable, IntWritable>> commentCnts =
                ds.windowAll(GlobalWindows.createWithEndOfStreamTrigger())
                        .apply(new HadoopReducerWrappedFunction<>(new AllCommentCntReducer()));

        String resultPath = tempFolder.toUri().toString();

        commentCnts.sinkTo(
                FileSink.forRowFormat(
                                new org.apache.flink.core.fs.Path(resultPath),
                                new SimpleStringEncoder<Tuple2<IntWritable, IntWritable>>())
                        .build());
        env.execute();

        String expected = "(42,15)\n";

        compareResultsByLinesInMemory(expected, resultPath);
    }

    @TestTemplate
    void testConfigurationViaJobConf(@TempDir Path tempFolder) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        JobConf conf = new JobConf();
        conf.set("my.cntPrefix", "Hello");

        DataStream<Tuple2<IntWritable, Text>> ds =
                HadoopTestData.getKVPairDataStream(env).map(new Mapper2());

        DataStream<Tuple2<IntWritable, IntWritable>> helloCnts =
                ds.keyBy(x -> x.f0)
                        .window(GlobalWindows.createWithEndOfStreamTrigger())
                        .apply(
                                new HadoopReducerWrappedFunction<>(
                                        new ConfigurableCntReducer(), conf));

        String resultPath = tempFolder.toUri().toString();

        helloCnts.sinkTo(
                FileSink.forRowFormat(
                                new org.apache.flink.core.fs.Path(resultPath),
                                new SimpleStringEncoder<Tuple2<IntWritable, IntWritable>>())
                        .build());
        env.execute();

        String expected = "(0,0)\n" + "(1,0)\n" + "(2,1)\n" + "(3,1)\n" + "(4,1)\n";

        compareResultsByLinesInMemory(expected, resultPath);
    }

    /** A {@link Reducer} to sum counts. */
    public static class CommentCntReducer
            implements Reducer<IntWritable, Text, IntWritable, IntWritable> {

        @Override
        public void reduce(
                IntWritable k,
                Iterator<Text> vs,
                OutputCollector<IntWritable, IntWritable> out,
                Reporter r)
                throws IOException {
            int commentCnt = 0;
            while (vs.hasNext()) {
                String v = vs.next().toString();
                if (v.startsWith("Comment")) {
                    commentCnt++;
                }
            }
            out.collect(k, new IntWritable(commentCnt));
        }

        @Override
        public void configure(final JobConf arg0) {}

        @Override
        public void close() throws IOException {}
    }

    /** A {@link Reducer} to sum counts. */
    public static class AllCommentCntReducer
            implements Reducer<IntWritable, Text, IntWritable, IntWritable> {

        @Override
        public void reduce(
                IntWritable k,
                Iterator<Text> vs,
                OutputCollector<IntWritable, IntWritable> out,
                Reporter r)
                throws IOException {
            int commentCnt = 0;
            while (vs.hasNext()) {
                String v = vs.next().toString();
                if (v.startsWith("Comment")) {
                    commentCnt++;
                }
            }
            out.collect(new IntWritable(42), new IntWritable(commentCnt));
        }

        @Override
        public void configure(final JobConf arg0) {}

        @Override
        public void close() throws IOException {}
    }

    /** A {@link Reducer} to sum counts for a specific prefix. */
    public static class ConfigurableCntReducer
            implements Reducer<IntWritable, Text, IntWritable, IntWritable> {
        private String countPrefix;

        @Override
        public void reduce(
                IntWritable k,
                Iterator<Text> vs,
                OutputCollector<IntWritable, IntWritable> out,
                Reporter r)
                throws IOException {
            int commentCnt = 0;
            while (vs.hasNext()) {
                String v = vs.next().toString();
                if (v.startsWith(this.countPrefix)) {
                    commentCnt++;
                }
            }
            out.collect(k, new IntWritable(commentCnt));
        }

        @Override
        public void configure(final JobConf c) {
            this.countPrefix = c.get("my.cntPrefix");
        }

        @Override
        public void close() throws IOException {}
    }

    /** Test mapper. */
    public static class Mapper1
            implements MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, Text>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<IntWritable, Text> map(Tuple2<IntWritable, Text> v) throws Exception {
            v.f0 = new IntWritable(v.f0.get() / 5);
            return v;
        }
    }

    /** Test mapper. */
    public static class Mapper2
            implements MapFunction<Tuple2<IntWritable, Text>, Tuple2<IntWritable, Text>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<IntWritable, Text> map(Tuple2<IntWritable, Text> v) throws Exception {
            v.f0 = new IntWritable(v.f0.get() % 5);
            return v;
        }
    }
}
