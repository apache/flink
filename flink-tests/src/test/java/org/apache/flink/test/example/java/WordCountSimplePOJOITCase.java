/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.java;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.legacy.io.TextInputFormat;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBaseJUnit4;
import org.apache.flink.util.Collector;

import java.io.Serializable;

import static org.apache.flink.test.util.TestBaseUtils.compareResultsByLinesInMemory;

/** WordCount with simple POJO example. */
public class WordCountSimplePOJOITCase extends JavaProgramTestBaseJUnit4 implements Serializable {
    private static final long serialVersionUID = 1L;
    protected String textPath;
    protected String resultPath;

    @Override
    protected void preSubmit() throws Exception {
        textPath = createTempFile("text.txt", WordCountData.TEXT);
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(WordCountData.COUNTS, resultPath);
    }

    @Override
    protected void testProgram() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<String> text = env.createInput(new TextInputFormat(new Path(textPath)));

        DataStream<WC> counts =
                text.flatMap(new Tokenizer())
                        .keyBy(x -> x.word)
                        .window(GlobalWindows.createWithEndOfStreamTrigger())
                        .reduce(
                                new ReduceFunction<WC>() {
                                    private static final long serialVersionUID = 1L;

                                    public WC reduce(WC value1, WC value2) {
                                        return new WC(value1.word, value1.count + value2.count);
                                    }
                                });

        counts.sinkTo(
                FileSink.forRowFormat(new Path(resultPath), new SimpleStringEncoder<WC>()).build());

        env.execute("WordCount with custom data types example");
    }

    private static final class Tokenizer implements FlatMapFunction<String, WC> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<WC> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new WC(token, 1));
                }
            }
        }
    }

    /** POJO with word and count. */
    public static class WC {
        public WC() {}

        public WC(String w, int c) {
            word = w;
            count = c;
        }

        public String word;
        public int count;

        @Override
        public String toString() {
            return word + " " + count;
        }
    }
}
