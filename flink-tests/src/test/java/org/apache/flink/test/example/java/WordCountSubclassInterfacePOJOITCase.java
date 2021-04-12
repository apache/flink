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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/** WordCount with subclass and interface example. */
@SuppressWarnings("serial")
public class WordCountSubclassInterfacePOJOITCase extends JavaProgramTestBase
        implements Serializable {
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
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(textPath);

        DataSet<WCBase> counts =
                text.flatMap(new Tokenizer())
                        .groupBy("word")
                        .reduce(
                                new ReduceFunction<WCBase>() {
                                    private static final long serialVersionUID = 1L;

                                    public WCBase reduce(WCBase value1, WCBase value2) {
                                        WC wc1 = (WC) value1;
                                        WC wc2 = (WC) value2;
                                        int c =
                                                wc1.secretCount.getCount()
                                                        + wc2.secretCount.getCount();
                                        wc1.secretCount.setCount(c);
                                        return wc1;
                                    }
                                })
                        .map(
                                new MapFunction<WCBase, WCBase>() {
                                    @Override
                                    public WCBase map(WCBase value) throws Exception {
                                        WC wc = (WC) value;
                                        wc.count = wc.secretCount.getCount();
                                        return wc;
                                    }
                                });

        counts.writeAsText(resultPath);

        env.execute("WordCount with custom data types example");
    }

    private static final class Tokenizer implements FlatMapFunction<String, WCBase> {

        @Override
        public void flatMap(String value, Collector<WCBase> out) {
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

    /** Abstract POJO. */
    public abstract static class WCBase {
        public String word;
        public int count;

        public WCBase(String w, int c) {
            this.word = w;
            this.count = c;
        }

        @Override
        public String toString() {
            return word + " " + count;
        }
    }

    /** POJO interface. */
    public interface CrazyCounter {
        int getCount();

        void setCount(int c);
    }

    /** Implementation of POJO interface. */
    public static class CrazyCounterImpl implements CrazyCounter {
        public int countz;

        public CrazyCounterImpl() {}

        public CrazyCounterImpl(int c) {
            this.countz = c;
        }

        @Override
        public int getCount() {
            return countz;
        }

        @Override
        public void setCount(int c) {
            this.countz = c;
        }
    }

    /** Subclass of abstract POJO. */
    public static class WC extends WCBase {
        public CrazyCounter secretCount;

        public WC() {
            super(null, 0);
        }

        public WC(String w, int c) {
            super(w, 0);
            this.secretCount = new CrazyCounterImpl(c);
        }
    }
}
