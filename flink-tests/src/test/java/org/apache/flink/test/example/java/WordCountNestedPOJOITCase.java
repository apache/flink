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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Date;

/** WordCount with nested POJO example. */
@SuppressWarnings("serial")
public class WordCountNestedPOJOITCase extends JavaProgramTestBase implements Serializable {
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

        DataSet<WC> counts =
                text.flatMap(new Tokenizer())
                        .groupBy("complex.someTest")
                        .reduce(
                                new ReduceFunction<WC>() {
                                    private static final long serialVersionUID = 1L;

                                    public WC reduce(WC value1, WC value2) {
                                        return new WC(
                                                value1.complex.someTest,
                                                value1.count + value2.count);
                                    }
                                });

        counts.writeAsText(resultPath);

        env.execute("WordCount with custom data types example");
    }

    private static final class Tokenizer implements FlatMapFunction<String, WC> {

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

    /** POJO with nested POJO. */
    public static class WC { // is a pojo
        public ComplexNestedClass complex; // is a pojo
        public int count; // is a BasicType

        public WC() {}

        public WC(String t, int c) {
            this.count = c;
            this.complex = new ComplexNestedClass();
            this.complex.word = new Tuple3<Long, Long, String>(0L, 0L, "egal");
            this.complex.date = new Date();
            this.complex.someFloat = 0.0f;
            this.complex.someNumber = 666;
            this.complex.someTest = t;
        }

        @Override
        public String toString() {
            return this.complex.someTest + " " + count;
        }
    }

    /** Nested POJO. */
    public static class ComplexNestedClass { // pojo
        public static int ignoreStaticField;
        public transient int ignoreTransientField;
        public Date date; // generic type
        public Integer someNumber; // BasicType
        public float someFloat; // BasicType
        public Tuple3<Long, Long, String> word; // Tuple Type with three basic types
        public String someTest;
    }
}
