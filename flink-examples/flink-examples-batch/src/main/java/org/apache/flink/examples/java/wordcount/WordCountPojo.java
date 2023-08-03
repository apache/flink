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

package org.apache.flink.examples.java.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.examples.java.util.DataSetDeprecationInfo.DATASET_DEPRECATION_INFO;

/**
 * This example shows an implementation of WordCount without using the Tuple2 type, but a custom
 * class.
 *
 * <p>Note: All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future
 * Flink major version. You can still build your application in DataSet, but you should move to
 * either the DataStream and/or Table API. This class is retained for testing purposes.
 */
@SuppressWarnings("serial")
public class WordCountPojo {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountPojo.class);

    /**
     * This is the POJO (Plain Old Java Object) that is being used for all the operations. As long
     * as all fields are public or have a getter/setter, the system can handle them
     */
    public static class Word {

        // fields
        private String word;
        private int frequency;

        // constructors
        public Word() {}

        public Word(String word, int i) {
            this.word = word;
            this.frequency = i;
        }

        // getters setters
        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getFrequency() {
            return frequency;
        }

        public void setFrequency(int frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "Word=" + word + " freq=" + frequency;
        }
    }

    public static void main(String[] args) throws Exception {

        LOGGER.warn(DATASET_DEPRECATION_INFO);

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            // get default test text data
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        DataSet<Word> counts =
                // split up the lines into Word objects (with frequency = 1)
                text.flatMap(new Tokenizer())
                        // group by the field word and sum up the frequency
                        .groupBy("word")
                        .reduce(
                                new ReduceFunction<Word>() {
                                    @Override
                                    public Word reduce(Word value1, Word value2) throws Exception {
                                        return new Word(
                                                value1.word, value1.frequency + value2.frequency);
                                    }
                                });

        if (params.has("output")) {
            counts.writeAsText(params.get("output"), WriteMode.OVERWRITE);
            // execute program
            env.execute("WordCount-Pojo Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple Word objects.
     */
    public static final class Tokenizer implements FlatMapFunction<String, Word> {

        @Override
        public void flatMap(String value, Collector<Word> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Word(token, 1));
                }
            }
        }
    }
}
