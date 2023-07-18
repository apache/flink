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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/** Test class used by the {@link org.apache.flink.test.classloading.ClassLoaderITCase}. */
@SuppressWarnings("serial")
public class StreamingProgram {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.fromElements(WordCountData.TEXT).rebalance();

        DataStream<Word> counts = text.flatMap(new Tokenizer()).keyBy("word").sum("frequency");

        counts.sinkTo(new DiscardingSink<>());

        env.execute();
    }
    // --------------------------------------------------------------------------------------------

    /** POJO with word and count. */
    public static class Word {

        private String word;
        private Integer frequency;

        public Word() {}

        public Word(String word, int i) {
            this.word = word;
            this.frequency = i;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getFrequency() {
            return frequency;
        }

        public void setFrequency(Integer frequency) {
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "(" + word + ", " + frequency + ")";
        }
    }

    private static class Tokenizer implements FlatMapFunction<String, Word> {
        @Override
        public void flatMap(String value, Collector<Word> out) throws Exception {
            StringTokenizer tokenizer = new StringTokenizer(value);
            while (tokenizer.hasMoreTokens()) {
                out.collect(new Word(tokenizer.nextToken(), 1));
            }
        }
    }
}
