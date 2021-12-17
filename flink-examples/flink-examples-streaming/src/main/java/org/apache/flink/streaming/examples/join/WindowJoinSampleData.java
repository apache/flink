/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.join;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.utils.ThrottledIterator;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/** Sample data for the {@link WindowJoin} example. */
@SuppressWarnings("serial")
public class WindowJoinSampleData {

    static final String[] NAMES = {"tom", "jerry", "alice", "bob", "john", "grace"};
    static final int GRADE_COUNT = 5;
    static final int SALARY_MAX = 10000;

    /** Continuously generates (name, grade). */
    public static class GradeSource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(GRADE_COUNT) + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new GradeSource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        }
    }

    /** Continuously generates (name, salary). */
    public static class SalarySource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(SALARY_MAX) + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new SalarySource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        }
    }
}
