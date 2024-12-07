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
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;

/** Sample data for the {@link WindowJoin} example. */
@SuppressWarnings("serial")
public class WindowJoinSampleData {

    static final String[] NAMES = {"tom", "jerry", "alice", "bob", "john", "grace"};
    static final int GRADE_COUNT = 5;
    static final int SALARY_MAX = 10000;

    /** Continuously generates (name, grade). */
    public static DataGeneratorSource<Tuple2<String, Integer>> getGradeGeneratorSource(
            double elementsPerSecond) {
        return getTupleGeneratorSource(GRADE_COUNT, elementsPerSecond);
    }

    /** Continuously generates (name, salary). */
    public static DataGeneratorSource<Tuple2<String, Integer>> getSalaryGeneratorSource(
            double elementsPerSecond) {
        return getTupleGeneratorSource(SALARY_MAX, elementsPerSecond);
    }

    private static DataGeneratorSource<Tuple2<String, Integer>> getTupleGeneratorSource(
            int maxValue, double elementsPerSecond) {
        final Random rnd = new Random();
        final GeneratorFunction<Long, Tuple2<String, Integer>> generatorFunction =
                index -> new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(maxValue) + 1);

        return new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(elementsPerSecond),
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
    }
}
