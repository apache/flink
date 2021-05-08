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

package org.apache.flink.test.broadcastvars;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.List;

/** Test broadcast input after branching. */
public class BroadcastBranchingITCase extends JavaProgramTestBase {
    private static final String RESULT = "(2,112)\n";

    //              Sc1(id,a,b,c) --
    //                              \
    //    Sc2(id,x) --------         Jn2(id) -- Mp2 -- Sk
    //                      \        /          / <=BC
    //                       Jn1(id) -- Mp1 ----
    //                      /
    //    Sc3(id,y) --------
    @Override
    protected void testProgram() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Sc1 generates M parameters a,b,c for second degree polynomials P(x) = ax^2 + bx + c
        // identified by id
        DataSet<Tuple4<String, Integer, Integer, Integer>> sc1 =
                env.fromElements(
                        new Tuple4<>("1", 61, 6, 29),
                        new Tuple4<>("2", 7, 13, 10),
                        new Tuple4<>("3", 8, 13, 27));

        // Sc2 generates N x values to be evaluated with the polynomial identified by id
        DataSet<Tuple2<String, Integer>> sc2 =
                env.fromElements(new Tuple2<>("1", 5), new Tuple2<>("2", 3), new Tuple2<>("3", 6));

        // Sc3 generates N y values to be evaluated with the polynomial identified by id
        DataSet<Tuple2<String, Integer>> sc3 =
                env.fromElements(new Tuple2<>("1", 2), new Tuple2<>("2", 3), new Tuple2<>("3", 7));

        // Jn1 matches x and y values on id and emits (id, x, y) triples
        JoinOperator<
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Integer, Integer>>
                jn1 = sc2.join(sc3).where(0).equalTo(0).with(new Jn1());

        // Jn2 matches polynomial and arguments by id, computes p = min(P(x),P(y)) and emits (id, p)
        // tuples
        JoinOperator<
                        Tuple3<String, Integer, Integer>,
                        Tuple4<String, Integer, Integer, Integer>,
                        Tuple2<String, Integer>>
                jn2 = jn1.join(sc1).where(0).equalTo(0).with(new Jn2());

        // Mp1 selects (id, x, y) triples where x = y and broadcasts z (=x=y) to Mp2
        FlatMapOperator<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> mp1 =
                jn1.flatMap(new Mp1());

        // Mp2 filters out all p values which can be divided by z
        List<Tuple2<String, Integer>> result =
                jn2.flatMap(new Mp2()).withBroadcastSet(mp1, "z").collect();

        JavaProgramTestBase.compareResultAsText(result, RESULT);
    }

    private static class Jn1
            implements JoinFunction<
                    Tuple2<String, Integer>,
                    Tuple2<String, Integer>,
                    Tuple3<String, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple3<String, Integer, Integer> join(
                Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
            return new Tuple3<>(first.f0, first.f1, second.f1);
        }
    }

    private static class Jn2
            implements JoinFunction<
                    Tuple3<String, Integer, Integer>,
                    Tuple4<String, Integer, Integer, Integer>,
                    Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private static int p(int x, int a, int b, int c) {
            return a * x * x + b * x + c;
        }

        @Override
        public Tuple2<String, Integer> join(
                Tuple3<String, Integer, Integer> first,
                Tuple4<String, Integer, Integer, Integer> second)
                throws Exception {
            int x = first.f1;
            int y = first.f2;
            int a = second.f1;
            int b = second.f2;
            int c = second.f3;

            int pX = p(x, a, b, c);
            int pY = p(y, a, b, c);
            int min = Math.min(pX, pY);
            return new Tuple2<>(first.f0, min);
        }
    }

    private static class Mp1
            implements FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(
                Tuple3<String, Integer, Integer> value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            if (value.f1.compareTo(value.f2) == 0) {
                out.collect(new Tuple2<>(value.f0, value.f1));
            }
        }
    }

    private static class Mp2
            extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private Collection<Tuple2<String, Integer>> zs;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.zs = getRuntimeContext().getBroadcastVariable("z");
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            int p = value.f1;

            for (Tuple2<String, Integer> z : zs) {
                if (z.f0.equals(value.f0)) {
                    if (p % z.f1 != 0) {
                        out.collect(value);
                    }
                }
            }
        }
    }
}
