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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.IntValue;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.is;

/**
 * These programs demonstrate the effects of user defined functions which modify input objects or
 * return locally created objects that are retained and reused on future calls. The programs do not
 * retain and later modify input objects.
 */
public class OverwriteObjects {

    public static final Logger LOG = LoggerFactory.getLogger(OverwriteObjects.class);

    // DataSets are created with this number of elements
    private static final int NUMBER_OF_ELEMENTS = 3_000_000;

    // DataSet values are randomly generated over this range
    private static final int KEY_RANGE = 1_000_000;

    private static final int MAX_PARALLELISM = 4;

    private static final long RANDOM_SEED = new Random().nextLong();

    private static final Tuple2Comparator<IntValue, IntValue> comparator = new Tuple2Comparator<>();

    public static void main(String[] args) throws Exception {
        new OverwriteObjects().run();
    }

    public void run() throws Exception {
        LOG.info("Random seed = {}", RANDOM_SEED);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        for (int parallelism = MAX_PARALLELISM; parallelism > 0; parallelism--) {
            LOG.info("Parallelism = {}", parallelism);

            env.setParallelism(parallelism);

            testReduce(env);
            testGroupedReduce(env);
            testJoin(env);
            testCross(env);
        }
    }

    // --------------------------------------------------------------------------------------------

    public void testReduce(ExecutionEnvironment env) throws Exception {
        /*
         * Test ChainedAllReduceDriver
         */

        LOG.info("Testing reduce");

        env.getConfig().enableObjectReuse();

        Tuple2<IntValue, IntValue> enabledResult =
                getDataSet(env).reduce(new OverwriteObjectsReduce(false)).collect().get(0);

        env.getConfig().disableObjectReuse();

        Tuple2<IntValue, IntValue> disabledResult =
                getDataSet(env).reduce(new OverwriteObjectsReduce(false)).collect().get(0);

        Assert.assertEquals(NUMBER_OF_ELEMENTS, enabledResult.f1.getValue());
        Assert.assertEquals(NUMBER_OF_ELEMENTS, disabledResult.f1.getValue());

        Assert.assertEquals(disabledResult, enabledResult);
    }

    public void testGroupedReduce(ExecutionEnvironment env) throws Exception {
        /*
         * Test ReduceCombineDriver and ReduceDriver
         */

        LOG.info("Testing grouped reduce");

        env.getConfig().enableObjectReuse();

        List<Tuple2<IntValue, IntValue>> enabledResult =
                getDataSet(env).groupBy(0).reduce(new OverwriteObjectsReduce(true)).collect();

        Collections.sort(enabledResult, comparator);

        env.getConfig().disableObjectReuse();

        List<Tuple2<IntValue, IntValue>> disabledResult =
                getDataSet(env).groupBy(0).reduce(new OverwriteObjectsReduce(true)).collect();

        Collections.sort(disabledResult, comparator);

        Assert.assertThat(disabledResult, is(enabledResult));
    }

    private class OverwriteObjectsReduce implements ReduceFunction<Tuple2<IntValue, IntValue>> {
        private Scrambler scrambler;

        public OverwriteObjectsReduce(boolean keyed) {
            scrambler = new Scrambler(keyed);
        }

        @Override
        public Tuple2<IntValue, IntValue> reduce(
                Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) throws Exception {
            return scrambler.scramble(a, b);
        }
    }

    // --------------------------------------------------------------------------------------------

    public void testJoin(ExecutionEnvironment env) throws Exception {
        /*
         * Test JoinDriver, LeftOuterJoinDriver, RightOuterJoinDriver, and FullOuterJoinDriver
         */

        for (JoinHint joinHint : JoinHint.values()) {
            if (joinHint == JoinHint.OPTIMIZER_CHOOSES) {
                continue;
            }

            List<Tuple2<IntValue, IntValue>> enabledResult;

            List<Tuple2<IntValue, IntValue>> disabledResult;

            // Inner join

            LOG.info("Testing inner join with JoinHint = {}", joinHint);

            env.getConfig().enableObjectReuse();

            enabledResult =
                    getDataSet(env)
                            .join(getDataSet(env), joinHint)
                            .where(0)
                            .equalTo(0)
                            .with(new OverwriteObjectsJoin())
                            .collect();

            Collections.sort(enabledResult, comparator);

            env.getConfig().disableObjectReuse();

            disabledResult =
                    getDataSet(env)
                            .join(getDataSet(env), joinHint)
                            .where(0)
                            .equalTo(0)
                            .with(new OverwriteObjectsJoin())
                            .collect();

            Collections.sort(disabledResult, comparator);

            Assert.assertEquals("JoinHint=" + joinHint, disabledResult, enabledResult);

            // Left outer join

            if (joinHint != JoinHint.BROADCAST_HASH_FIRST) {
                LOG.info("Testing left outer join with JoinHint = {}", joinHint);

                env.getConfig().enableObjectReuse();

                enabledResult =
                        getDataSet(env)
                                .leftOuterJoin(getFilteredDataSet(env), joinHint)
                                .where(0)
                                .equalTo(0)
                                .with(new OverwriteObjectsJoin())
                                .collect();

                Collections.sort(enabledResult, comparator);

                env.getConfig().disableObjectReuse();

                disabledResult =
                        getDataSet(env)
                                .leftOuterJoin(getFilteredDataSet(env), joinHint)
                                .where(0)
                                .equalTo(0)
                                .with(new OverwriteObjectsJoin())
                                .collect();

                Collections.sort(disabledResult, comparator);

                Assert.assertThat("JoinHint=" + joinHint, disabledResult, is(enabledResult));
            }

            // Right outer join

            if (joinHint != JoinHint.BROADCAST_HASH_SECOND) {
                LOG.info("Testing right outer join with JoinHint = {}", joinHint);

                env.getConfig().enableObjectReuse();

                enabledResult =
                        getDataSet(env)
                                .rightOuterJoin(getFilteredDataSet(env), joinHint)
                                .where(0)
                                .equalTo(0)
                                .with(new OverwriteObjectsJoin())
                                .collect();

                Collections.sort(enabledResult, comparator);

                env.getConfig().disableObjectReuse();

                disabledResult =
                        getDataSet(env)
                                .rightOuterJoin(getFilteredDataSet(env), joinHint)
                                .where(0)
                                .equalTo(0)
                                .with(new OverwriteObjectsJoin())
                                .collect();

                Collections.sort(disabledResult, comparator);

                Assert.assertThat("JoinHint=" + joinHint, disabledResult, is(enabledResult));
            }

            // Full outer join

            if (joinHint != JoinHint.BROADCAST_HASH_FIRST
                    && joinHint != JoinHint.BROADCAST_HASH_SECOND) {
                LOG.info("Testing full outer join with JoinHint = {}", joinHint);

                env.getConfig().enableObjectReuse();

                enabledResult =
                        getDataSet(env)
                                .fullOuterJoin(getFilteredDataSet(env), joinHint)
                                .where(0)
                                .equalTo(0)
                                .with(new OverwriteObjectsJoin())
                                .collect();

                Collections.sort(enabledResult, comparator);

                env.getConfig().disableObjectReuse();

                disabledResult =
                        getDataSet(env)
                                .fullOuterJoin(getFilteredDataSet(env), joinHint)
                                .where(0)
                                .equalTo(0)
                                .with(new OverwriteObjectsJoin())
                                .collect();

                Collections.sort(disabledResult, comparator);

                Assert.assertThat("JoinHint=" + joinHint, disabledResult, is(enabledResult));
            }
        }
    }

    private class OverwriteObjectsJoin
            implements JoinFunction<
                    Tuple2<IntValue, IntValue>,
                    Tuple2<IntValue, IntValue>,
                    Tuple2<IntValue, IntValue>> {
        private Scrambler scrambler = new Scrambler(true);

        @Override
        public Tuple2<IntValue, IntValue> join(
                Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) throws Exception {
            return scrambler.scramble(a, b);
        }
    }

    // --------------------------------------------------------------------------------------------

    public void testCross(ExecutionEnvironment env) throws Exception {
        /*
         * Test CrossDriver
         */

        LOG.info("Testing cross");

        DataSet<Tuple2<IntValue, IntValue>> small = getDataSet(env, 100, 20);
        DataSet<Tuple2<IntValue, IntValue>> large = getDataSet(env, 10000, 2000);

        // test NESTEDLOOP_BLOCKED_OUTER_FIRST and NESTEDLOOP_BLOCKED_OUTER_SECOND with object reuse
        // enabled

        env.getConfig().enableObjectReuse();

        List<Tuple2<IntValue, IntValue>> enabledResultWithHuge =
                small.crossWithHuge(large).with(new OverwriteObjectsCross()).collect();

        List<Tuple2<IntValue, IntValue>> enabledResultWithTiny =
                small.crossWithTiny(large).with(new OverwriteObjectsCross()).collect();

        Assert.assertThat(enabledResultWithHuge, is(enabledResultWithTiny));

        // test NESTEDLOOP_BLOCKED_OUTER_FIRST and NESTEDLOOP_BLOCKED_OUTER_SECOND with object reuse
        // disabled

        env.getConfig().disableObjectReuse();

        List<Tuple2<IntValue, IntValue>> disabledResultWithHuge =
                small.crossWithHuge(large).with(new OverwriteObjectsCross()).collect();

        List<Tuple2<IntValue, IntValue>> disabledResultWithTiny =
                small.crossWithTiny(large).with(new OverwriteObjectsCross()).collect();

        Assert.assertThat(disabledResultWithHuge, is(disabledResultWithTiny));

        // verify match between object reuse enabled and disabled
        Assert.assertThat(disabledResultWithHuge, is(enabledResultWithHuge));
        Assert.assertThat(disabledResultWithTiny, is(enabledResultWithTiny));
    }

    private class OverwriteObjectsCross
            implements CrossFunction<
                    Tuple2<IntValue, IntValue>,
                    Tuple2<IntValue, IntValue>,
                    Tuple2<IntValue, IntValue>> {
        private Scrambler scrambler = new Scrambler(true);

        @Override
        public Tuple2<IntValue, IntValue> cross(
                Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) throws Exception {
            return scrambler.scramble(a, b);
        }
    }

    // --------------------------------------------------------------------------------------------

    private DataSet<Tuple2<IntValue, IntValue>> getDataSet(
            ExecutionEnvironment env, int numberOfElements, int keyRange) {
        return env.fromCollection(
                new TupleIntValueIntValueIterator(numberOfElements, keyRange),
                TupleTypeInfo.<Tuple2<IntValue, IntValue>>getBasicAndBasicValueTupleTypeInfo(
                        IntValue.class, IntValue.class));
    }

    private DataSet<Tuple2<IntValue, IntValue>> getDataSet(ExecutionEnvironment env) {
        return getDataSet(env, NUMBER_OF_ELEMENTS, KEY_RANGE);
    }

    private DataSet<Tuple2<IntValue, IntValue>> getFilteredDataSet(ExecutionEnvironment env) {
        return getDataSet(env)
                .filter(
                        new FilterFunction<Tuple2<IntValue, IntValue>>() {
                            @Override
                            public boolean filter(Tuple2<IntValue, IntValue> value)
                                    throws Exception {
                                return (value.f0.getValue() % 2) == 0;
                            }
                        });
    }

    private static class TupleIntValueIntValueIterator
            implements Iterator<Tuple2<IntValue, IntValue>>, Serializable {
        private int numElements;
        private final int keyRange;
        private Tuple2<IntValue, IntValue> ret = new Tuple2<>(new IntValue(), new IntValue());

        public TupleIntValueIntValueIterator(int numElements, int keyRange) {
            this.numElements = numElements;
            this.keyRange = keyRange;
        }

        private final Random rnd = new Random(123);

        @Override
        public boolean hasNext() {
            return numElements > 0;
        }

        @Override
        public Tuple2<IntValue, IntValue> next() {
            numElements--;
            ret.f0.setValue(rnd.nextInt(keyRange));
            ret.f1.setValue(1);
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class Tuple2Comparator<T0 extends Comparable<T0>, T1 extends Comparable<T1>>
            implements Comparator<Tuple2<T0, T1>> {
        @Override
        public int compare(Tuple2<T0, T1> o1, Tuple2<T0, T1> o2) {
            int cmp = o1.f0.compareTo(o2.f0);

            if (cmp != 0) {
                return cmp;
            }

            return o1.f1.compareTo(o2.f1);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class Scrambler implements Serializable {
        private Tuple2<IntValue, IntValue> d = new Tuple2<>(new IntValue(), new IntValue());

        private final boolean keyed;

        public Scrambler(boolean keyed) {
            this.keyed = keyed;
        }

        public Tuple2<IntValue, IntValue> scramble(
                Tuple2<IntValue, IntValue> a, Tuple2<IntValue, IntValue> b) {
            /*
             * Scramble all fields except returned object's key
             *
             * Randomly select among four return values:
             *
             *   0) return first object (a)
             *   1) return second object (b)
             *   2) return new object
             *   3) return reused local object (d)
             */

            Random random = new Random(RANDOM_SEED);

            if (a != null && b != null) {
                random.setSeed((((long) a.f0.getValue()) << 32) + b.f0.getValue());
            } else if (a != null) {
                random.setSeed(a.f0.getValue());
            } else if (b != null) {
                random.setSeed(b.f0.getValue());
            } else {
                throw new RuntimeException("One of a or b should be not null");
            }

            Tuple2<IntValue, IntValue> result;

            switch (random.nextInt(4)) {
                case 0:
                    result = a;
                    break;
                case 1:
                    result = b;
                    break;
                case 2:
                    result = d;
                    break;
                case 3:
                    result = new Tuple2<>(new IntValue(), new IntValue());
                    break;
                default:
                    throw new RuntimeException("Unexpected value in switch statement");
            }

            if (a == null || b == null) {
                // null values are seen when processing outer joins
                if (result == null) {
                    result = d;
                }

                if (a == null) {
                    b.f0.copyTo(result.f0);
                    b.f1.copyTo(result.f1);
                } else {
                    a.f0.copyTo(result.f0);
                    a.f1.copyTo(result.f1);
                }
            } else {
                if (keyed) {
                    result.f0.setValue(a.f0.getValue());
                } else {
                    result.f0.setValue(a.f0.getValue() + b.f0.getValue());
                }

                result.f1.setValue(a.f1.getValue() + b.f1.getValue());
            }

            scrambleIfNot(a, result);
            scrambleIfNot(b, result);
            scrambleIfNot(d, result);

            return result;
        }

        private Random random = new Random(~RANDOM_SEED);

        private void scrambleIfNot(Tuple2<IntValue, IntValue> t, Object o) {
            // verify that the tuple is not null and the same as the
            // comparison object, then scramble the fields
            if (t != null && t != o) {
                t.f0.setValue(random.nextInt());
                t.f1.setValue(random.nextInt());
            }
        }
    }
}
