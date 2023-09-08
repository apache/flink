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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.test.operators.util.CollectionDataSets;
import org.apache.flink.test.operators.util.CollectionDataSets.CustomType;
import org.apache.flink.test.operators.util.CollectionDataSets.POJO;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.test.util.TestBaseUtils.compareResultAsText;
import static org.apache.flink.test.util.TestBaseUtils.compareResultAsTuples;

/** Integration tests for {@link CoGroupFunction} and {@link RichCoGroupFunction}. */
@RunWith(Parameterized.class)
public class CoGroupITCase extends MultipleProgramsTestBase {

    public CoGroupITCase(TestExecutionMode mode) {
        super(mode);
    }

    /*
     * CoGroup on tuples with key field selector
     */
    @Test
    public void testCoGroupTuplesWithKeyFieldSelector() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<Integer, Integer>> coGroupDs =
                ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple5CoGroup());

        List<Tuple2<Integer, Integer>> result = coGroupDs.collect();

        String expected = "1,0\n" + "2,6\n" + "3,24\n" + "4,60\n" + "5,120\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupOnTwoCustomTypeInputsWithKeyExtractors() throws Exception {
        /*
         * CoGroup on two custom type inputs with key extractors
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> coGroupDs =
                ds.coGroup(ds2)
                        .where(new KeySelector4())
                        .equalTo(new KeySelector5())
                        .with(new CustomTypeCoGroup());

        List<CustomType> result = coGroupDs.collect();

        String expected =
                "1,0,test\n"
                        + "2,6,test\n"
                        + "3,24,test\n"
                        + "4,60,test\n"
                        + "5,120,test\n"
                        + "6,210,test\n";

        compareResultAsText(result, expected);
    }

    private static class KeySelector4 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    private static class KeySelector5 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    @Test
    public void testCorrectnessOfCoGroupIfUDFReturnsLeftInputObjects() throws Exception {
        /*
         * check correctness of cogroup if UDF returns left input objects
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> coGroupDs =
                ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple3ReturnLeft());

        List<Tuple3<Integer, Long, String>> result = coGroupDs.collect();

        String expected =
                "1,1,Hi\n"
                        + "2,2,Hello\n"
                        + "3,2,Hello world\n"
                        + "4,3,Hello world, how are you?\n"
                        + "5,3,I am fine.\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCorrectnessOfCoGroupIfUDFReturnsRightInputObjects() throws Exception {
        /*
         * check correctness of cogroup if UDF returns right input objects
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> coGroupDs =
                ds.coGroup(ds2).where(0).equalTo(0).with(new Tuple5ReturnRight());

        List<Tuple5<Integer, Long, Integer, String, Long>> result = coGroupDs.collect();

        String expected =
                "1,1,0,Hallo,1\n"
                        + "2,2,1,Hallo Welt,2\n"
                        + "2,3,2,Hallo Welt wie,1\n"
                        + "3,4,3,Hallo Welt wie gehts?,2\n"
                        + "3,5,4,ABC,2\n"
                        + "3,6,5,BCD,3\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupWithBroadcastSet() throws Exception {
        /*
         * Reduce with broadcast set
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Integer, Integer>> coGroupDs =
                ds.coGroup(ds2)
                        .where(0)
                        .equalTo(0)
                        .with(new Tuple5CoGroupBC())
                        .withBroadcastSet(intDs, "ints");

        List<Tuple3<Integer, Integer, Integer>> result = coGroupDs.collect();

        String expected = "1,0,55\n" + "2,6,55\n" + "3,24,55\n" + "4,60,55\n" + "5,120,55\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupOnATupleInputWithKeyFieldSelectorAndACustomTypeInputWithKeyExtractor()
            throws Exception {
        /*
         * CoGroup on a tuple input with key field selector and a custom type input with key extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> coGroupDs =
                ds.coGroup(ds2).where(2).equalTo(new KeySelector2()).with(new MixedCoGroup());

        List<Tuple3<Integer, Long, String>> result = coGroupDs.collect();

        String expected =
                "0,1,test\n"
                        + "1,2,test\n"
                        + "2,5,test\n"
                        + "3,15,test\n"
                        + "4,33,test\n"
                        + "5,63,test\n"
                        + "6,109,test\n"
                        + "7,4,test\n"
                        + "8,4,test\n"
                        + "9,4,test\n"
                        + "10,5,test\n"
                        + "11,5,test\n"
                        + "12,5,test\n"
                        + "13,5,test\n"
                        + "14,5,test\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector2 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    @Test
    public void testCoGroupOnACustomTypeWithKeyExtractorAndATupleInputWithKeyFieldSelector()
            throws Exception {
        /*
         * CoGroup on a tuple input with key field selector and a custom type input with key extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> coGroupDs =
                ds2.coGroup(ds).where(new KeySelector3()).equalTo(2).with(new MixedCoGroup2());

        List<CustomType> result = coGroupDs.collect();

        String expected =
                "0,1,test\n"
                        + "1,2,test\n"
                        + "2,5,test\n"
                        + "3,15,test\n"
                        + "4,33,test\n"
                        + "5,63,test\n"
                        + "6,109,test\n"
                        + "7,4,test\n"
                        + "8,4,test\n"
                        + "9,4,test\n"
                        + "10,5,test\n"
                        + "11,5,test\n"
                        + "12,5,test\n"
                        + "13,5,test\n"
                        + "14,5,test\n";

        compareResultAsText(result, expected);
    }

    private static class KeySelector3 implements KeySelector<CustomType, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(CustomType in) {
            return in.myInt;
        }
    }

    @Test
    public void testCoGroupWithMultipleKeyFieldsWithFieldSelector() throws Exception {
        /*
         * CoGroup with multiple key fields
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> coGrouped =
                ds1.coGroup(ds2).where(0, 4).equalTo(0, 1).with(new Tuple5Tuple3CoGroup());

        List<Tuple3<Integer, Long, String>> result = coGrouped.collect();

        String expected =
                "1,1,Hallo\n"
                        + "2,2,Hallo Welt\n"
                        + "3,2,Hallo Welt wie gehts?\n"
                        + "3,2,ABC\n"
                        + "5,3,HIJ\n"
                        + "5,3,IJK\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupWithMultipleKeyFieldsWithStaticClassKeyExtractor() throws Exception {
        /*
         * CoGroup with multiple key fields
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> coGrouped =
                ds1.coGroup(ds2)
                        .where(new KeySelector7())
                        .equalTo(new KeySelector8())
                        .with(new Tuple5Tuple3CoGroup());

        List<Tuple3<Integer, Long, String>> result = coGrouped.collect();

        String expected =
                "1,1,Hallo\n"
                        + "2,2,Hallo Welt\n"
                        + "3,2,Hallo Welt wie gehts?\n"
                        + "3,2,ABC\n"
                        + "5,3,HIJ\n"
                        + "5,3,IJK\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupWithMultipleKeyFieldsWithInnerClassKeyExtractorWithClosureCleaner()
            throws Exception {
        /*
         * CoGroup with multiple key fields, test working closure cleaner for inner classes
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> coGrouped =
                ds1.coGroup(ds2)
                        .where(
                                new KeySelector<
                                        Tuple5<Integer, Long, Integer, String, Long>,
                                        Tuple2<Integer, Long>>() {
                                    @Override
                                    public Tuple2<Integer, Long> getKey(
                                            Tuple5<Integer, Long, Integer, String, Long> t)
                                            throws Exception {
                                        return new Tuple2<Integer, Long>(t.f0, t.f4);
                                    }
                                })
                        .equalTo(
                                new KeySelector<
                                        Tuple3<Integer, Long, String>, Tuple2<Integer, Long>>() {

                                    @Override
                                    public Tuple2<Integer, Long> getKey(
                                            Tuple3<Integer, Long, String> t) {
                                        return new Tuple2<>(t.f0, t.f1);
                                    }
                                })
                        .with(
                                new CoGroupFunction<
                                        Tuple5<Integer, Long, Integer, String, Long>,
                                        Tuple3<Integer, Long, String>,
                                        Tuple3<Integer, Long, String>>() {
                                    @Override
                                    public void coGroup(
                                            Iterable<Tuple5<Integer, Long, Integer, String, Long>>
                                                    first,
                                            Iterable<Tuple3<Integer, Long, String>> second,
                                            Collector<Tuple3<Integer, Long, String>> out) {
                                        List<String> strs = new ArrayList<>();

                                        for (Tuple5<Integer, Long, Integer, String, Long> t :
                                                first) {
                                            strs.add(t.f3);
                                        }

                                        for (Tuple3<Integer, Long, String> t : second) {
                                            for (String s : strs) {
                                                out.collect(
                                                        new Tuple3<Integer, Long, String>(
                                                                t.f0, t.f1, s));
                                            }
                                        }
                                    }
                                });

        List<Tuple3<Integer, Long, String>> result = coGrouped.collect();

        String expected =
                "1,1,Hallo\n"
                        + "2,2,Hallo Welt\n"
                        + "3,2,Hallo Welt wie gehts?\n"
                        + "3,2,ABC\n"
                        + "5,3,HIJ\n"
                        + "5,3,IJK\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupWithMultipleKeyFieldsWithInnerClassKeyExtractorWithoutClosureCleaner()
            throws Exception {
        /*
         * CoGroup with multiple key fields, test that disabling closure cleaner leads to an exception when using inner
         * classes.
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
        boolean correctExceptionTriggered = false;
        try {
            DataSet<Tuple3<Integer, Long, String>> coGrouped =
                    ds1.coGroup(ds2)
                            .where(
                                    new KeySelector<
                                            Tuple5<Integer, Long, Integer, String, Long>,
                                            Tuple2<Integer, Long>>() {
                                        @Override
                                        public Tuple2<Integer, Long> getKey(
                                                Tuple5<Integer, Long, Integer, String, Long> t)
                                                throws Exception {
                                            return new Tuple2<Integer, Long>(t.f0, t.f4);
                                        }
                                    })
                            .equalTo(
                                    new KeySelector<
                                            Tuple3<Integer, Long, String>,
                                            Tuple2<Integer, Long>>() {

                                        @Override
                                        public Tuple2<Integer, Long> getKey(
                                                Tuple3<Integer, Long, String> t) {
                                            return new Tuple2<Integer, Long>(t.f0, t.f1);
                                        }
                                    })
                            .with(
                                    new CoGroupFunction<
                                            Tuple5<Integer, Long, Integer, String, Long>,
                                            Tuple3<Integer, Long, String>,
                                            Tuple3<Integer, Long, String>>() {
                                        @Override
                                        public void coGroup(
                                                Iterable<
                                                                Tuple5<
                                                                        Integer,
                                                                        Long,
                                                                        Integer,
                                                                        String,
                                                                        Long>>
                                                        first,
                                                Iterable<Tuple3<Integer, Long, String>> second,
                                                Collector<Tuple3<Integer, Long, String>> out) {
                                            List<String> strs = new ArrayList<String>();

                                            for (Tuple5<Integer, Long, Integer, String, Long> t :
                                                    first) {
                                                strs.add(t.f3);
                                            }

                                            for (Tuple3<Integer, Long, String> t : second) {
                                                for (String s : strs) {
                                                    out.collect(
                                                            new Tuple3<Integer, Long, String>(
                                                                    t.f0, t.f1, s));
                                                }
                                            }
                                        }
                                    });
        } catch (InvalidProgramException ex) {
            correctExceptionTriggered = (ex.getCause() instanceof java.io.NotSerializableException);
        }
        Assert.assertTrue(correctExceptionTriggered);
    }

    private static class KeySelector7
            implements KeySelector<
                    Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
            return new Tuple2<Integer, Long>(t.f0, t.f4);
        }
    }

    private static class KeySelector8
            implements KeySelector<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> t) {
            return new Tuple2<Integer, Long>(t.f0, t.f1);
        }
    }

    @Test
    public void testCoGroupTwoCustomTypeInputsWithExpressionKeys() throws Exception {
        /*
         * CoGroup on two custom type inputs using expression keys
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> coGroupDs =
                ds.coGroup(ds2).where("myInt").equalTo("myInt").with(new CustomTypeCoGroup());

        List<CustomType> result = coGroupDs.collect();

        String expected =
                "1,0,test\n"
                        + "2,6,test\n"
                        + "3,24,test\n"
                        + "4,60,test\n"
                        + "5,120,test\n"
                        + "6,210,test\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testCoGroupOnTwoCustomTypeInputsWithExpressionKeyAndFieldSelector()
            throws Exception {
        /*
         * CoGroup on two custom type inputs using expression keys
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<CustomType> coGroupDs =
                ds.coGroup(ds2).where("nestedPojo.longNumber").equalTo(6).with(new CoGroup1());

        List<CustomType> result = coGroupDs.collect();

        String expected = "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n";

        compareResultAsText(result, expected);
    }

    private static class CoGroup1
            implements CoGroupFunction<
                    POJO,
                    Tuple7<Integer, String, Integer, Integer, Long, String, Long>,
                    CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<POJO> first,
                Iterable<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> second,
                Collector<CustomType> out)
                throws Exception {
            for (POJO p : first) {
                for (Tuple7<Integer, String, Integer, Integer, Long, String, Long> t : second) {
                    Assert.assertTrue(p.nestedPojo.longNumber == t.f6);
                    out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"));
                }
            }
        }
    }

    @Test
    public void testCoGroupFieldSelectorAndComplicatedKeySelector() throws Exception {
        /*
         * CoGroup field-selector (expression keys) + key selector function
         * The key selector is unnecessary complicated (Tuple1) ;)
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<CustomType> coGroupDs =
                ds.coGroup(ds2).where(new KeySelector6()).equalTo(6).with(new CoGroup3());

        List<CustomType> result = coGroupDs.collect();

        String expected = "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n";

        compareResultAsText(result, expected);
    }

    private static class KeySelector6 implements KeySelector<POJO, Tuple1<Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple1<Long> getKey(POJO value) throws Exception {
            return new Tuple1<Long>(value.nestedPojo.longNumber);
        }
    }

    private static class CoGroup3
            implements CoGroupFunction<
                    POJO,
                    Tuple7<Integer, String, Integer, Integer, Long, String, Long>,
                    CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<POJO> first,
                Iterable<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> second,
                Collector<CustomType> out)
                throws Exception {
            for (POJO p : first) {
                for (Tuple7<Integer, String, Integer, Integer, Long, String, Long> t : second) {
                    Assert.assertTrue(p.nestedPojo.longNumber == t.f6);
                    out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"));
                }
            }
        }
    }

    @Test
    public void testCoGroupFieldSelectorAndKeySelector() throws Exception {
        /*
         * CoGroup field-selector (expression keys) + key selector function
         * The key selector is simple here
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<CustomType> coGroupDs =
                ds.coGroup(ds2).where(new KeySelector1()).equalTo(6).with(new CoGroup2());

        List<CustomType> result = coGroupDs.collect();

        String expected = "-1,20000,Flink\n" + "-1,10000,Flink\n" + "-1,30000,Flink\n";

        compareResultAsText(result, expected);
    }

    @Test
    public void testCoGroupWithAtomicType1() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Integer> ds2 = env.fromElements(0, 1, 2);

        DataSet<Tuple3<Integer, Long, String>> coGroupDs =
                ds1.coGroup(ds2).where(0).equalTo("*").with(new CoGroupAtomic1());

        List<Tuple3<Integer, Long, String>> result = coGroupDs.collect();

        String expected = "(1,1,Hi)\n" + "(2,2,Hello)";

        compareResultAsText(result, expected);
    }

    @Test
    public void testCoGroupWithAtomicType2() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> ds1 = env.fromElements(0, 1, 2);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> coGroupDs =
                ds1.coGroup(ds2).where("*").equalTo(0).with(new CoGroupAtomic2());

        List<Tuple3<Integer, Long, String>> result = coGroupDs.collect();

        String expected = "(1,1,Hi)\n" + "(2,2,Hello)";

        compareResultAsText(result, expected);
    }

    @Test
    public void testCoGroupWithRangePartitioning() throws Exception {
        /*
         * Test coGroup on tuples with multiple key field positions and same customized distribution
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds1 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);

        env.setParallelism(4);
        TestDistribution testDis = new TestDistribution();
        DataSet<Tuple3<Integer, Long, String>> coGrouped =
                DataSetUtils.partitionByRange(ds1, testDis, 0, 4)
                        .coGroup(DataSetUtils.partitionByRange(ds2, testDis, 0, 1))
                        .where(0, 4)
                        .equalTo(0, 1)
                        .with(new Tuple5Tuple3CoGroup());

        List<Tuple3<Integer, Long, String>> result = coGrouped.collect();

        String expected =
                "1,1,Hallo\n"
                        + "2,2,Hallo Welt\n"
                        + "3,2,Hallo Welt wie gehts?\n"
                        + "3,2,ABC\n"
                        + "5,3,HIJ\n"
                        + "5,3,IJK\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testCoGroupLambda() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> left =
                env.fromElements(
                        new Tuple2<>(1, "hello"), new Tuple2<>(2, "what's"), new Tuple2<>(2, "up"));
        DataSet<Tuple2<Integer, String>> right =
                env.fromElements(
                        new Tuple2<>(1, "not"), new Tuple2<>(1, "much"), new Tuple2<>(2, "really"));
        DataSet<Integer> joined =
                left.coGroup(right)
                        .where(0)
                        .equalTo(0)
                        .with(
                                (Iterable<Tuple2<Integer, String>> values1,
                                        Iterable<Tuple2<Integer, String>> values2,
                                        Collector<Integer> out) -> {
                                    int sum = 0;
                                    for (Tuple2<Integer, String> next : values1) {
                                        sum += next.f0;
                                    }
                                    for (Tuple2<Integer, String> next : values2) {
                                        sum += next.f0;
                                    }
                                    out.collect(sum);
                                })
                        .returns(Integer.class);
        List<Integer> result = joined.collect();

        String expected = "6\n3\n";

        compareResultAsText(result, expected);
    }

    // --------------------------------------------------------------------------------------------
    //  UDF classes
    // --------------------------------------------------------------------------------------------

    private static class KeySelector1 implements KeySelector<POJO, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long getKey(POJO value) throws Exception {
            return value.nestedPojo.longNumber;
        }
    }

    private static class CoGroup2
            implements CoGroupFunction<
                    POJO,
                    Tuple7<Integer, String, Integer, Integer, Long, String, Long>,
                    CustomType> {
        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<POJO> first,
                Iterable<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> second,
                Collector<CustomType> out)
                throws Exception {
            for (POJO p : first) {
                for (Tuple7<Integer, String, Integer, Integer, Long, String, Long> t : second) {
                    Assert.assertTrue(p.nestedPojo.longNumber == t.f6);
                    out.collect(new CustomType(-1, p.nestedPojo.longNumber, "Flink"));
                }
            }
        }
    }

    private static class Tuple5CoGroup
            implements CoGroupFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
                Collector<Tuple2<Integer, Integer>> out) {
            int sum = 0;
            int id = 0;

            for (Tuple5<Integer, Long, Integer, String, Long> element : first) {
                sum += element.f2;
                id = element.f0;
            }

            for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
                sum += element.f2;
                id = element.f0;
            }

            out.collect(new Tuple2<Integer, Integer>(id, sum));
        }
    }

    private static class CustomTypeCoGroup
            implements CoGroupFunction<CustomType, CustomType, CustomType> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<CustomType> first,
                Iterable<CustomType> second,
                Collector<CustomType> out) {

            CustomType o = new CustomType(0, 0, "test");

            for (CustomType element : first) {
                o.myInt = element.myInt;
                o.myLong += element.myLong;
            }

            for (CustomType element : second) {
                o.myInt = element.myInt;
                o.myLong += element.myLong;
            }

            out.collect(o);
        }
    }

    private static class MixedCoGroup
            implements CoGroupFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    CustomType,
                    Tuple3<Integer, Long, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
                Iterable<CustomType> second,
                Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {

            long sum = 0;
            int id = 0;

            for (Tuple5<Integer, Long, Integer, String, Long> element : first) {
                sum += element.f0;
                id = element.f2;
            }

            for (CustomType element : second) {
                id = element.myInt;
                sum += element.myLong;
            }

            out.collect(new Tuple3<Integer, Long, String>(id, sum, "test"));
        }
    }

    private static class MixedCoGroup2
            implements CoGroupFunction<
                    CustomType, Tuple5<Integer, Long, Integer, String, Long>, CustomType> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<CustomType> first,
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
                Collector<CustomType> out) {
            CustomType o = new CustomType(0, 0, "test");

            for (CustomType element : first) {
                o.myInt = element.myInt;
                o.myLong += element.myLong;
            }

            for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
                o.myInt = element.f2;
                o.myLong += element.f0;
            }

            out.collect(o);
        }
    }

    private static class Tuple3ReturnLeft
            implements CoGroupFunction<
                    Tuple3<Integer, Long, String>,
                    Tuple3<Integer, Long, String>,
                    Tuple3<Integer, Long, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Tuple3<Integer, Long, String>> first,
                Iterable<Tuple3<Integer, Long, String>> second,
                Collector<Tuple3<Integer, Long, String>> out) {
            for (Tuple3<Integer, Long, String> element : first) {
                if (element.f0 < 6) {
                    out.collect(element);
                }
            }
        }
    }

    private static class Tuple5ReturnRight
            implements CoGroupFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
                Collector<Tuple5<Integer, Long, Integer, String, Long>> out) {
            for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
                if (element.f0 < 4) {
                    out.collect(element);
                }
            }
        }
    }

    private static class Tuple5CoGroupBC
            extends RichCoGroupFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple3<Integer, Integer, Integer>> {

        private static final long serialVersionUID = 1L;

        private int broadcast = 42;

        @Override
        public void open(OpenContext openContext) {

            Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
            int sum = 0;
            for (Integer i : ints) {
                sum += i;
            }
            broadcast = sum;
        }

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> second,
                Collector<Tuple3<Integer, Integer, Integer>> out) {
            int sum = 0;
            int id = 0;

            for (Tuple5<Integer, Long, Integer, String, Long> element : first) {
                sum += element.f2;
                id = element.f0;
            }

            for (Tuple5<Integer, Long, Integer, String, Long> element : second) {
                sum += element.f2;
                id = element.f0;
            }

            out.collect(new Tuple3<Integer, Integer, Integer>(id, sum, broadcast));
        }
    }

    private static class Tuple5Tuple3CoGroup
            implements CoGroupFunction<
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple3<Integer, Long, String>,
                    Tuple3<Integer, Long, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Tuple5<Integer, Long, Integer, String, Long>> first,
                Iterable<Tuple3<Integer, Long, String>> second,
                Collector<Tuple3<Integer, Long, String>> out) {
            List<String> strs = new ArrayList<String>();

            for (Tuple5<Integer, Long, Integer, String, Long> t : first) {
                strs.add(t.f3);
            }

            for (Tuple3<Integer, Long, String> t : second) {
                for (String s : strs) {
                    out.collect(new Tuple3<Integer, Long, String>(t.f0, t.f1, s));
                }
            }
        }
    }

    private static class CoGroupAtomic1
            implements CoGroupFunction<
                    Tuple3<Integer, Long, String>, Integer, Tuple3<Integer, Long, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Tuple3<Integer, Long, String>> first,
                Iterable<Integer> second,
                Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            List<Integer> ints = new ArrayList<Integer>();

            for (Integer i : second) {
                ints.add(i);
            }

            for (Tuple3<Integer, Long, String> t : first) {
                for (Integer i : ints) {
                    if (t.f0.equals(i)) {
                        out.collect(t);
                    }
                }
            }
        }
    }

    private static class CoGroupAtomic2
            implements CoGroupFunction<
                    Integer, Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>> {

        private static final long serialVersionUID = 1L;

        @Override
        public void coGroup(
                Iterable<Integer> first,
                Iterable<Tuple3<Integer, Long, String>> second,
                Collector<Tuple3<Integer, Long, String>> out)
                throws Exception {
            List<Integer> ints = new ArrayList<Integer>();

            for (Integer i : first) {
                ints.add(i);
            }

            for (Tuple3<Integer, Long, String> t : second) {
                for (Integer i : ints) {
                    if (t.f0.equals(i)) {
                        out.collect(t);
                    }
                }
            }
        }
    }

    /** Test {@link DataDistribution}. */
    public static class TestDistribution implements DataDistribution {
        public Object[][] boundaries =
                new Object[][] {
                    new Object[] {2, 2L},
                    new Object[] {5, 4L},
                    new Object[] {10, 12L},
                    new Object[] {21, 6L}
                };

        public TestDistribution() {}

        @Override
        public Object[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
            return boundaries[bucketNum];
        }

        @Override
        public int getNumberOfFields() {
            return 2;
        }

        @Override
        public TypeInformation[] getKeyTypes() {
            return new TypeInformation[] {
                BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
            };
        }

        @Override
        public void write(DataOutputView out) throws IOException {}

        @Override
        public void read(DataInputView in) throws IOException {}

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestDistribution;
        }
    }
}
