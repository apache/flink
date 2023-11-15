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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
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
import java.util.Collection;
import java.util.List;

import static org.apache.flink.test.util.TestBaseUtils.compareResultAsTuples;

/** Integration tests for {@link JoinFunction} and {@link FlatJoinFunction}. */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class JoinITCase extends MultipleProgramsTestBase {

    public JoinITCase(TestExecutionMode mode) {
        super(mode);
    }

    @Test
    public void testUDFJoinOnTuplesWithKeyFieldPositions() throws Exception {
        /*
         * UDF Join on tuples with key field positions
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.join(ds2).where(1).equalTo(1).with(new T3T5FlatJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testeUDFJoinOnTuplesWithMultipleKeyFieldPositions() throws Exception {
        /*
         * UDF Join on tuples with multiple key field positions
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.join(ds2).where(0, 1).equalTo(0, 4).with(new T3T5FlatJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected =
                "Hi,Hallo\n"
                        + "Hello,Hallo Welt\n"
                        + "Hello world,Hallo Welt wie gehts?\n"
                        + "Hello world,ABC\n"
                        + "I am fine.,HIJ\n"
                        + "I am fine.,IJK\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testDefaultJoinOnTuples() throws Exception {
        /*
         * Default Join on tuples
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>>>
                joinDs = ds1.join(ds2).where(0).equalTo(2);

        List<Tuple2<Tuple3<Integer, Long, String>, Tuple5<Integer, Long, Integer, String, Long>>>
                result = joinDs.collect();

        String expected =
                "(1,1,Hi),(2,2,1,Hallo Welt,2)\n"
                        + "(2,2,Hello),(2,3,2,Hallo Welt wie,1)\n"
                        + "(3,2,Hello world),(3,4,3,Hallo Welt wie gehts?,2)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinWithHuge() throws Exception {
        /*
         * Join with Huge
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.joinWithHuge(ds2).where(1).equalTo(1).with(new T3T5FlatJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinWithTiny() throws Exception {
        /*
         * Join with Tiny
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.joinWithTiny(ds2).where(1).equalTo(1).with(new T3T5FlatJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected = "Hi,Hallo\n" + "Hello,Hallo Welt\n" + "Hello world,Hallo Welt\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinThatReturnsTheLeftInputObject() throws Exception {
        /*
         * Join that returns the left input object
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> joinDs =
                ds1.join(ds2).where(1).equalTo(1).with(new LeftReturningJoin());

        List<Tuple3<Integer, Long, String>> result = joinDs.collect();

        String expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinThatReturnsTheRightInputObject() throws Exception {
        /*
         * Join that returns the right input object
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> joinDs =
                ds1.join(ds2).where(1).equalTo(1).with(new RightReturningJoin());

        List<Tuple5<Integer, Long, Integer, String, Long>> result = joinDs.collect();

        String expected = "1,1,0,Hallo,1\n" + "2,2,1,Hallo Welt,2\n" + "2,2,1,Hallo Welt,2\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinWithBroadcastSet() throws Exception {
        /*
         * Join with broadcast set
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> intDs = CollectionDataSets.getIntegerDataSet(env);

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.getSmall5TupleDataSet(env);
        DataSet<Tuple3<String, String, Integer>> joinDs =
                ds1.join(ds2)
                        .where(1)
                        .equalTo(4)
                        .with(new T3T5BCJoin())
                        .withBroadcastSet(intDs, "ints");

        List<Tuple3<String, String, Integer>> result = joinDs.collect();

        String expected =
                "Hi,Hallo,55\n"
                        + "Hi,Hallo Welt wie,55\n"
                        + "Hello,Hallo Welt,55\n"
                        + "Hello world,Hallo Welt,55\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinOnACustomTypeInputWithKeyExtractorAndATupleInputWithKeyFieldSelector()
            throws Exception {
        /*
         * Join on a tuple input with key field selector and a custom type input with key extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds1 = CollectionDataSets.getSmallCustomTypeDataSet(env);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.join(ds2).where(new KeySelector1()).equalTo(0).with(new CustT3Join());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected = "Hi,Hi\n" + "Hello,Hello\n" + "Hello world,Hello\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector1 implements KeySelector<CustomType, Integer> {
        @Override
        public Integer getKey(CustomType value) {
            return value.myInt;
        }
    }

    @Test
    public void testProjectOnATuple1Input() throws Exception {
        /*
         * Project join on a tuple input 1
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple6<String, Long, String, Integer, Long, Long>> joinDs =
                ds1.join(ds2)
                        .where(1)
                        .equalTo(1)
                        .projectFirst(2, 1)
                        .projectSecond(3)
                        .projectFirst(0)
                        .projectSecond(4, 1);

        List<Tuple6<String, Long, String, Integer, Long, Long>> result = joinDs.collect();

        String expected =
                "Hi,1,Hallo,1,1,1\n"
                        + "Hello,2,Hallo Welt,2,2,2\n"
                        + "Hello world,2,Hallo Welt,3,2,2\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testProjectJoinOnATuple2Input() throws Exception {
        /*
         * Project join on a tuple input 2
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple6<String, String, Long, Long, Long, Integer>> joinDs =
                ds1.join(ds2)
                        .where(1)
                        .equalTo(1)
                        .projectSecond(3)
                        .projectFirst(2, 1)
                        .projectSecond(4, 1)
                        .projectFirst(0);

        List<Tuple6<String, String, Long, Long, Long, Integer>> result = joinDs.collect();

        String expected =
                "Hallo,Hi,1,1,1,1\n"
                        + "Hallo Welt,Hello,2,2,2,2\n"
                        + "Hallo Welt,Hello world,2,2,2,3\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinOnATupleInputWithKeyFieldSelectorAndACustomTypeInputWithKeyExtractor()
            throws Exception {
        /*
         * Join on a tuple input with key field selector and a custom type input with key extractor
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.join(ds2).where(1).equalTo(new KeySelector2()).with(new T3CustJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected = "Hi,Hello\n" + "Hello,Hello world\n" + "Hello world,Hello world\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector2 implements KeySelector<CustomType, Long> {
        @Override
        public Long getKey(CustomType value) {
            return value.myLong;
        }
    }

    @Test
    public void testDefaultJoinOnTwoCustomTypeInputsWithKeyExtractors() throws Exception {
        /*
         * (Default) Join on two custom type inputs with key extractors
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds1 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env);

        DataSet<Tuple2<CustomType, CustomType>> joinDs =
                ds1.join(ds2).where(new KeySelector5()).equalTo(new KeySelector6());

        List<Tuple2<CustomType, CustomType>> result = joinDs.collect();

        String expected =
                "1,0,Hi,1,0,Hi\n"
                        + "2,1,Hello,2,1,Hello\n"
                        + "2,1,Hello,2,2,Hello world\n"
                        + "2,2,Hello world,2,1,Hello\n"
                        + "2,2,Hello world,2,2,Hello world\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testDefaultJoinOnTwoCustomTypeInputsWithInnerClassKeyExtractorsClosureCleaner()
            throws Exception {
        /*
         * (Default) Join on two custom type inputs with key extractors, implemented as inner classes to test closure
         * cleaning
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<CustomType> ds1 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env);

        DataSet<Tuple2<CustomType, CustomType>> joinDs =
                ds1.join(ds2)
                        .where(
                                new KeySelector<CustomType, Integer>() {
                                    @Override
                                    public Integer getKey(CustomType value) {
                                        return value.myInt;
                                    }
                                })
                        .equalTo(
                                new KeySelector<CustomType, Integer>() {

                                    @Override
                                    public Integer getKey(CustomType value) throws Exception {
                                        return value.myInt;
                                    }
                                });

        List<Tuple2<CustomType, CustomType>> result = joinDs.collect();

        String expected =
                "1,0,Hi,1,0,Hi\n"
                        + "2,1,Hello,2,1,Hello\n"
                        + "2,1,Hello,2,2,Hello world\n"
                        + "2,2,Hello world,2,1,Hello\n"
                        + "2,2,Hello world,2,2,Hello world\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void
            testDefaultJoinOnTwoCustomTypeInputsWithInnerClassKeyExtractorsDisabledClosureCleaner()
                    throws Exception {
        /*
         * (Default) Join on two custom type inputs with key extractors, check if disabling closure cleaning works
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();

        DataSet<CustomType> ds1 = CollectionDataSets.getCustomTypeDataSet(env);
        DataSet<CustomType> ds2 = CollectionDataSets.getSmallCustomTypeDataSet(env);
        boolean correctExceptionTriggered = false;
        try {
            DataSet<Tuple2<CustomType, CustomType>> joinDs =
                    ds1.join(ds2)
                            .where(
                                    new KeySelector<CustomType, Integer>() {
                                        @Override
                                        public Integer getKey(CustomType value) {
                                            return value.myInt;
                                        }
                                    })
                            .equalTo(
                                    new KeySelector<CustomType, Integer>() {

                                        @Override
                                        public Integer getKey(CustomType value) throws Exception {
                                            return value.myInt;
                                        }
                                    });
        } catch (InvalidProgramException ex) {
            correctExceptionTriggered = (ex.getCause() instanceof java.io.NotSerializableException);
        }
        Assert.assertTrue(correctExceptionTriggered);
    }

    private static class KeySelector5 implements KeySelector<CustomType, Integer> {
        @Override
        public Integer getKey(CustomType value) {
            return value.myInt;
        }
    }

    private static class KeySelector6 implements KeySelector<CustomType, Integer> {
        @Override
        public Integer getKey(CustomType value) {
            return value.myInt;
        }
    }

    @Test
    public void testUDFJoinOnTuplesWithTupleReturningKeySelectors() throws Exception {
        /*
         * UDF Join on tuples with tuple-returning key selectors
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);
        DataSet<Tuple2<String, String>> joinDs =
                ds1.join(ds2)
                        .where(new KeySelector3())
                        .equalTo(new KeySelector4())
                        .with(new T3T5FlatJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected =
                "Hi,Hallo\n"
                        + "Hello,Hallo Welt\n"
                        + "Hello world,Hallo Welt wie gehts?\n"
                        + "Hello world,ABC\n"
                        + "I am fine.,HIJ\n"
                        + "I am fine.,IJK\n";

        compareResultAsTuples(result, expected);
    }

    private static class KeySelector3
            implements KeySelector<Tuple3<Integer, Long, String>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> getKey(Tuple3<Integer, Long, String> t) {
            return new Tuple2<Integer, Long>(t.f0, t.f1);
        }
    }

    private static class KeySelector4
            implements KeySelector<
                    Tuple5<Integer, Long, Integer, String, Long>, Tuple2<Integer, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Long> getKey(Tuple5<Integer, Long, Integer, String, Long> t) {
            return new Tuple2<Integer, Long>(t.f0, t.f4);
        }
    }

    @Test
    public void testJoinNestedPojoAgainstTupleSelectedUsingString() throws Exception {
        /*
         * Join nested pojo against tuple (selected using a string)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>>
                joinDs = ds1.join(ds2).where("nestedPojo.longNumber").equalTo("f6");

        List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result =
                joinDs.collect();

        String expected =
                "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n"
                        + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n"
                        + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinNestedPojoAgainstTupleSelectedUsingInteger() throws Exception {
        /*
         * Join nested pojo against tuple (selected as an integer)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>>
                joinDs =
                        ds1.join(ds2).where("nestedPojo.longNumber").equalTo(6); // <--- difference!

        List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result =
                joinDs.collect();

        String expected =
                "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n"
                        + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n"
                        + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testSelectingMultipleFieldsUsingExpressionLanguage() throws Exception {
        /*
         * selecting multiple fields using expression language
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>>
                joinDs =
                        ds1.join(ds2)
                                .where("nestedPojo.longNumber", "number", "str")
                                .equalTo("f6", "f0", "f1");

        env.setParallelism(1);
        List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result =
                joinDs.collect();

        String expected =
                "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n"
                        + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n"
                        + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testNestedIntoTuple() throws Exception {
        /*
         * nested into tuple
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>>
                joinDs =
                        ds1.join(ds2)
                                .where(
                                        "nestedPojo.longNumber",
                                        "number",
                                        "nestedTupleWithCustom.f0")
                                .equalTo("f6", "f0", "f2");

        env.setParallelism(1);
        List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result =
                joinDs.collect();

        String expected =
                "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n"
                        + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n"
                        + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testNestedIntoTupleIntoPojo() throws Exception {
        /*
         * nested into tuple into pojo
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Integer, String, Integer, Integer, Long, String, Long>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSet(env);
        DataSet<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>>
                joinDs =
                        ds1.join(ds2)
                                .where(
                                        "nestedTupleWithCustom.f0",
                                        "nestedTupleWithCustom.f1.myInt",
                                        "nestedTupleWithCustom.f1.myLong")
                                .equalTo("f2", "f3", "f4");

        env.setParallelism(1);
        List<Tuple2<POJO, Tuple7<Integer, String, Integer, Integer, Long, String, Long>>> result =
                joinDs.collect();

        String expected =
                "1 First (10,100,1000,One) 10000,(1,First,10,100,1000,One,10000)\n"
                        + "2 Second (20,200,2000,Two) 20000,(2,Second,20,200,2000,Two,20000)\n"
                        + "3 Third (30,300,3000,Three) 30000,(3,Third,30,300,3000,Three,30000)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testNonPojoToVerifyFullTupleKeys() throws Exception {
        /*
         * Non-POJO test to verify that full-tuple keys are working.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds1 =
                CollectionDataSets.getSmallNestedTupleDataSet(env);
        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds2 =
                CollectionDataSets.getSmallNestedTupleDataSet(env);
        DataSet<
                        Tuple2<
                                Tuple2<Tuple2<Integer, Integer>, String>,
                                Tuple2<Tuple2<Integer, Integer>, String>>>
                joinDs =
                        ds1.join(ds2)
                                .where(0)
                                .equalTo("f0.f0", "f0.f1"); // key is now Tuple2<Integer, Integer>

        env.setParallelism(1);
        List<
                        Tuple2<
                                Tuple2<Tuple2<Integer, Integer>, String>,
                                Tuple2<Tuple2<Integer, Integer>, String>>>
                result = joinDs.collect();

        String expected =
                "((1,1),one),((1,1),one)\n"
                        + "((2,2),two),((2,2),two)\n"
                        + "((3,3),three),((3,3),three)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testNonPojoToVerifyNestedTupleElementSelection() throws Exception {
        /*
         * Non-POJO test to verify "nested" tuple-element selection.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds1 =
                CollectionDataSets.getSmallNestedTupleDataSet(env);
        DataSet<Tuple2<Tuple2<Integer, Integer>, String>> ds2 =
                CollectionDataSets.getSmallNestedTupleDataSet(env);
        DataSet<
                        Tuple2<
                                Tuple2<Tuple2<Integer, Integer>, String>,
                                Tuple2<Tuple2<Integer, Integer>, String>>>
                joinDs =
                        ds1.join(ds2)
                                .where("f0.f0")
                                .equalTo("f0.f0"); // key is now Integer from Tuple2<Integer,
        // Integer>

        env.setParallelism(1);
        List<
                        Tuple2<
                                Tuple2<Tuple2<Integer, Integer>, String>,
                                Tuple2<Tuple2<Integer, Integer>, String>>>
                result = joinDs.collect();

        String expected =
                "((1,1),one),((1,1),one)\n"
                        + "((2,2),two),((2,2),two)\n"
                        + "((3,3),three),((3,3),three)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testFullPojoWithFullTuple() throws Exception {
        /*
         * full pojo with full tuple
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<POJO> ds1 = CollectionDataSets.getSmallPojoDataSet(env);
        DataSet<Tuple7<Long, Integer, Integer, Long, String, Integer, String>> ds2 =
                CollectionDataSets.getSmallTuplebasedDataSetMatchingPojo(env);
        DataSet<Tuple2<POJO, Tuple7<Long, Integer, Integer, Long, String, Integer, String>>>
                joinDs = ds1.join(ds2).where("*").equalTo("*");

        env.setParallelism(1);
        List<Tuple2<POJO, Tuple7<Long, Integer, Integer, Long, String, Integer, String>>> result =
                joinDs.collect();

        String expected =
                "1 First (10,100,1000,One) 10000,(10000,10,100,1000,One,1,First)\n"
                        + "2 Second (20,200,2000,Two) 20000,(20000,20,200,2000,Two,2,Second)\n"
                        + "3 Third (30,300,3000,Three) 30000,(30000,30,300,3000,Three,3,Third)\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testNonPojoToVerifyNestedTupleElementSelectionWithFirstKeyFieldGreaterThanZero()
            throws Exception {
        /*
         * Non-POJO test to verify "nested" tuple-element selection with the first key field greater than 0.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Tuple2<Tuple3<Integer, Long, String>, Tuple3<Integer, Long, String>>> ds2 =
                ds1.join(ds1).where(0).equalTo(0);
        DataSet<
                        Tuple2<
                                Tuple2<
                                        Tuple3<Integer, Long, String>,
                                        Tuple3<Integer, Long, String>>,
                                Tuple2<
                                        Tuple3<Integer, Long, String>,
                                        Tuple3<Integer, Long, String>>>>
                joinDs = ds2.join(ds2).where("f1.f0").equalTo("f0.f0");

        env.setParallelism(1);
        List<
                        Tuple2<
                                Tuple2<
                                        Tuple3<Integer, Long, String>,
                                        Tuple3<Integer, Long, String>>,
                                Tuple2<
                                        Tuple3<Integer, Long, String>,
                                        Tuple3<Integer, Long, String>>>>
                result = joinDs.collect();

        String expected =
                "((1,1,Hi),(1,1,Hi)),((1,1,Hi),(1,1,Hi))\n"
                        + "((2,2,Hello),(2,2,Hello)),((2,2,Hello),(2,2,Hello))\n"
                        + "((3,2,Hello world),(3,2,Hello world)),((3,2,Hello world),(3,2,Hello world))\n";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinWithAtomicType1() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.getSmall3TupleDataSet(env);
        DataSet<Integer> ds2 = env.fromElements(1, 2);

        DataSet<Tuple2<Tuple3<Integer, Long, String>, Integer>> joinDs =
                ds1.join(ds2).where(0).equalTo("*");

        List<Tuple2<Tuple3<Integer, Long, String>, Integer>> result = joinDs.collect();

        String expected = "(1,1,Hi),1\n" + "(2,2,Hello),2";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinWithAtomicType2() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> ds1 = env.fromElements(1, 2);
        DataSet<Tuple3<Integer, Long, String>> ds2 = CollectionDataSets.getSmall3TupleDataSet(env);

        DataSet<Tuple2<Integer, Tuple3<Integer, Long, String>>> joinDs =
                ds1.join(ds2).where("*").equalTo(0);

        List<Tuple2<Integer, Tuple3<Integer, Long, String>>> result = joinDs.collect();

        String expected = "1,(1,1,Hi)\n" + "2,(2,2,Hello)";

        compareResultAsTuples(result, expected);
    }

    @Test
    public void testJoinWithRangePartitioning() throws Exception {
        /*
         * Test Join on tuples with multiple key field positions and same customized distribution
         */

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Long, String>> ds1 = CollectionDataSets.get3TupleDataSet(env);
        DataSet<Tuple5<Integer, Long, Integer, String, Long>> ds2 =
                CollectionDataSets.get5TupleDataSet(env);

        env.setParallelism(4);
        TestDistribution testDis = new TestDistribution();
        DataSet<Tuple2<String, String>> joinDs =
                DataSetUtils.partitionByRange(ds1, testDis, 0, 1)
                        .join(DataSetUtils.partitionByRange(ds2, testDis, 0, 4))
                        .where(0, 1)
                        .equalTo(0, 4)
                        .with(new T3T5FlatJoin());

        List<Tuple2<String, String>> result = joinDs.collect();

        String expected =
                "Hi,Hallo\n"
                        + "Hello,Hallo Welt\n"
                        + "Hello world,Hallo Welt wie gehts?\n"
                        + "Hello world,ABC\n"
                        + "I am fine.,HIJ\n"
                        + "I am fine.,IJK\n";

        compareResultAsTuples(result, expected);
    }

    private static class T3T5FlatJoin
            implements FlatJoinFunction<
                    Tuple3<Integer, Long, String>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple2<String, String>> {

        @Override
        public void join(
                Tuple3<Integer, Long, String> first,
                Tuple5<Integer, Long, Integer, String, Long> second,
                Collector<Tuple2<String, String>> out) {

            out.collect(new Tuple2<String, String>(first.f2, second.f3));
        }
    }

    private static class LeftReturningJoin
            implements JoinFunction<
                    Tuple3<Integer, Long, String>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple3<Integer, Long, String>> {

        @Override
        public Tuple3<Integer, Long, String> join(
                Tuple3<Integer, Long, String> first,
                Tuple5<Integer, Long, Integer, String, Long> second) {

            return first;
        }
    }

    private static class RightReturningJoin
            implements JoinFunction<
                    Tuple3<Integer, Long, String>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple5<Integer, Long, Integer, String, Long>> {

        @Override
        public Tuple5<Integer, Long, Integer, String, Long> join(
                Tuple3<Integer, Long, String> first,
                Tuple5<Integer, Long, Integer, String, Long> second) {

            return second;
        }
    }

    private static class T3T5BCJoin
            extends RichFlatJoinFunction<
                    Tuple3<Integer, Long, String>,
                    Tuple5<Integer, Long, Integer, String, Long>,
                    Tuple3<String, String, Integer>> {

        private int broadcast;

        @Override
        public void open(OpenContext openContext) {

            Collection<Integer> ints = this.getRuntimeContext().getBroadcastVariable("ints");
            int sum = 0;
            for (Integer i : ints) {
                sum += i;
            }
            broadcast = sum;
        }

        /*
        @Override
        public Tuple3<String, String, Integer> join(
        		Tuple3<Integer, Long, String> first,
        		Tuple5<Integer, Long, Integer, String, Long> second) {

        	return new Tuple3<String, String, Integer>(first.f2, second.f3, broadcast);
        }
         */

        @Override
        public void join(
                Tuple3<Integer, Long, String> first,
                Tuple5<Integer, Long, Integer, String, Long> second,
                Collector<Tuple3<String, String, Integer>> out)
                throws Exception {
            out.collect(new Tuple3<String, String, Integer>(first.f2, second.f3, broadcast));
        }
    }

    private static class T3CustJoin
            implements JoinFunction<
                    Tuple3<Integer, Long, String>, CustomType, Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> join(Tuple3<Integer, Long, String> first, CustomType second) {

            return new Tuple2<String, String>(first.f2, second.myString);
        }
    }

    private static class CustT3Join
            implements JoinFunction<
                    CustomType, Tuple3<Integer, Long, String>, Tuple2<String, String>> {

        @Override
        public Tuple2<String, String> join(CustomType first, Tuple3<Integer, Long, String> second) {

            return new Tuple2<String, String>(first.myString, second.f2);
        }
    }

    /** Test data distribution. */
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
