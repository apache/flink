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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.checkAndExtractLambda;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the type extractor for lambda functions. Many tests only work if the compiler supports
 * lambdas properly otherwise a MissingTypeInfo is returned.
 */
class LambdaExtractionTest {

    private static final TypeInformation<Tuple2<Tuple1<Integer>, Boolean>>
            NESTED_TUPLE_BOOLEAN_TYPE =
                    new TypeHint<Tuple2<Tuple1<Integer>, Boolean>>() {}.getTypeInfo();

    private static final TypeInformation<Tuple2<Tuple1<Integer>, Double>> NESTED_TUPLE_DOUBLE_TYPE =
            new TypeHint<Tuple2<Tuple1<Integer>, Double>>() {}.getTypeInfo();

    @Test
    @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
    void testIdentifyLambdas() throws TypeExtractionException {
        MapFunction<?, ?> anonymousFromInterface =
                new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) {
                        return Integer.parseInt(value);
                    }
                };

        MapFunction<?, ?> anonymousFromClass =
                new RichMapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) {
                        return Integer.parseInt(value);
                    }
                };

        MapFunction<?, ?> fromProperClass = new StaticMapper();

        MapFunction<?, ?> fromDerived =
                new ToTuple<Integer>() {
                    @Override
                    public Tuple2<Integer, Long> map(Integer value) {
                        return new Tuple2<>(value, 1L);
                    }
                };

        MapFunction<String, Integer> staticLambda = Integer::parseInt;
        MapFunction<Integer, String> instanceLambda = Object::toString;
        MapFunction<String, Integer> constructorLambda = Integer::new;

        assertThat(checkAndExtractLambda(anonymousFromInterface)).isNull();
        assertThat(checkAndExtractLambda(anonymousFromClass)).isNull();
        assertThat(checkAndExtractLambda(fromProperClass)).isNull();
        assertThat(checkAndExtractLambda(fromDerived)).isNull();
        assertThat(checkAndExtractLambda(staticLambda)).isNotNull();
        assertThat(checkAndExtractLambda(instanceLambda)).isNotNull();
        assertThat(checkAndExtractLambda(constructorLambda)).isNotNull();
        assertThat(checkAndExtractLambda(STATIC_LAMBDA)).isNotNull();
    }

    private static class StaticMapper implements MapFunction<String, Integer> {
        @Override
        public Integer map(String value) {
            return Integer.parseInt(value);
        }
    }

    private interface ToTuple<T> extends MapFunction<T, Tuple2<T, Long>> {
        @Override
        Tuple2<T, Long> map(T value) throws Exception;
    }

    private static final MapFunction<String, Integer> STATIC_LAMBDA = Integer::parseInt;

    private static class MyClass {
        private final String s = "mystring";

        public MapFunction<Integer, String> getMapFunction() {
            return (i) -> s;
        }
    }

    @Test
    void testLambdaWithMemberVariable() {
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(new MyClass().getMapFunction(), Types.INT);
        assertThat(ti).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Test
    void testLambdaWithLocalVariable() {
        String s = "mystring";
        final int k = 24;
        int j = 26;

        MapFunction<Integer, String> f = (i) -> s + k + j;

        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(f, Types.INT);
        assertThat(ti).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Test
    void testLambdaWithNonGenericResultType() {
        MapFunction<Tuple2<Tuple1<Integer>, Boolean>, Boolean> f = (i) -> null;

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(f, NESTED_TUPLE_BOOLEAN_TYPE, null, true);
        assertThat(ti).isInstanceOf(BasicTypeInfo.class);
        assertThat(ti).isEqualTo(BasicTypeInfo.BOOLEAN_TYPE_INFO);
    }

    @Test
    void testMapLambda() {
        MapFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f =
                (i) -> null;

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(f, NESTED_TUPLE_BOOLEAN_TYPE, null, true);
        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType()).isTrue();
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    @Test
    void testFlatMapLambda() {
        FlatMapFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f =
                (i, out) -> out.collect(null);

        TypeInformation<?> ti =
                TypeExtractor.getFlatMapReturnTypes(f, NESTED_TUPLE_BOOLEAN_TYPE, null, true);
        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType()).isTrue();
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    @Test
    void testMapPartitionLambda() {
        MapPartitionFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f =
                (i, o) -> {};

        TypeInformation<?> ti =
                TypeExtractor.getMapPartitionReturnTypes(f, NESTED_TUPLE_BOOLEAN_TYPE, null, true);
        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType()).isTrue();
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    @Test
    void testJoinLambda() {
        JoinFunction<
                        Tuple2<Tuple1<Integer>, Boolean>,
                        Tuple2<Tuple1<Integer>, Double>,
                        Tuple2<Tuple1<Integer>, String>>
                f = (i1, i2) -> null;

        TypeInformation<?> ti =
                TypeExtractor.getJoinReturnTypes(
                        f, NESTED_TUPLE_BOOLEAN_TYPE, NESTED_TUPLE_DOUBLE_TYPE, null, true);
        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType()).isTrue();
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    @Test
    void testCoGroupLambda() {
        CoGroupFunction<
                        Tuple2<Tuple1<Integer>, Boolean>,
                        Tuple2<Tuple1<Integer>, Double>,
                        Tuple2<Tuple1<Integer>, String>>
                f = (i1, i2, o) -> {};

        TypeInformation<?> ti =
                TypeExtractor.getCoGroupReturnTypes(
                        f, NESTED_TUPLE_BOOLEAN_TYPE, NESTED_TUPLE_DOUBLE_TYPE, null, true);
        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType()).isTrue();
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    @Test
    void testKeySelectorLambda() {
        KeySelector<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f =
                (i) -> null;

        TypeInformation<?> ti =
                TypeExtractor.getKeySelectorTypes(f, NESTED_TUPLE_BOOLEAN_TYPE, null, true);
        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0).isTupleType()).isTrue();
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    @SuppressWarnings("rawtypes")
    @Test
    void testLambdaTypeErasure() {
        MapFunction<Tuple1<Integer>, Tuple1> f = (i) -> null;
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        f, new TypeHint<Tuple1<Integer>>() {}.getTypeInfo(), null, true);
        assertThat(ti).isInstanceOf(MissingTypeInfo.class);
    }

    @Test
    void testLambdaWithoutTypeErasure() {
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        Tuple1::of, BasicTypeInfo.STRING_TYPE_INFO, null, true);
        assertThat(ti).isInstanceOf(MissingTypeInfo.class);
    }

    @Test
    void testPartitionerLambda() {
        Partitioner<Tuple2<Integer, String>> partitioner =
                (key, numPartitions) -> key.f1.length() % numPartitions;
        final TypeInformation<?> ti = TypeExtractor.getPartitionerTypes(partitioner, null, true);

        if (!(ti instanceof MissingTypeInfo)) {
            assertThat(ti.isTupleType()).isTrue();
            assertThat(ti.getArity()).isEqualTo(2);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            assertThat(((TupleTypeInfo<?>) ti).getTypeAt(1))
                    .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        }
    }

    private static class MyType {
        private int key;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        protected int getKey2() {
            return 0;
        }
    }

    @Test
    void testInstanceMethodRefSameType() {
        MapFunction<MyType, Integer> f = MyType::getKey;
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(f, TypeExtractor.createTypeInfo(MyType.class));
        assertThat(ti).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    @Test
    void testInstanceMethodRefSuperType() {
        MapFunction<Integer, String> f = Object::toString;
        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(f, BasicTypeInfo.INT_TYPE_INFO);
        assertThat(ti).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
    }

    private static class MySubtype extends MyType {
        public boolean test;
    }

    @Test
    void testInstanceMethodRefSuperTypeProtected() {
        MapFunction<MySubtype, Integer> f = MyType::getKey2;
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(f, TypeExtractor.createTypeInfo(MySubtype.class));
        assertThat(ti).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    @Test
    void testConstructorMethodRef() {
        MapFunction<String, Integer> f = Integer::new;
        TypeInformation<?> ti = TypeExtractor.getMapReturnTypes(f, BasicTypeInfo.STRING_TYPE_INFO);
        assertThat(ti).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    private interface InterfaceWithDefaultMethod {
        void samMethod();

        default void defaultMethod() {}
    }

    @Test
    void testSamMethodExtractionInterfaceWithDefaultMethod() {
        final Method sam =
                TypeExtractionUtils.getSingleAbstractMethod(InterfaceWithDefaultMethod.class);
        assertThat(sam).isNotNull();
        assertThat(sam.getName()).isEqualTo("samMethod");
    }

    private interface InterfaceWithMultipleMethods {
        void firstMethod();

        void secondMethod();
    }

    @Test
    void getSingleAbstractMethodMultipleMethods() {
        assertThatThrownBy(
                        () ->
                                TypeExtractionUtils.getSingleAbstractMethod(
                                        InterfaceWithMultipleMethods.class))
                .isInstanceOf(InvalidTypesException.class);
    }

    private interface InterfaceWithoutAbstractMethod {
        default void defaultMethod() {}
    }

    @Test
    void testSingleAbstractMethodNoAbstractMethods() {
        assertThatThrownBy(
                        () ->
                                TypeExtractionUtils.getSingleAbstractMethod(
                                        InterfaceWithoutAbstractMethod.class))
                .isInstanceOf(InvalidTypesException.class);
    }

    private abstract class AbstractClassWithSingleAbstractMethod {
        public abstract void defaultMethod();
    }

    @Test
    void testSingleAbstractMethodNotAnInterface() {
        assertThatThrownBy(
                        () ->
                                TypeExtractionUtils.getSingleAbstractMethod(
                                        AbstractClassWithSingleAbstractMethod.class))
                .isInstanceOf(InvalidTypesException.class);
    }
}
