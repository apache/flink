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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Pojo Type tests.
 *
 * <p>Pojo is a bean-style class with getters, setters and empty ctor OR a class with all fields
 * public (or for every private field, there has to be a public getter/setter) everything else is a
 * generic type (that can't be used for field selection)
 */
public class PojoTypeExtractionTest {

    /** Simple test type that implements the {@link Value} interface. */
    public static class MyValue implements Value {
        private static final long serialVersionUID = 8607223484689147046L;

        @Override
        public void write(DataOutputView out) throws IOException {}

        @Override
        public void read(DataInputView in) throws IOException {}
    }

    public static class HasDuplicateField extends WC {
        @SuppressWarnings("unused")
        private int count; // duplicate
    }

    @Test
    void testDuplicateFieldException() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(HasDuplicateField.class);
        assertThat(ti).isInstanceOf(GenericTypeInfo.class);
    }

    // test with correct pojo types
    public static class WC { // is a pojo
        public ComplexNestedClass complex; // is a pojo
        private int count; // is a BasicType

        public WC() {}

        public int getCount() {
            return count;
        }

        public void setCount(int c) {
            this.count = c;
        }
    }

    public static class ComplexNestedClass { // pojo
        public static int ignoreStaticField;
        public transient int ignoreTransientField;
        public Date date; // generic type
        public Integer someNumberWithÜnicödeNäme; // BasicType
        public float someFloat; // BasicType
        public Tuple3<Long, Long, String> word; // Tuple Type with three basic types
        public Object nothing; // generic type
        public MyValue valueType; // writableType
        public List<String> collection;
    }

    // all public test
    public static class AllPublic extends ComplexNestedClass {
        public ArrayList<String> somethingFancy; // generic type
        public FancyCollectionSubtype<Integer> fancyIds; // generic type
        public String[] fancyArray; // generic type
    }

    public static class FancyCollectionSubtype<T> extends HashSet<T> {
        private static final long serialVersionUID = -3494469602638179921L;
    }

    public static class ParentSettingGenerics extends PojoWithGenerics<Integer, Long> {
        public String field3;
    }

    public static class PojoWithGenerics<T1, T2> {
        public int key;
        public T1 field1;
        public T2 field2;
    }

    public static class ComplexHierarchyTop extends ComplexHierarchy<Tuple1<String>> {}

    public static class ComplexHierarchy<T> extends PojoWithGenerics<FromTuple, T> {}

    // extends from Tuple and adds a field
    public static class FromTuple extends Tuple3<String, String, Long> {
        private static final long serialVersionUID = 1L;
        public int special;
    }

    public static class IncorrectPojo {
        private int isPrivate;

        public int getIsPrivate() {
            return isPrivate;
        }
        // setter is missing (intentional)
    }

    // correct pojo
    public static class BeanStylePojo {
        public String abc;
        private int field;

        public int getField() {
            return this.field;
        }

        public void setField(int f) {
            this.field = f;
        }
    }

    public static class WrongCtorPojo {
        public int a;

        public WrongCtorPojo(int a) {
            this.a = a;
        }
    }

    public static class PojoWithGenericFields {
        private Collection<String> users;
        private boolean favorited;

        public boolean isFavorited() {
            return favorited;
        }

        public void setFavorited(boolean favorited) {
            this.favorited = favorited;
        }

        public Collection<String> getUsers() {
            return users;
        }

        public void setUsers(Collection<String> users) {
            this.users = users;
        }
    }

    @Test
    void testPojoWithGenericFields() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(PojoWithGenericFields.class);

        assertThat(typeForClass).isInstanceOf(PojoTypeInfo.class);
    }

    // in this test, the location of the getters and setters is mixed across the type hierarchy.
    public static class TypedPojoGetterSetterCheck extends GenericPojoGetterSetterCheck<String> {
        public void setPackageProtected(String in) {
            this.packageProtected = in;
        }
    }

    public static class GenericPojoGetterSetterCheck<T> {
        T packageProtected;

        public T getPackageProtected() {
            return packageProtected;
        }
    }

    @Test
    void testIncorrectPojos() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(IncorrectPojo.class);
        assertThat(typeForClass).isInstanceOf(GenericTypeInfo.class);

        typeForClass = TypeExtractor.createTypeInfo(WrongCtorPojo.class);
        assertThat(typeForClass).isInstanceOf(GenericTypeInfo.class);
    }

    @Test
    void testCorrectPojos() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(BeanStylePojo.class);
        assertThat(typeForClass).isInstanceOf(PojoTypeInfo.class);

        typeForClass = TypeExtractor.createTypeInfo(TypedPojoGetterSetterCheck.class);
        assertThat(typeForClass).isInstanceOf(PojoTypeInfo.class);
    }

    @Test
    void testPojoWC() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(WC.class);
        checkWCPojoAsserts(typeForClass);

        WC t = new WC();
        t.complex = new ComplexNestedClass();
        TypeInformation<?> typeForObject = TypeExtractor.getForObject(t);
        checkWCPojoAsserts(typeForObject);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void checkWCPojoAsserts(TypeInformation<?> typeInfo) {
        assertThat(typeInfo.isBasicType()).isFalse();
        assertThat(typeInfo.isTupleType()).isFalse();
        assertThat(typeInfo.getTotalFields()).isEqualTo(10);
        assertThat(typeInfo).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<?> pojoType = (PojoTypeInfo<?>) typeInfo;

        List<FlatFieldDescriptor> ffd = new ArrayList<FlatFieldDescriptor>();
        String[] fields = {
            "count",
            "complex.date",
            "complex.collection",
            "complex.nothing",
            "complex.someFloat",
            "complex.someNumberWithÜnicödeNäme",
            "complex.valueType",
            "complex.word.f0",
            "complex.word.f1",
            "complex.word.f2"
        };
        int[] positions = {9, 1, 0, 2, 3, 4, 5, 6, 7, 8};
        assertThat(fields).hasSameSizeAs(positions);
        for (int i = 0; i < fields.length; i++) {
            pojoType.getFlatFields(fields[i], 0, ffd);
            assertThat(ffd).as("Too many keys returned").hasSize(1);
            assertThat(ffd.get(0).getPosition())
                    .as("position of field " + fields[i] + " wrong")
                    .isEqualTo(positions[i]);
            ffd.clear();
        }

        pojoType.getFlatFields("complex.word.*", 0, ffd);
        assertThat(ffd).hasSize(3);
        // check if it returns 5,6,7
        for (FlatFieldDescriptor ffdE : ffd) {
            final int pos = ffdE.getPosition();
            assertThat(pos).isGreaterThanOrEqualTo(6).isLessThanOrEqualTo(8);
            if (pos == 6) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Long.class);
            }
            if (pos == 7) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Long.class);
            }
            if (pos == 8) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(String.class);
            }
        }
        ffd.clear();

        // scala style full tuple selection for pojos
        pojoType.getFlatFields("complex.word._", 0, ffd);
        assertThat(ffd).hasSize(3);
        ffd.clear();

        pojoType.getFlatFields("complex.*", 0, ffd);
        assertThat(ffd).hasSize(9);
        // check if it returns 0-7
        for (FlatFieldDescriptor ffdE : ffd) {
            final int pos = ffdE.getPosition();
            assertThat(ffdE.getPosition()).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(8);
            if (pos == 0) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(List.class);
            }
            if (pos == 1) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Date.class);
            }
            if (pos == 2) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Object.class);
            }
            if (pos == 3) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Float.class);
            }
            if (pos == 4) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Integer.class);
            }
            if (pos == 5) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(MyValue.class);
            }
            if (pos == 6) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Long.class);
            }
            if (pos == 7) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Long.class);
            }
            if (pos == 8) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(String.class);
            }
            if (pos == 9) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Integer.class);
            }
        }
        ffd.clear();

        pojoType.getFlatFields("*", 0, ffd);
        assertThat(ffd).hasSize(10);
        // check if it returns 0-8
        for (FlatFieldDescriptor ffdE : ffd) {
            assertThat(ffdE.getPosition() <= 9).isTrue();
            assertThat(0 <= ffdE.getPosition()).isTrue();
            if (ffdE.getPosition() == 9) {
                assertThat(ffdE.getType().getTypeClass()).isEqualTo(Integer.class);
            }
        }
        ffd.clear();

        TypeInformation<?> typeComplexNested = pojoType.getTypeAt(0); // ComplexNestedClass complex
        assertThat(typeComplexNested).isInstanceOf(PojoTypeInfo.class);

        assertThat(typeComplexNested.getArity()).isEqualTo(7);
        assertThat(typeComplexNested.getTotalFields()).isEqualTo(9);
        PojoTypeInfo<?> pojoTypeComplexNested = (PojoTypeInfo<?>) typeComplexNested;

        boolean dateSeen = false,
                intSeen = false,
                floatSeen = false,
                tupleSeen = false,
                objectSeen = false,
                writableSeen = false,
                collectionSeen = false;
        for (int i = 0; i < pojoTypeComplexNested.getArity(); i++) {
            PojoField field = pojoTypeComplexNested.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("date")) {
                if (dateSeen) {
                    fail("already seen");
                }
                dateSeen = true;
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.DATE_TYPE_INFO);
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(Date.class);
            } else if (name.equals("someNumberWithÜnicödeNäme")) {
                if (intSeen) {
                    fail("already seen");
                }
                intSeen = true;
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(Integer.class);
            } else if (name.equals("someFloat")) {
                if (floatSeen) {
                    fail("already seen");
                }
                floatSeen = true;
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.FLOAT_TYPE_INFO);
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(Float.class);
            } else if (name.equals("word")) {
                if (tupleSeen) {
                    fail("already seen");
                }
                tupleSeen = true;
                assertThat(field.getTypeInformation() instanceof TupleTypeInfo<?>).isTrue();
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(Tuple3.class);
                // do some more advanced checks on the tuple
                TupleTypeInfo<?> tupleTypeFromComplexNested =
                        (TupleTypeInfo<?>) field.getTypeInformation();
                assertThat(tupleTypeFromComplexNested.getTypeAt(0))
                        .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
                assertThat(tupleTypeFromComplexNested.getTypeAt(1))
                        .isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
                assertThat(tupleTypeFromComplexNested.getTypeAt(2))
                        .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            } else if (name.equals("nothing")) {
                if (objectSeen) {
                    fail("already seen");
                }
                objectSeen = true;
                assertThat(field.getTypeInformation())
                        .isEqualTo(new GenericTypeInfo<Object>(Object.class));
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(Object.class);
            } else if (name.equals("valueType")) {
                if (writableSeen) {
                    fail("already seen");
                }
                writableSeen = true;
                assertThat(field.getTypeInformation())
                        .isEqualTo(new ValueTypeInfo<>(MyValue.class));
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(MyValue.class);
            } else if (name.equals("collection")) {
                if (collectionSeen) {
                    fail("already seen");
                }
                collectionSeen = true;
                assertThat(field.getTypeInformation()).isEqualTo(new GenericTypeInfo(List.class));

            } else {
                fail("Unexpected field " + field);
            }
        }
        assertThat(dateSeen).as("Field was not present").isTrue();
        assertThat(intSeen).as("Field was not present").isTrue();
        assertThat(floatSeen).as("Field was not present").isTrue();
        assertThat(tupleSeen).as("Field was not present").isTrue();
        assertThat(objectSeen).as("Field was not present").isTrue();
        assertThat(writableSeen).as("Field was not present").isTrue();
        assertThat(collectionSeen).as("Field was not present").isTrue();

        TypeInformation<?> typeAtOne = pojoType.getTypeAt(1); // int count
        assertThat(typeAtOne).isInstanceOf(BasicTypeInfo.class);

        assertThat(typeInfo.getTypeClass()).isEqualTo(WC.class);
        assertThat(typeInfo.getArity()).isEqualTo(2);
    }

    @Test
    void testPojoAllPublic() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(AllPublic.class);
        checkAllPublicAsserts(typeForClass);

        TypeInformation<?> typeForObject = TypeExtractor.getForObject(new AllPublic());
        checkAllPublicAsserts(typeForObject);
    }

    private void checkAllPublicAsserts(TypeInformation<?> typeInformation) {
        assertThat(typeInformation).isInstanceOf(PojoTypeInfo.class);
        assertThat(typeInformation.getArity()).isEqualTo(10);
        assertThat(typeInformation.getTotalFields()).isEqualTo(12);
        // check if the three additional fields are identified correctly
        boolean arrayListSeen = false, multisetSeen = false, strArraySeen = false;
        PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeInformation;
        for (int i = 0; i < pojoTypeForClass.getArity(); i++) {
            PojoField field = pojoTypeForClass.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("somethingFancy")) {
                if (arrayListSeen) {
                    fail("already seen");
                }
                arrayListSeen = true;
                assertThat(field.getTypeInformation() instanceof GenericTypeInfo).isTrue();
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(ArrayList.class);
            } else if (name.equals("fancyIds")) {
                if (multisetSeen) {
                    fail("already seen");
                }
                multisetSeen = true;
                assertThat(field.getTypeInformation() instanceof GenericTypeInfo).isTrue();
                assertThat(field.getTypeInformation().getTypeClass())
                        .isEqualTo(FancyCollectionSubtype.class);
            } else if (name.equals("fancyArray")) {
                if (strArraySeen) {
                    fail("already seen");
                }
                strArraySeen = true;
                assertThat(field.getTypeInformation())
                        .isEqualTo(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO);
                assertThat(field.getTypeInformation().getTypeClass()).isEqualTo(String[].class);
            } else if (Arrays.asList(
                            "date",
                            "someNumberWithÜnicödeNäme",
                            "someFloat",
                            "word",
                            "nothing",
                            "valueType",
                            "collection")
                    .contains(name)) {
                // ignore these, they are inherited from the ComplexNestedClass
            } else {
                fail("Unexpected field " + field);
            }
        }
        assertThat(arrayListSeen).as("Field was not present").isTrue();
        assertThat(multisetSeen).as("Field was not present").isTrue();
        assertThat(strArraySeen).as("Field was not present").isTrue();
    }

    @Test
    void testPojoExtendingTuple() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(FromTuple.class);
        checkFromTuplePojo(typeForClass);

        FromTuple ft = new FromTuple();
        ft.f0 = "";
        ft.f1 = "";
        ft.f2 = 0L;
        TypeInformation<?> typeForObject = TypeExtractor.getForObject(ft);
        checkFromTuplePojo(typeForObject);
    }

    private void checkFromTuplePojo(TypeInformation<?> typeInformation) {
        assertThat(typeInformation).isInstanceOf(PojoTypeInfo.class);
        assertThat(typeInformation.getTotalFields()).isEqualTo(4);
        PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeInformation;
        for (int i = 0; i < pojoTypeForClass.getArity(); i++) {
            PojoField field = pojoTypeForClass.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("special")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            } else if (name.equals("f0") || name.equals("f1")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            } else if (name.equals("f2")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
            } else {
                fail("unexpected field");
            }
        }
    }

    @Test
    void testPojoWithGenerics() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(ParentSettingGenerics.class);
        assertThat(typeForClass).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
        for (int i = 0; i < pojoTypeForClass.getArity(); i++) {
            PojoField field = pojoTypeForClass.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("field1")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            } else if (name.equals("field2")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
            } else if (name.equals("field3")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            } else if (name.equals("key")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            } else {
                fail("Unexpected field " + field);
            }
        }
    }

    /** Test if the TypeExtractor is accepting untyped generics, making them GenericTypes */
    @Test
    void testPojoWithGenericsSomeFieldsGeneric() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(PojoWithGenerics.class);
        assertThat(typeForClass).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
        for (int i = 0; i < pojoTypeForClass.getArity(); i++) {
            PojoField field = pojoTypeForClass.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("field1")) {
                assertThat(field.getTypeInformation())
                        .isEqualTo(new GenericTypeInfo<Object>(Object.class));
            } else if (name.equals("field2")) {
                assertThat(field.getTypeInformation())
                        .isEqualTo(new GenericTypeInfo<Object>(Object.class));
            } else if (name.equals("key")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            } else {
                fail("Unexpected field " + field);
            }
        }
    }

    @Test
    void testPojoWithComplexHierarchy() {
        TypeInformation<?> typeForClass = TypeExtractor.createTypeInfo(ComplexHierarchyTop.class);
        assertThat(typeForClass).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<?> pojoTypeForClass = (PojoTypeInfo<?>) typeForClass;
        for (int i = 0; i < pojoTypeForClass.getArity(); i++) {
            PojoField field = pojoTypeForClass.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("field1")) {
                assertThat(field.getTypeInformation() instanceof PojoTypeInfo<?>)
                        .isTrue(); // From tuple is pojo (not tuple type!)
            } else if (name.equals("field2")) {
                assertThat(field.getTypeInformation() instanceof TupleTypeInfo<?>).isTrue();
                assertThat(((TupleTypeInfo<?>) field.getTypeInformation()).getTypeAt(0))
                        .isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            } else if (name.equals("key")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            } else {
                fail("Unexpected field " + field);
            }
        }
    }

    public static class MyMapper<T>
            implements MapFunction<PojoWithGenerics<Long, T>, PojoWithGenerics<T, T>> {
        private static final long serialVersionUID = 1L;

        @Override
        public PojoWithGenerics<T, T> map(PojoWithGenerics<Long, T> value) throws Exception {
            return null;
        }
    }

    @Test
    void testGenericPojoTypeInference1() {
        MyMapper<String> function = new MyMapper<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(new TypeHint<PojoWithGenerics<Long, String>>() {}));

        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<?> pti = (PojoTypeInfo<?>) ti;
        for (int i = 0; i < pti.getArity(); i++) {
            PojoField field = pti.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("field1")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            } else if (name.equals("field2")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
            } else if (name.equals("key")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
            } else {
                fail("Unexpected field " + field);
            }
        }
    }

    public static class PojoTuple<A, B, C> extends Tuple3<B, C, Long> {
        private static final long serialVersionUID = 1L;

        public A extraField;
    }

    public static class MyMapper2<D, E> implements MapFunction<Tuple2<E, D>, PojoTuple<E, D, D>> {
        private static final long serialVersionUID = 1L;

        @Override
        public PojoTuple<E, D, D> map(Tuple2<E, D> value) throws Exception {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericPojoTypeInference2() {
        MyMapper2<Boolean, Character> function = new MyMapper2<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(new TypeHint<Tuple2<Character, Boolean>>() {}));

        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        PojoTypeInfo<?> pti = (PojoTypeInfo<?>) ti;
        for (int i = 0; i < pti.getArity(); i++) {
            PojoField field = pti.getPojoFieldAt(i);
            String name = field.getField().getName();
            if (name.equals("extraField")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.CHAR_TYPE_INFO);
            } else if (name.equals("f0")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.BOOLEAN_TYPE_INFO);
            } else if (name.equals("f1")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.BOOLEAN_TYPE_INFO);
            } else if (name.equals("f2")) {
                assertThat(field.getTypeInformation()).isEqualTo(BasicTypeInfo.LONG_TYPE_INFO);
            } else {
                fail("Unexpected field " + field);
            }
        }
    }

    public static class MyMapper3<D, E> implements MapFunction<PojoTuple<E, D, D>, Tuple2<E, D>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<E, D> map(PojoTuple<E, D, D> value) throws Exception {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericPojoTypeInference3() {
        MyMapper3<Boolean, Character> function = new MyMapper3<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(
                                new TypeHint<PojoTuple<Character, Boolean, Boolean>>() {}));

        assertThat(ti).isInstanceOf(TupleTypeInfo.class);
        TupleTypeInfo<?> tti = (TupleTypeInfo<?>) ti;
        assertThat(tti.getTypeAt(0)).isEqualTo(BasicTypeInfo.CHAR_TYPE_INFO);
        assertThat(tti.getTypeAt(1)).isEqualTo(BasicTypeInfo.BOOLEAN_TYPE_INFO);
    }

    public static class PojoWithParameterizedFields1<Z> {
        public Tuple2<Z, Z> field;
    }

    public static class MyMapper4<A> implements MapFunction<PojoWithParameterizedFields1<A>, A> {
        private static final long serialVersionUID = 1L;

        @Override
        public A map(PojoWithParameterizedFields1<A> value) throws Exception {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericPojoTypeInference4() {
        MyMapper4<Byte> function = new MyMapper4<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(new TypeHint<PojoWithParameterizedFields1<Byte>>() {}));
        assertThat(ti).isEqualTo(BasicTypeInfo.BYTE_TYPE_INFO);
    }

    public static class PojoWithParameterizedFields2<Z> {
        public PojoWithGenerics<Z, Z> field;
    }

    public static class MyMapper5<A> implements MapFunction<PojoWithParameterizedFields2<A>, A> {
        private static final long serialVersionUID = 1L;

        @Override
        public A map(PojoWithParameterizedFields2<A> value) throws Exception {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericPojoTypeInference5() {
        MyMapper5<Byte> function = new MyMapper5<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(new TypeHint<PojoWithParameterizedFields2<Byte>>() {}));
        assertThat(ti).isEqualTo(BasicTypeInfo.BYTE_TYPE_INFO);
    }

    public static class PojoWithParameterizedFields3<Z> {
        public Z[] field;
    }

    public static class MyMapper6<A> implements MapFunction<PojoWithParameterizedFields3<A>, A> {
        private static final long serialVersionUID = 1L;

        @Override
        public A map(PojoWithParameterizedFields3<A> value) throws Exception {
            return null;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericPojoTypeInference6() {
        MyMapper6<Integer> function = new MyMapper6<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(
                                new TypeHint<PojoWithParameterizedFields3<Integer>>() {}));
        assertThat(ti).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    public static class MyMapper7<A> implements MapFunction<PojoWithParameterizedFields4<A>, A> {
        private static final long serialVersionUID = 1L;

        @Override
        public A map(PojoWithParameterizedFields4<A> value) throws Exception {
            return null;
        }
    }

    public static class PojoWithParameterizedFields4<Z> {
        public Tuple1<Z>[] field;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void testGenericPojoTypeInference7() {
        MyMapper7<Integer> function = new MyMapper7<>();

        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function,
                        TypeInformation.of(
                                new TypeHint<PojoWithParameterizedFields4<Integer>>() {}));

        assertThat(ti).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    public static class RecursivePojo1 {
        public RecursivePojo1 field;
    }

    public static class RecursivePojo2 {
        public Tuple1<RecursivePojo2> field;
    }

    public static class RecursivePojo3 {
        public NestedPojo field;
    }

    public static class NestedPojo {
        public RecursivePojo3 field;
    }

    @Test
    void testRecursivePojo1() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(RecursivePojo1.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        assertThat(((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation().getClass())
                .isEqualTo(GenericTypeInfo.class);
    }

    @Test
    void testRecursivePojo2() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(RecursivePojo2.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        PojoField pf = ((PojoTypeInfo) ti).getPojoFieldAt(0);
        assertThat(pf.getTypeInformation() instanceof TupleTypeInfo).isTrue();
        assertThat(((TupleTypeInfo) pf.getTypeInformation()).getTypeAt(0).getClass())
                .isEqualTo(GenericTypeInfo.class);
    }

    @Test
    void testRecursivePojo3() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(RecursivePojo3.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        PojoField pf = ((PojoTypeInfo) ti).getPojoFieldAt(0);
        assertThat(pf.getTypeInformation() instanceof PojoTypeInfo).isTrue();
        assertThat(
                        ((PojoTypeInfo) pf.getTypeInformation())
                                .getPojoFieldAt(0)
                                .getTypeInformation()
                                .getClass())
                .isEqualTo(GenericTypeInfo.class);
    }

    public static class FooBarPojo {
        public int foo, bar;

        public FooBarPojo() {}
    }

    public static class DuplicateMapper
            implements MapFunction<FooBarPojo, Tuple2<FooBarPojo, FooBarPojo>> {
        @Override
        public Tuple2<FooBarPojo, FooBarPojo> map(FooBarPojo value) throws Exception {
            return null;
        }
    }

    @Test
    void testDualUseOfPojo() {
        MapFunction<?, ?> function = new DuplicateMapper();
        TypeInformation<?> ti =
                TypeExtractor.getMapReturnTypes(
                        function, (TypeInformation) TypeExtractor.createTypeInfo(FooBarPojo.class));
        assertThat(ti).isInstanceOf(TupleTypeInfo.class);
        TupleTypeInfo<?> tti = ((TupleTypeInfo) ti);
        assertThat(tti.getTypeAt(0) instanceof PojoTypeInfo).isTrue();
        assertThat(tti.getTypeAt(1) instanceof PojoTypeInfo).isTrue();
    }

    public static class PojoWithRecursiveGenericField<K, V> {
        public PojoWithRecursiveGenericField<K, V> parent;

        public PojoWithRecursiveGenericField() {}
    }

    @Test
    void testPojoWithRecursiveGenericField() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(PojoWithRecursiveGenericField.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        assertThat(((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation().getClass())
                .isEqualTo(GenericTypeInfo.class);
    }

    public static class MutualPojoA {
        public MutualPojoB field;
    }

    public static class MutualPojoB {
        public MutualPojoA field;
    }

    @Test
    void testPojosWithMutualRecursion() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(MutualPojoB.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        TypeInformation<?> pti = ((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation();
        assertThat(pti).isInstanceOf(PojoTypeInfo.class);
        assertThat(((PojoTypeInfo) pti).getPojoFieldAt(0).getTypeInformation().getClass())
                .isEqualTo(GenericTypeInfo.class);
    }

    public static class Container<T> {
        public T field;
    }

    public static class MyType extends Container<Container<Object>> {}

    @Test
    void testRecursivePojoWithTypeVariable() {
        TypeInformation<?> ti = TypeExtractor.createTypeInfo(MyType.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);
        TypeInformation<?> pti = ((PojoTypeInfo) ti).getPojoFieldAt(0).getTypeInformation();
        assertThat(pti).isInstanceOf(PojoTypeInfo.class);
        assertThat(((PojoTypeInfo) pti).getPojoFieldAt(0).getTypeInformation().getClass())
                .isEqualTo(GenericTypeInfo.class);
    }

    /** POJO generated using Lombok. */
    @Getter
    @Setter
    @NoArgsConstructor
    public static class TestLombok {
        private int age = 10;
        private String name;
    }

    @Test
    void testLombokPojo() {
        TypeInformation<TestLombok> ti = TypeExtractor.getForClass(TestLombok.class);
        assertThat(ti).isInstanceOf(PojoTypeInfo.class);

        PojoTypeInfo<TestLombok> pti = (PojoTypeInfo<TestLombok>) ti;
        assertThat(pti.getTypeAt(0)).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
        assertThat(pti.getTypeAt(1)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
    }
}
