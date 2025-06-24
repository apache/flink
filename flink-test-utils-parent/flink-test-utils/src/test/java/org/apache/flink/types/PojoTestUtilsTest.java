/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.types;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PojoTestUtilsTest {

    @Test
    void testNonPojoRejected() {
        assertThatThrownBy(() -> PojoTestUtils.assertSerializedAsPojo(NoPojo.class))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void testPojoAccepted() {
        PojoTestUtils.assertSerializedAsPojo(Pojo.class);
    }

    @Test
    void testPojoAcceptedIfKryoRequired() {
        PojoTestUtils.assertSerializedAsPojo(PojoRequiringKryo.class);
    }

    @Test
    void testPojoTypeInfoOnInterface() {
        // reported in FLINK-35887
        PojoTestUtils.assertSerializedAsPojo(Foo.class);
    }

    @Test
    void testWithoutKryoPojoAccepted() {
        PojoTestUtils.assertSerializedAsPojoWithoutKryo(Pojo.class);
    }

    @Test
    void testWithoutKryoPojoRejected() {
        assertThatThrownBy(
                        () ->
                                PojoTestUtils.assertSerializedAsPojoWithoutKryo(
                                        PojoRequiringKryo.class))
                .isInstanceOf(AssertionError.class);
    }

    private static class NoPojo {}

    public static class Pojo {
        public int x;
    }

    public static class PojoRequiringKryo {
        public List<?> x;
    }

    @TypeInfo(FooFactory.class)
    public interface Foo {}

    public static class FooFactory extends TypeInfoFactory<Foo> {
        @Override
        public TypeInformation<Foo> createTypeInfo(Type type, Map<String, TypeInformation<?>> map) {
            return Types.POJO(Foo.class, new HashMap<>());
        }
    }
}
