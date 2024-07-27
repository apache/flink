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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class SerializersTest {

    // recursive
    public static class Node {
        private Node parent;
    }

    public static class FromNested {
        Node recurseMe;
    }

    public static class FromGeneric1 {}

    public static class FromGeneric2 {}

    public static class Nested1 {
        private FromNested fromNested;
        private Path yodaInterval;
    }

    public static class ClassWithNested {

        Nested1 nested;
        int ab;

        ArrayList<FromGeneric1> addGenType;
        FromGeneric2[] genericArrayType;
    }

    @Test
    void testTypeRegistration() {
        SerializerConfigImpl conf = new SerializerConfigImpl();
        Serializers.recursivelyRegisterType(ClassWithNested.class, conf, new HashSet<Class<?>>());

        KryoSerializer<String> kryo =
                new KryoSerializer<>(String.class, conf); // we create Kryo from another type.

        assertThat(kryo.getKryo().getRegistration(FromNested.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(ClassWithNested.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(Path.class).getId()).isPositive();

        // check if the generic type from one field is also registered (its very likely that
        // generic types are also used as fields somewhere.
        assertThat(kryo.getKryo().getRegistration(FromGeneric1.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(FromGeneric2.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(Node.class).getId()).isPositive();

        // register again and make sure classes are still registered
        SerializerConfigImpl conf2 = new SerializerConfigImpl();
        Serializers.recursivelyRegisterType(ClassWithNested.class, conf2, new HashSet<Class<?>>());
        KryoSerializer<String> kryo2 = new KryoSerializer<>(String.class, conf);
        assertThat(kryo2.getKryo().getRegistration(FromNested.class).getId()).isPositive();
    }

    @Test
    void testTypeRegistrationFromTypeInfo() {
        SerializerConfigImpl conf = new SerializerConfigImpl();
        Serializers.recursivelyRegisterType(
                new GenericTypeInfo<>(ClassWithNested.class), conf, new HashSet<Class<?>>());

        KryoSerializer<String> kryo =
                new KryoSerializer<>(String.class, conf); // we create Kryo from another type.

        assertThat(kryo.getKryo().getRegistration(FromNested.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(ClassWithNested.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(Path.class).getId()).isPositive();

        // check if the generic type from one field is also registered (its very likely that
        // generic types are also used as fields somewhere.
        assertThat(kryo.getKryo().getRegistration(FromGeneric1.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(FromGeneric2.class).getId()).isPositive();
        assertThat(kryo.getKryo().getRegistration(Node.class).getId()).isPositive();
    }
}
