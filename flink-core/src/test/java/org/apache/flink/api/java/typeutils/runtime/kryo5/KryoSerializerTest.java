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

package org.apache.flink.api.java.typeutils.runtime.kryo5;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.Kryo5Registration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import com.esotericsoftware.kryo.kryo5.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class KryoSerializerTest {
    public <T> void testSerialize(KryoSerializer<T> kryoSerializer, T o) throws IOException {
        KryoClearedBufferTest.TestDataOutputView target =
                new KryoClearedBufferTest.TestDataOutputView(500);
        kryoSerializer.serialize(o, target);

        DataInputViewStreamWrapper input =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(target.getBuffer()));
        T o2 = kryoSerializer.deserialize(input);

        Assertions.assertEquals(o, o2);
    }

    @Test
    public void testListSerialization() throws IOException {
        LinkedHashMap<Class<?>, ExecutionConfig.SerializableKryo5Serializer<?>>
                defaultSerializerMap = new LinkedHashMap<>();
        LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses =
                new LinkedHashMap<>();
        LinkedHashMap<String, Kryo5Registration> kryoRegistration = new LinkedHashMap<>();
        KryoSerializer<ArrayList> kryoSerializer =
                new KryoSerializer<>(
                        ArrayList.class,
                        defaultSerializerMap,
                        defaultSerializerClasses,
                        kryoRegistration,
                        null);

        ArrayList<Object> testList = new ArrayList<>();
        testList.add(123);
        testList.add("xyz");
        testList.add(false);
        testList.add(789);

        testSerialize(kryoSerializer, testList);
    }
}
