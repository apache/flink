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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Ignore
public class SerializerTestInstance<T> extends SerializerTestBase<T> {

    private final TypeSerializer<T> serializer;

    private final Class<T> typeClass;

    private final int length;

    private final T[] testData;

    // --------------------------------------------------------------------------------------------

    @SafeVarargs
    public SerializerTestInstance(
            TypeSerializer<T> serializer, Class<T> typeClass, int length, T... testData) {
        this(new DeeplyEqualsChecker(), serializer, typeClass, length, testData);
    }

    @SafeVarargs
    public SerializerTestInstance(
            DeeplyEqualsChecker checker,
            TypeSerializer<T> serializer,
            Class<T> typeClass,
            int length,
            T... testData) {
        super(checker);
        this.serializer = serializer;
        this.typeClass = typeClass;
        this.length = length;
        this.testData = testData;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    protected TypeSerializer<T> createSerializer() {
        return this.serializer;
    }

    @Override
    protected int getLength() {
        return this.length;
    }

    @Override
    protected Class<T> getTypeClass() {
        return this.typeClass;
    }

    @Override
    protected T[] getTestData() {
        return this.testData;
    }

    // --------------------------------------------------------------------------------------------

    public void testAll() {
        for (Method method : SerializerTestBase.class.getMethods()) {
            if (method.getAnnotation(Test.class) == null) {
                continue;
            }
            try {
                method.invoke(this);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Unable to invoke test " + method.getName(), e);
            } catch (InvocationTargetException e) {
                sneakyThrow(e.getCause());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}
