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

package org.apache.flink.test.runtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Test registering types with Kryo. */
@RunWith(Parameterized.class)
public class RegisterTypeWithKryoSerializerITCase extends MultipleProgramsTestBase {

    public RegisterTypeWithKryoSerializerITCase(TestExecutionMode mode) {
        super(mode);
    }

    /**
     * Tests whether the kryo serializer is forwarded via the ExecutionConfig.
     *
     * @throws Exception
     */
    @Test
    public void testRegisterTypeWithKryoSerializer() throws Exception {
        int numElements = 10;
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.registerTypeWithKryoSerializer(TestClass.class, new TestClassSerializer());

        DataSet<Long> input = env.generateSequence(0, numElements - 1);

        DataSet<TestClass> mapped =
                input.map(
                        new MapFunction<Long, TestClass>() {
                            private static final long serialVersionUID = -529116076312998262L;

                            @Override
                            public TestClass map(Long value) throws Exception {
                                return new TestClass(value);
                            }
                        });

        List<TestClass> expected = new ArrayList<>(numElements);

        for (int i = 0; i < numElements; i++) {
            expected.add(new TestClass(42));
        }

        compareResultCollections(
                expected,
                mapped.collect(),
                new Comparator<TestClass>() {
                    @Override
                    public int compare(TestClass o1, TestClass o2) {
                        return (int) (o1.getValue() - o2.getValue());
                    }
                });
    }

    static class TestClass {
        private final long value;
        private Object obj = new Object();

        public TestClass(long value) {
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "TestClass(" + value + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TestClass) {
                TestClass other = (TestClass) obj;

                return value == other.value;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return (int) value;
        }
    }

    static class TestClassSerializer extends Serializer<TestClass> implements Serializable {

        private static final long serialVersionUID = -3585880741695717533L;

        @Override
        public void write(Kryo kryo, Output output, TestClass testClass) {
            output.writeLong(42);
        }

        @Override
        public TestClass read(Kryo kryo, Input input, Class<TestClass> aClass) {
            return new TestClass(input.readLong());
        }
    }
}
