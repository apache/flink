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

package org.apache.flink.api.java;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests concerning type extraction by ExecutionEnvironment methods. */
@SuppressWarnings("serial")
public class TypeExtractionTest {

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testFunctionWithMissingGenericsAndReturns() {

        RichMapFunction function =
                new RichMapFunction() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object map(Object value) throws Exception {
                        return null;
                    }
                };

        TypeInformation<?> info =
                ExecutionEnvironment.getExecutionEnvironment()
                        .fromElements("arbitrary", "data")
                        .map(function)
                        .returns(Types.STRING)
                        .getResultType();

        assertEquals(Types.STRING, info);
    }

    @Test
    public void testGetterSetterWithVertex() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(new VertexTyped(0L, 3.0), new VertexTyped(1L, 1.0));
    }

    // ------------------------------------------------------------------------
    //  Test types
    // ------------------------------------------------------------------------

    /**
     * Representation of Vertex with maximum of 2 keys and a value.
     *
     * @param <K> keys type
     * @param <V> value type
     */
    public static class Vertex<K, V> {

        private K key1;
        private K key2;
        private V value;

        public Vertex() {}

        public Vertex(K key, V value) {
            this.key1 = key;
            this.key2 = key;
            this.value = value;
        }

        public Vertex(K key1, K key2, V value) {
            this.key1 = key1;
            this.key2 = key2;
            this.value = value;
        }

        public void setKey1(K key1) {
            this.key1 = key1;
        }

        public void setKey2(K key2) {
            this.key2 = key2;
        }

        public K getKey1() {
            return key1;
        }

        public K getKey2() {
            return key2;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public V getValue() {
            return value;
        }
    }

    /** A {@link Vertex} with {@link Long} as key and {@link Double} as value. */
    public static class VertexTyped extends Vertex<Long, Double> {
        public VertexTyped(Long l, Double d) {
            super(l, d);
        }

        public VertexTyped() {}
    }
}
