/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.ExecutionConfig;

import org.junit.Test;

import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.INT_VALUE_ARRAY_TYPE_INFO;
import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.LONG_VALUE_ARRAY_TYPE_INFO;
import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.NULL_VALUE_ARRAY_TYPE_INFO;
import static org.apache.flink.graph.types.valuearray.ValueArrayTypeInfo.STRING_VALUE_ARRAY_TYPE_INFO;
import static org.junit.Assert.assertEquals;

/** Tests for {@link ValueArrayTypeInfo}. */
public class ValueArrayTypeInfoTest {

    private ExecutionConfig config = new ExecutionConfig();

    @Test
    public void testIntValueArray() {
        assertEquals(INT_VALUE_ARRAY_TYPE_INFO.getTypeClass(), ValueArray.class);
        assertEquals(
                INT_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass(),
                IntValueArraySerializer.class);
        assertEquals(
                INT_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass(),
                IntValueArrayComparator.class);
    }

    @Test
    public void testLongValueArray() {
        assertEquals(LONG_VALUE_ARRAY_TYPE_INFO.getTypeClass(), ValueArray.class);
        assertEquals(
                LONG_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass(),
                LongValueArraySerializer.class);
        assertEquals(
                LONG_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass(),
                LongValueArrayComparator.class);
    }

    @Test
    public void testNullValueArray() {
        assertEquals(NULL_VALUE_ARRAY_TYPE_INFO.getTypeClass(), ValueArray.class);
        assertEquals(
                NULL_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass(),
                NullValueArraySerializer.class);
        assertEquals(
                NULL_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass(),
                NullValueArrayComparator.class);
    }

    @Test
    public void testStringValueArray() {
        assertEquals(STRING_VALUE_ARRAY_TYPE_INFO.getTypeClass(), ValueArray.class);
        assertEquals(
                STRING_VALUE_ARRAY_TYPE_INFO.createSerializer(config).getClass(),
                StringValueArraySerializer.class);
        assertEquals(
                STRING_VALUE_ARRAY_TYPE_INFO.createComparator(true, config).getClass(),
                StringValueArrayComparator.class);
    }
}
