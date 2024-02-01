/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link TypeHint}. */
class TypeHintTest {

    @Test
    void testTypeInfoDirect() {

        // simple (non-generic case)
        TypeHint<String> stringInfo1 = new TypeHint<String>() {};
        TypeHint<String> stringInfo2 = new TypeHint<String>() {};

        assertThat(stringInfo1.getTypeInfo()).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);

        assertThat(stringInfo2).hasSameHashCodeAs(stringInfo1);
        assertThat(stringInfo2).isEqualTo(stringInfo1);
        assertThat(stringInfo2.toString()).isEqualTo(stringInfo1.toString());

        // generic case
        TypeHint<Tuple3<String, Double, Boolean>> generic =
                new TypeHint<Tuple3<String, Double, Boolean>>() {};

        TypeInformation<Tuple3<String, Double, Boolean>> tupleInfo =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO,
                        BasicTypeInfo.BOOLEAN_TYPE_INFO);

        assertThat(generic.getTypeInfo()).isEqualTo(tupleInfo);
    }

    @Test
    <T> void testWithGenericParameter() {
        assertThatThrownBy(() -> new TypeHint<T>() {}).isInstanceOf(FlinkRuntimeException.class);

        // this works, because "List" goes to the GenericType (blackbox) which does
        // not care about generic parametrization
        new TypeHint<List<T>>() {};
    }
}
