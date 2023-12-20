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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the {@link TypeInformation} class. */
class TypeInformationTest {

    @Test
    void testOfClass() {
        assertThat(TypeInformation.of(String.class)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Test
    void testOfGenericClassForFlink() {
        try {
            TypeInformation.of(Tuple3.class);
            fail("should fail with an exception");
        } catch (FlinkRuntimeException e) {
            // check that the error message mentions the TypeHint
            assertThat(e.getMessage().indexOf("TypeHint")).isNotEqualTo(-1);
        }
    }

    @Test
    void testOfGenericClassForGenericType() {
        assertThat(TypeInformation.of(List.class)).isEqualTo(new GenericTypeInfo<>(List.class));
    }

    @Test
    void testOfTypeHint() {
        assertThat(TypeInformation.of(String.class)).isEqualTo(BasicTypeInfo.STRING_TYPE_INFO);
        assertEquals(BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<String>() {}));

        TypeInformation<Tuple3<String, Double, Boolean>> tupleInfo =
                new TupleTypeInfo<>(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.DOUBLE_TYPE_INFO,
                        BasicTypeInfo.BOOLEAN_TYPE_INFO);

        assertEquals(
                tupleInfo, TypeInformation.of(new TypeHint<Tuple3<String, Double, Boolean>>() {}));
    }
}
