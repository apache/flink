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

import org.junit.jupiter.api.Test;

import static org.apache.flink.architecture.common.Predicates.getClassSimpleNameFromFqName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.flink.architecture.common.Predicates}. */
class PredicatesTest {

    @Test
    void testGetClassSimpleNameFromFqName() {
        assertThat(getClassSimpleNameFromFqName("com.example.OuterClass")).isEqualTo("OuterClass");
        assertThat(getClassSimpleNameFromFqName("com.example.OuterClass.InnerClass"))
                .isEqualTo("InnerClass");
        assertThat(getClassSimpleNameFromFqName("com.example.OuterClass$InnerClass"))
                .isEqualTo("InnerClass");
    }

    @Test
    void testInvalidInput() {
        assertThatThrownBy(() -> getClassSimpleNameFromFqName(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("Fully qualified class name cannot be null");
        assertThatThrownBy(() -> getClassSimpleNameFromFqName(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Fully qualified class name cannot be empty");
        assertThatThrownBy(() -> getClassSimpleNameFromFqName("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Fully qualified class name cannot be empty");
    }
}
